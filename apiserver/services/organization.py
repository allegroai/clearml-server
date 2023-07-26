from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from operator import itemgetter
from typing import Mapping, Type, Sequence, Optional, Callable

from flask import stream_with_context
from mongoengine import Q

from apiserver.apierrors import errors
from apiserver.apimodels.organization import (
    TagsRequest,
    EntitiesCountRequest,
    DownloadForGetAll,
    EntityType,
    PrepareDownloadForGetAll,
)
from apiserver.bll.model import Metadata
from apiserver.bll.organization import OrgBLL, Tags
from apiserver.bll.project import ProjectBLL
from apiserver.config_repo import config
from apiserver.database.model import User, AttributedDocument, EntityVisibility
from apiserver.database.model.model import Model
from apiserver.database.model.project import Project
from apiserver.database.model.task.task import Task, TaskType
from apiserver.redis_manager import redman
from apiserver.service_repo import endpoint, APICall
from apiserver.services.models import conform_model_data
from apiserver.services.tasks import (
    escape_execution_parameters,
    _hidden_query,
    conform_task_data,
)
from apiserver.services.utils import get_tags_filter_dictionary, sort_tags_response
from apiserver.utilities import json
from apiserver.utilities.dicts import nested_get

org_bll = OrgBLL()
project_bll = ProjectBLL()
redis = redman.connection("apiserver")


@endpoint("organization.get_tags", request_data_model=TagsRequest)
def get_tags(call: APICall, company, request: TagsRequest):
    filter_dict = get_tags_filter_dictionary(request.filter)
    ret = defaultdict(set)
    for entity in Tags.Model, Tags.Task:
        tags = org_bll.get_tags(
            company, entity, include_system=request.include_system, filter_=filter_dict,
        )
        for field, vals in tags.items():
            ret[field] |= vals

    call.result.data = sort_tags_response(ret)


@endpoint("organization.get_user_companies")
def get_user_companies(call: APICall, company_id: str, _):
    users = [
        {"id": u.id, "name": u.name, "avatar": u.avatar}
        for u in User.objects(company=company_id).only("avatar", "name", "company")
    ]

    call.result.data = {
        "companies": [
            {
                "id": company_id,
                "name": call.identity.company_name,
                "allocated": len(users),
                "owners": sorted(users, key=itemgetter("name")),
            }
        ]
    }


@endpoint("organization.get_entities_count")
def get_entities_count(call: APICall, company, request: EntitiesCountRequest):
    entity_classes: Mapping[str, Type[AttributedDocument]] = {
        "projects": Project,
        "tasks": Task,
        "models": Model,
        "pipelines": Project,
        "datasets": Project,
        "reports": Task,
    }
    ret = {}
    for field, entity_cls in entity_classes.items():
        data = call.data.get(field)
        if data is None:
            continue

        if field == "reports":
            data["type"] = TaskType.report
            data["include_subprojects"] = True

        if request.active_users:
            if entity_cls is Project:
                requested_ids = data.get("id")
                if isinstance(requested_ids, str):
                    requested_ids = [requested_ids]
                ids, _ = project_bll.get_projects_with_selected_children(
                    company=company,
                    users=request.active_users,
                    project_ids=requested_ids,
                    allow_public=request.allow_public,
                )
                if not ids:
                    ret[field] = 0
                    continue
                data["id"] = ids
            elif not data.get("user"):
                data["user"] = request.active_users

        query = Q()
        if (
            entity_cls in (Project, Task)
            and field not in ("reports", "pipelines", "datasets")
            and not request.search_hidden
        ):
            query &= Q(system_tags__ne=EntityVisibility.hidden.value)

        ret[field] = entity_cls.get_count(
            company=company,
            query_dict=data,
            query=query,
            allow_public=request.allow_public,
        )

    call.result.data = ret


def _get_download_getter_fn(
    company: str,
    call: APICall,
    call_data: dict,
    allow_public: bool,
    entity_type: EntityType,
) -> Optional[Callable[[int, int], Sequence[dict]]]:
    def get_task_data() -> Sequence[dict]:
        tasks = Task.get_many_with_join(
            company=company,
            query_dict=call_data,
            query=_hidden_query(call_data),
            allow_public=allow_public,
        )
        conform_task_data(call, tasks)
        return tasks

    def get_model_data() -> Sequence[dict]:
        models = Model.get_many_with_join(
            company=company, query_dict=call_data, allow_public=allow_public,
        )
        conform_model_data(call, models)
        return models

    if entity_type == EntityType.task:
        call_data = escape_execution_parameters(call_data)
        get_fn = get_task_data
    elif entity_type == EntityType.model:
        call_data = Metadata.escape_query_parameters(call_data)
        get_fn = get_model_data
    else:
        raise errors.bad_request.ValidationError(
            f"Unsupported entity type: {str(entity_type)}"
        )

    def getter(page: int, page_size: int) -> Sequence[dict]:
        call_data.pop("scroll_id", None)
        call_data["page"] = page
        call_data["page_size"] = page_size
        return get_fn()

    return getter


@endpoint("organization.prepare_download_for_get_all")
def prepare_download_for_get_all(
    call: APICall, company: str, request: PrepareDownloadForGetAll
):
    # validate input params
    getter = _get_download_getter_fn(
        company,
        call,
        call_data=call.data.copy(),
        allow_public=request.allow_public,
        entity_type=request.entity_type,
    )
    if getter:
        getter(0, 1)

    redis.setex(
        f"get_all_download_{call.id}",
        int(config.get("services.organization.download.redis_timeout_sec", 300)),
        json.dumps(call.data),
    )

    call.result.data = dict(prepare_id=call.id)


@endpoint("organization.download_for_get_all")
def download_for_get_all(call: APICall, company, request: DownloadForGetAll):
    request_data = redis.get(f"get_all_download_{request.prepare_id}")
    if not request_data:
        raise errors.bad_request.InvalidId(
            f"prepare ID not found", prepare_id=request.prepare_id
        )

    try:
        call_data = json.loads(request_data)
        request = PrepareDownloadForGetAll(**call_data)
    except Exception as ex:
        raise errors.server_error.DataError("failed parsing prepared data", ex=ex)

    def generate():
        projection = call_data.get("only_fields", [])
        headers = ",".join(projection)

        get_fn = _get_download_getter_fn(
            company,
            call,
            call_data=call_data,
            allow_public=request.allow_public,
            entity_type=request.entity_type,
        )
        if not get_fn:
            return headers

        fields = [path.split(".") for path in projection]

        def get_entity_field_as_str(data: dict, field: Sequence[str]) -> str:
            val = nested_get(data, field, "")
            if isinstance(val, dict):
                val = val.get("id", "")

            return str(val)

        def get_string_from_entity_data(data: dict) -> str:
            return ",".join(get_entity_field_as_str(data, f) for f in fields)

        with ThreadPoolExecutor(1) as pool:
            page = 0
            page_size = int(
                config.get("services.organization.download.batch_size", 500)
            )
            future = pool.submit(get_fn, page, page_size)
            out = [headers]

            while True:
                result = future.result()
                if not result:
                    break

                page += 1
                future = pool.submit(get_fn, page, page_size)

                out.extend(get_string_from_entity_data(r) for r in result)
                yield "\n".join(out) + "\n"
                out = []

        if out:
            yield "\n".join(out)

    call.result.filename = f"{request.entity_type}_export.{request.download_type}"
    call.result.content_type = "text/csv"
    call.result.raw_data = stream_with_context(generate())
