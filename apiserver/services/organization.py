from collections import defaultdict
from operator import itemgetter
from typing import Mapping, Type

from mongoengine import Q

from apiserver.apimodels.organization import TagsRequest, EntitiesCountRequest
from apiserver.bll.organization import OrgBLL, Tags
from apiserver.bll.project import ProjectBLL
from apiserver.database.model import User, AttributedDocument, EntityVisibility
from apiserver.database.model.model import Model
from apiserver.database.model.project import Project
from apiserver.database.model.task.task import Task, TaskType
from apiserver.service_repo import endpoint, APICall
from apiserver.services.utils import get_tags_filter_dictionary, sort_tags_response

org_bll = OrgBLL()
project_bll = ProjectBLL()


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
                ids, _ = project_bll.get_projects_with_active_user(
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
            and field != "reports"
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
