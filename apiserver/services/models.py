from datetime import datetime
from typing import Sequence

from mongoengine import Q, EmbeddedDocument

from apiserver import database
from apiserver.apierrors import errors
from apiserver.apierrors.errors.bad_request import InvalidModelId
from apiserver.apimodels.base import UpdateResponse, MakePublicRequest, MoveRequest
from apiserver.apimodels.models import (
    CreateModelRequest,
    CreateModelResponse,
    PublishModelRequest,
    PublishModelResponse,
    ModelTaskPublishResponse,
    GetFrameworksRequest,
    DeleteModelRequest,
    DeleteMetadataRequest,
    AddOrUpdateMetadataRequest,
)
from apiserver.bll.organization import OrgBLL, Tags
from apiserver.bll.project import ProjectBLL, project_ids_with_children
from apiserver.bll.task import TaskBLL
from apiserver.bll.task.utils import deleted_prefix
from apiserver.config_repo import config
from apiserver.database.errors import translate_errors_context
from apiserver.database.model import validate_id
from apiserver.database.model.metadata import metadata_add_or_update, metadata_delete
from apiserver.database.model.model import Model
from apiserver.database.model.project import Project
from apiserver.database.model.task.task import Task, TaskStatus, ModelItem
from apiserver.database.utils import (
    parse_from_call,
    get_company_or_none_constraint,
    filter_fields,
)
from apiserver.service_repo import APICall, endpoint
from apiserver.services.utils import (
    conform_tag_fields,
    conform_output_tags,
    ModelsBackwardsCompatibility,
    validate_metadata,
    get_metadata_from_api,
)
from apiserver.timing_context import TimingContext

log = config.logger(__file__)
org_bll = OrgBLL()
project_bll = ProjectBLL()


@endpoint("models.get_by_id", required_fields=["model"])
def get_by_id(call: APICall, company_id, _):
    model_id = call.data["model"]

    with translate_errors_context():
        models = Model.get_many(
            company=company_id,
            query_dict=call.data,
            query=Q(id=model_id),
            allow_public=True,
        )
        if not models:
            raise errors.bad_request.InvalidModelId(
                "no such public or company model", id=model_id, company=company_id,
            )
        conform_output_tags(call, models[0])
        call.result.data = {"model": models[0]}


@endpoint("models.get_by_task_id", required_fields=["task"])
def get_by_task_id(call: APICall, company_id, _):
    if call.requested_endpoint_version > ModelsBackwardsCompatibility.max_version:
        raise errors.moved_permanently.NotSupported("use models.get_by_id/get_all apis")

    task_id = call.data["task"]

    with translate_errors_context():
        query = dict(id=task_id, company=company_id)
        task = Task.get(_only=["models"], **query)
        if not task:
            raise errors.bad_request.InvalidTaskId(**query)
        if not task.models.output:
            raise errors.bad_request.MissingTaskFields(field="models.output")

        model_id = task.models.output[-1].model
        model = Model.objects(
            Q(id=model_id) & get_company_or_none_constraint(company_id)
        ).first()
        if not model:
            raise errors.bad_request.InvalidModelId(
                "no such public or company model", id=model_id, company=company_id,
            )
        model_dict = model.to_proper_dict()
        conform_output_tags(call, model_dict)
        call.result.data = {"model": model_dict}


def _process_include_subprojects(call_data: dict):
    include_subprojects = call_data.pop("include_subprojects", False)
    project_ids = call_data.get("project")
    if not project_ids or not include_subprojects:
        return

    if not isinstance(project_ids, list):
        project_ids = [project_ids]
    call_data["project"] = project_ids_with_children(project_ids)


@endpoint("models.get_all_ex", required_fields=[])
def get_all_ex(call: APICall, company_id, _):
    conform_tag_fields(call, call.data)
    with translate_errors_context():
        _process_include_subprojects(call.data)
        with TimingContext("mongo", "models_get_all_ex"):
            models = Model.get_many_with_join(
                company=company_id, query_dict=call.data, allow_public=True
            )
        conform_output_tags(call, models)
        call.result.data = {"models": models}


@endpoint("models.get_by_id_ex", required_fields=["id"])
def get_by_id_ex(call: APICall, company_id, _):
    conform_tag_fields(call, call.data)
    with translate_errors_context():
        with TimingContext("mongo", "models_get_by_id_ex"):
            models = Model.get_many_with_join(
                company=company_id, query_dict=call.data, allow_public=True
            )
        conform_output_tags(call, models)
        call.result.data = {"models": models}


@endpoint("models.get_all", required_fields=[])
def get_all(call: APICall, company_id, _):
    conform_tag_fields(call, call.data)
    with translate_errors_context():
        with TimingContext("mongo", "models_get_all"):
            models = Model.get_many(
                company=company_id,
                parameters=call.data,
                query_dict=call.data,
                allow_public=True,
            )
        conform_output_tags(call, models)
        call.result.data = {"models": models}


@endpoint("models.get_frameworks", request_data_model=GetFrameworksRequest)
def get_frameworks(call: APICall, company_id, request: GetFrameworksRequest):
    call.result.data = {
        "frameworks": sorted(
            project_bll.get_model_frameworks(company_id, project_ids=request.projects)
        )
    }


create_fields = {
    "name": None,
    "tags": list,
    "system_tags": list,
    "task": Task,
    "comment": None,
    "uri": None,
    "project": Project,
    "parent": Model,
    "framework": None,
    "design": None,
    "labels": dict,
    "ready": None,
    "metadata": list,
}


def parse_model_fields(call, valid_fields):
    fields = parse_from_call(call.data, valid_fields, Model.get_fields())
    conform_tag_fields(call, fields, validate=True)
    metadata = fields.get("metadata")
    if metadata:
        validate_metadata(metadata)
    return fields


def _update_cached_tags(company: str, project: str, fields: dict):
    org_bll.update_tags(
        company,
        Tags.Model,
        project=project,
        tags=fields.get("tags"),
        system_tags=fields.get("system_tags"),
    )


def _reset_cached_tags(company: str, projects: Sequence[str]):
    org_bll.reset_tags(
        company, Tags.Model, projects=projects,
    )


def _get_company_model(company_id: str, model_id: str, only_fields=None) -> Model:
    query = dict(company=company_id, id=model_id)
    qs = Model.objects(**query)
    if only_fields:
        qs = qs.only(*only_fields)
    model = qs.first()
    if not model:
        raise errors.bad_request.InvalidModelId(**query)
    return model


@endpoint("models.update_for_task", required_fields=["task"])
def update_for_task(call: APICall, company_id, _):
    if call.requested_endpoint_version > ModelsBackwardsCompatibility.max_version:
        raise errors.moved_permanently.NotSupported("use tasks.add_or_update_model")

    task_id = call.data["task"]
    uri = call.data.get("uri")
    iteration = call.data.get("iteration")
    override_model_id = call.data.get("override_model_id")
    if not (uri or override_model_id) or (uri and override_model_id):
        raise errors.bad_request.MissingRequiredFields(
            "exactly one field is required", fields=("uri", "override_model_id")
        )

    with translate_errors_context():

        query = dict(id=task_id, company=company_id)
        task = Task.get_for_writing(
            id=task_id,
            company=company_id,
            _only=["models", "execution", "name", "status", "project"],
        )
        if not task:
            raise errors.bad_request.InvalidTaskId(**query)

        allowed_states = [TaskStatus.created, TaskStatus.in_progress]
        if task.status not in allowed_states:
            raise errors.bad_request.InvalidTaskStatus(
                f"model can only be updated for tasks in the {allowed_states} states",
                **query,
            )

        if override_model_id:
            model = _get_company_model(
                company_id=company_id, model_id=override_model_id
            )
        else:
            if "name" not in call.data:
                # use task name if name not provided
                call.data["name"] = task.name

            if "comment" not in call.data:
                call.data["comment"] = f"Created by task `{task.name}` ({task.id})"

            if task.models.output:
                # model exists, update
                model_id = task.models.output[-1].model
                res = _update_model(call, company_id, model_id=model_id).to_struct()
                res.update({"id": model_id, "created": False})
                call.result.data = res
                return

            # new model, create
            fields = parse_model_fields(call, create_fields)

            # create and save model
            model = Model(
                id=database.utils.id(),
                created=datetime.utcnow(),
                user=call.identity.user,
                company=company_id,
                project=task.project,
                framework=task.execution.framework,
                parent=task.models.input[0].model if task.models.input else None,
                design=task.execution.model_desc,
                labels=task.execution.model_labels,
                ready=(task.status == TaskStatus.published),
                **fields,
            )
            model.save()
            _update_cached_tags(company_id, project=model.project, fields=fields)

        TaskBLL.update_statistics(
            task_id=task_id,
            company_id=company_id,
            last_iteration_max=iteration,
            models__output=[
                ModelItem(model=model.id, name=model.name, updated=datetime.utcnow())
            ],
        )

        call.result.data = {"id": model.id, "created": True}


@endpoint(
    "models.create",
    request_data_model=CreateModelRequest,
    response_data_model=CreateModelResponse,
)
def create(call: APICall, company_id, req_model: CreateModelRequest):

    if req_model.public:
        company_id = ""

    with translate_errors_context():

        project = req_model.project
        if project:
            validate_id(Project, company=company_id, project=project)

        task = req_model.task
        req_data = req_model.to_struct()
        if task:
            validate_task(company_id, req_data)

        fields = filter_fields(Model, req_data)
        conform_tag_fields(call, fields, validate=True)

        validate_metadata(fields.get("metadata"))

        # create and save model
        model = Model(
            id=database.utils.id(),
            user=call.identity.user,
            company=company_id,
            created=datetime.utcnow(),
            **fields,
        )
        model.save()
        _update_cached_tags(company_id, project=model.project, fields=fields)

        call.result.data_model = CreateModelResponse(id=model.id, created=True)


def prepare_update_fields(call, company_id, fields: dict):
    fields = fields.copy()
    if "uri" in fields:
        # clear UI cache if URI is provided (model updated)
        fields["ui_cache"] = fields.pop("ui_cache", {})
    if "task" in fields:
        validate_task(company_id, fields)

    if "labels" in fields:
        labels = fields["labels"]

        def find_other_types(iterable, type_):
            res = [x for x in iterable if not isinstance(x, type_)]
            try:
                return set(res)
            except TypeError:
                # Un-hashable, probably
                return res

        invalid_keys = find_other_types(labels.keys(), str)
        if invalid_keys:
            raise errors.bad_request.ValidationError(
                "labels keys must be strings", keys=invalid_keys
            )

        invalid_values = find_other_types(labels.values(), int)
        if invalid_values:
            raise errors.bad_request.ValidationError(
                "labels values must be integers", values=invalid_values
            )

    conform_tag_fields(call, fields, validate=True)
    return fields


def validate_task(company_id, fields: dict):
    Task.get_for_writing(company=company_id, id=fields["task"], _only=["id"])


@endpoint("models.edit", required_fields=["model"], response_data_model=UpdateResponse)
def edit(call: APICall, company_id, _):
    model_id = call.data["model"]

    with translate_errors_context():
        model = _get_company_model(company_id=company_id, model_id=model_id)

        fields = parse_model_fields(call, create_fields)
        fields = prepare_update_fields(call, company_id, fields)

        for key in fields:
            field = getattr(model, key, None)
            value = fields[key]
            if (
                field
                and isinstance(value, dict)
                and isinstance(field, EmbeddedDocument)
            ):
                d = field.to_mongo(use_db_field=False).to_dict()
                d.update(value)
                fields[key] = d

        iteration = call.data.get("iteration")
        task_id = model.task or fields.get("task")
        if task_id and iteration is not None:
            TaskBLL.update_statistics(
                task_id=task_id, company_id=company_id, last_iteration_max=iteration,
            )

        if fields:
            updated = model.update(upsert=False, **fields)
            if updated:
                new_project = fields.get("project", model.project)
                if new_project != model.project:
                    _reset_cached_tags(
                        company_id, projects=[new_project, model.project]
                    )
                else:
                    _update_cached_tags(
                        company_id, project=model.project, fields=fields
                    )
            conform_output_tags(call, fields)
            call.result.data_model = UpdateResponse(updated=updated, fields=fields)
        else:
            call.result.data_model = UpdateResponse(updated=0)


def _update_model(call: APICall, company_id, model_id=None):
    model_id = model_id or call.data["model"]

    with translate_errors_context():
        model = _get_company_model(company_id=company_id, model_id=model_id)

        data = prepare_update_fields(call, company_id, call.data)

        task_id = data.get("task")
        iteration = data.get("iteration")
        if task_id and iteration is not None:
            TaskBLL.update_statistics(
                task_id=task_id, company_id=company_id, last_iteration_max=iteration,
            )

        metadata = data.get("metadata")
        if metadata:
            validate_metadata(metadata)

        updated_count, updated_fields = Model.safe_update(company_id, model.id, data)
        if updated_count:
            new_project = updated_fields.get("project", model.project)
            if new_project != model.project:
                _reset_cached_tags(company_id, projects=[new_project, model.project])
            else:
                _update_cached_tags(
                    company_id, project=model.project, fields=updated_fields
                )
        conform_output_tags(call, updated_fields)
        return UpdateResponse(updated=updated_count, fields=updated_fields)


@endpoint(
    "models.update", required_fields=["model"], response_data_model=UpdateResponse
)
def update(call, company_id, _):
    call.result.data_model = _update_model(call, company_id)


@endpoint(
    "models.set_ready",
    request_data_model=PublishModelRequest,
    response_data_model=PublishModelResponse,
)
def set_ready(call: APICall, company_id, req_model: PublishModelRequest):
    updated, published_task_data = TaskBLL.model_set_ready(
        model_id=req_model.model,
        company_id=company_id,
        publish_task=req_model.publish_task,
        force_publish_task=req_model.force_publish_task,
    )

    call.result.data_model = PublishModelResponse(
        updated=updated,
        published_task=ModelTaskPublishResponse(**published_task_data)
        if published_task_data
        else None,
    )


@endpoint("models.delete", request_data_model=DeleteModelRequest)
def delete(call: APICall, company_id, request: DeleteModelRequest):
    model_id = request.model
    force = request.force

    with translate_errors_context():
        model = _get_company_model(
            company_id=company_id,
            model_id=model_id,
            only_fields=("id", "task", "project", "uri"),
        )
        deleted_model_id = f"{deleted_prefix}{model_id}"

        using_tasks = Task.objects(models__input__model=model_id).only("id")
        if using_tasks:
            if not force:
                raise errors.bad_request.ModelInUse(
                    "as execution model, use force=True to delete",
                    num_tasks=len(using_tasks),
                )
            # update deleted model id in using tasks
            Task._get_collection().update_many(
                filter={"_id": {"$in": [t.id for t in using_tasks]}},
                update={"$set": {"models.input.$[elem].model": deleted_model_id}},
                array_filters=[{"elem.model": model_id}],
                upsert=False,
            )

        if model.task:
            task: Task = Task.objects(id=model.task).first()
            if task and task.status == TaskStatus.published:
                if not force:
                    raise errors.bad_request.ModelCreatingTaskExists(
                        "and published, use force=True to delete", task=model.task
                    )
                if task.models.output and model_id in task.models.output:
                    now = datetime.utcnow()
                    Task._get_collection().update_one(
                        filter={"_id": model.task, "models.output.model": model_id},
                        update={
                            "$set": {
                                "models.output.$[elem].model": deleted_model_id,
                                "output.error": f"model deleted on {now.isoformat()}",
                            },
                            "last_change": now,
                        },
                        array_filters=[{"elem.model": model_id}],
                        upsert=False,
                    )

        del_count = Model.objects(id=model_id, company=company_id).delete()
        if del_count:
            _reset_cached_tags(company_id, projects=[model.project])
        call.result.data = dict(deleted=del_count > 0, url=model.uri,)


@endpoint("models.make_public", min_version="2.9", request_data_model=MakePublicRequest)
def make_public(call: APICall, company_id, request: MakePublicRequest):
    with translate_errors_context():
        call.result.data = Model.set_public(
            company_id, ids=request.ids, invalid_cls=InvalidModelId, enabled=True
        )


@endpoint(
    "models.make_private", min_version="2.9", request_data_model=MakePublicRequest
)
def make_public(call: APICall, company_id, request: MakePublicRequest):
    with translate_errors_context():
        call.result.data = Model.set_public(
            company_id, request.ids, invalid_cls=InvalidModelId, enabled=False
        )


@endpoint("models.move", request_data_model=MoveRequest)
def move(call: APICall, company_id: str, request: MoveRequest):
    if not (request.project or request.project_name):
        raise errors.bad_request.MissingRequiredFields(
            "project or project_name is required"
        )

    with translate_errors_context():
        return {
            "project_id": project_bll.move_under_project(
                entity_cls=Model,
                user=call.identity.user,
                company=company_id,
                ids=request.ids,
                project=request.project,
                project_name=request.project_name,
            )
        }


@endpoint("models.add_or_update_metadata", min_version="2.13")
def add_or_update_metadata(
    _: APICall, company_id: str, request: AddOrUpdateMetadataRequest
):
    model_id = request.model
    _get_company_model(company_id=company_id, model_id=model_id)

    return {
        "updated": metadata_add_or_update(
            cls=Model, _id=model_id, items=get_metadata_from_api(request.metadata),
        )
    }


@endpoint("models.delete_metadata", min_version="2.13")
def delete_metadata(_: APICall, company_id: str, request: DeleteMetadataRequest):
    model_id = request.model
    _get_company_model(company_id=company_id, model_id=model_id, only_fields=("id",))

    return {"updated": metadata_delete(cls=Model, _id=model_id, keys=request.keys)}
