from datetime import datetime
from urllib.parse import urlparse

from mongoengine import Q, EmbeddedDocument

import database
from apierrors import errors
from apimodels.base import UpdateResponse
from apimodels.models import (
    CreateModelRequest,
    CreateModelResponse,
    PublishModelRequest,
    PublishModelResponse,
    ModelTaskPublishResponse,
)
from bll.task import TaskBLL
from config import config
from database.errors import translate_errors_context
from database.fields import SupportedURLField
from database.model import validate_id
from database.model.model import Model
from database.model.project import Project
from database.model.task.task import Task, TaskStatus
from database.utils import (
    parse_from_call,
    get_company_or_none_constraint,
    filter_fields,
)
from service_repo import APICall, endpoint
from timing_context import TimingContext

log = config.logger(__file__)
get_all_query_options = Model.QueryParameterOptions(
    pattern_fields=("name", "comment"),
    fields=("ready",),
    list_fields=("tags", "framework", "uri", "id", "project", "task", "parent"),
)


@endpoint("models.get_by_id", required_fields=["model"])
def get_by_id(call):
    assert isinstance(call, APICall)
    model_id = call.data["model"]

    with translate_errors_context():
        res = Model.get_many(
            company=call.identity.company,
            query_dict=call.data,
            query=Q(id=model_id),
            allow_public=True,
        )
        if not res:
            raise errors.bad_request.InvalidModelId(
                "no such public or company model",
                id=model_id,
                company=call.identity.company,
            )

        call.result.data = {"model": res[0]}


@endpoint("models.get_by_task_id", required_fields=["task"])
def get_by_task_id(call):
    assert isinstance(call, APICall)
    task_id = call.data["task"]

    with translate_errors_context():
        query = dict(id=task_id, company=call.identity.company)
        res = Task.get(_only=["output"], **query)
        if not res:
            raise errors.bad_request.InvalidTaskId(**query)
        if not res.output:
            raise errors.bad_request.MissingTaskFields(field="output")
        if not res.output.model:
            raise errors.bad_request.MissingTaskFields(field="output.model")

        model_id = res.output.model
        res = Model.objects(
            Q(id=model_id) & get_company_or_none_constraint(call.identity.company)
        ).first()
        if not res:
            raise errors.bad_request.InvalidModelId(
                "no such public or company model",
                id=model_id,
                company=call.identity.company,
            )
        call.result.data = {"model": res.to_proper_dict()}


@endpoint("models.get_all_ex", required_fields=[])
def get_all_ex(call):
    assert isinstance(call, APICall)

    with translate_errors_context():
        with TimingContext("mongo", "models_get_all_ex"):
            models = Model.get_many_with_join(
                company=call.identity.company,
                query_dict=call.data,
                allow_public=True,
                query_options=get_all_query_options,
            )

        call.result.data = {"models": models}


@endpoint("models.get_all", required_fields=[])
def get_all(call):
    assert isinstance(call, APICall)

    with translate_errors_context():
        with TimingContext("mongo", "models_get_all"):
            models = Model.get_many(
                company=call.identity.company,
                parameters=call.data,
                query_dict=call.data,
                allow_public=True,
                query_options=get_all_query_options,
            )

        call.result.data = {"models": models}


create_fields = {
    "name": None,
    "tags": list,
    "task": Task,
    "comment": None,
    "uri": None,
    "project": Project,
    "parent": Model,
    "framework": None,
    "design": None,
    "labels": dict,
    "ready": None,
}

schemes = list(SupportedURLField.schemes)


def _validate_uri(uri):
    parsed_uri = urlparse(uri)
    if parsed_uri.scheme not in schemes:
        raise errors.bad_request.InvalidModelUri("unsupported scheme", uri=uri)
    elif not parsed_uri.path:
        raise errors.bad_request.InvalidModelUri("missing path", uri=uri)


def parse_model_fields(call, valid_fields):
    fields = parse_from_call(call.data, valid_fields, Model.get_fields())
    tags = fields.get("tags")
    if tags:
        fields["tags"] = list(set(tags))
    return fields


@endpoint("models.update_for_task", required_fields=["task"])
def update_for_task(call, company_id, _):
    assert isinstance(call, APICall)
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
            _only=["output", "execution", "name", "status", "project"],
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
            query = dict(company=company_id, id=override_model_id)
            model = Model.objects(**query).first()
            if not model:
                raise errors.bad_request.InvalidModelId(**query)
        else:
            if "name" not in call.data:
                # use task name if name not provided
                call.data["name"] = task.name

            if "comment" not in call.data:
                call.data["comment"] = f"Created by task `{task.name}` ({task.id})"

            if task.output and task.output.model:
                # model exists, update
                res = _update_model(call, model_id=task.output.model).to_struct()
                res.update({"id": task.output.model, "created": False})
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
                parent=task.execution.model,
                design=task.execution.model_desc,
                labels=task.execution.model_labels,
                ready=(task.status == TaskStatus.published),
                **fields,
            )
            model.save()

        TaskBLL.update_statistics(
            task_id=task_id,
            company_id=company_id,
            last_iteration_max=iteration,
            output__model=model.id,
        )

        call.result.data = {"id": model.id, "created": True}


@endpoint(
    "models.create",
    request_data_model=CreateModelRequest,
    response_data_model=CreateModelResponse,
)
def create(call, company, req_model):
    assert isinstance(call, APICall)
    assert isinstance(req_model, CreateModelRequest)
    identity = call.identity

    if req_model.public:
        company = ""

    with translate_errors_context():

        project = req_model.project
        if project:
            validate_id(Project, company=company, project=project)

        uri = req_model.uri
        if uri:
            _validate_uri(uri)
        task = req_model.task
        req_data = req_model.to_struct()
        if task:
            validate_task(call, req_data)

        fields = filter_fields(Model, req_data)
        # create and save model
        model = Model(
            id=database.utils.id(),
            user=identity.user,
            company=company,
            created=datetime.utcnow(),
            **fields,
        )
        model.save()

        call.result.data_model = CreateModelResponse(id=model.id, created=True)


def prepare_update_fields(call, fields):
    fields = fields.copy()
    if "uri" in fields:
        _validate_uri(fields["uri"])

        # clear UI cache if URI is provided (model updated)
        fields["ui_cache"] = fields.pop("ui_cache", {})
    if "task" in fields:
        validate_task(call, fields)
    return fields


def validate_task(call, fields):
    Task.get_for_writing(company=call.identity.company, id=fields["task"], _only=["id"])


@endpoint("models.edit", required_fields=["model"], response_data_model=UpdateResponse)
def edit(call):
    assert isinstance(call, APICall)
    identity = call.identity
    model_id = call.data["model"]

    with translate_errors_context():
        query = dict(id=model_id, company=identity.company)
        model = Model.objects(**query).first()
        if not model:
            raise errors.bad_request.InvalidModelId(**query)

        fields = parse_model_fields(call, create_fields)
        fields = prepare_update_fields(call, fields)

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
        task_id = model.task or fields.get('task')
        if task_id and iteration is not None:
            TaskBLL.update_statistics(
                task_id=task_id,
                company_id=identity.company,
                last_iteration_max=iteration,
            )

        if fields:
            updated = model.update(upsert=False, **fields)
            call.result.data_model = UpdateResponse(updated=updated, fields=fields)
        else:
            call.result.data_model = UpdateResponse(updated=0)


def _update_model(call, model_id=None):
    assert isinstance(call, APICall)
    identity = call.identity
    model_id = model_id or call.data["model"]

    with translate_errors_context():
        # get model by id
        query = dict(id=model_id, company=identity.company)
        model = Model.objects(**query).first()
        if not model:
            raise errors.bad_request.InvalidModelId(**query)

        data = prepare_update_fields(call, call.data)

        task_id = data.get("task")
        iteration = data.get("iteration")
        if task_id and iteration is not None:
            TaskBLL.update_statistics(
                task_id=task_id,
                company_id=identity.company,
                last_iteration_max=iteration,
            )

        updated_count, updated_fields = Model.safe_update(
            call.identity.company, model.id, data
        )
        return UpdateResponse(updated=updated_count, fields=updated_fields)


@endpoint(
    "models.update", required_fields=["model"], response_data_model=UpdateResponse
)
def update(call):
    call.result.data_model = _update_model(call)


@endpoint(
    "models.set_ready",
    request_data_model=PublishModelRequest,
    response_data_model=PublishModelResponse,
)
def set_ready(call: APICall, company, req_model: PublishModelRequest):
    updated, published_task_data = TaskBLL.model_set_ready(
        model_id=req_model.model,
        company_id=company,
        publish_task=req_model.publish_task,
        force_publish_task=req_model.force_publish_task
    )

    call.result.data_model = PublishModelResponse(
        updated=updated,
        published_task=ModelTaskPublishResponse(
            **published_task_data
        ) if published_task_data else None
    )


@endpoint("models.delete", required_fields=["model"])
def update(call):
    assert isinstance(call, APICall)
    identity = call.identity
    model_id = call.data["model"]
    force = call.data.get("force", False)

    with translate_errors_context():
        query = dict(id=model_id, company=identity.company)
        model = Model.objects(**query).only("id", "task").first()
        if not model:
            raise errors.bad_request.InvalidModelId(**query)

        deleted_model_id = f"__DELETED__{model_id}"

        using_tasks = Task.objects(execution__model=model_id).only("id")
        if using_tasks:
            if not force:
                raise errors.bad_request.ModelInUse(
                    "as execution model, use force=True to delete",
                    num_tasks=len(using_tasks),
                )
            # update deleted model id in using tasks
            using_tasks.update(
                execution__model=deleted_model_id, upsert=False, multi=True
            )

        if model.task:
            task = Task.objects(id=model.task).first()
            if task and task.status == TaskStatus.published:
                if not force:
                    raise errors.bad_request.ModelCreatingTaskExists(
                        "and published, use force=True to delete", task=model.task
                    )
                task.update(
                    output__model=deleted_model_id,
                    output__error=f"model deleted on {datetime.utcnow().isoformat()}",
                    upsert=False,
                )

        del_count = Model.objects(**query).delete()
        call.result.data = dict(deleted=del_count > 0)
