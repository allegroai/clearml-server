from copy import deepcopy
from datetime import datetime
from typing import Sequence, Union, Tuple

import attr
import dpath
from mongoengine import EmbeddedDocument, Q
from mongoengine.queryset.transform import COMPARISON_OPERATORS
from pymongo import UpdateOne

from apiserver.apierrors import errors, APIError
from apiserver.apierrors.errors.bad_request import InvalidTaskId
from apiserver.apimodels.base import (
    UpdateResponse,
    IdResponse,
    MakePublicRequest,
    MoveRequest,
)
from apiserver.apimodels.tasks import (
    StartedResponse,
    ResetResponse,
    PublishRequest,
    PublishResponse,
    CreateRequest,
    UpdateRequest,
    SetRequirementsRequest,
    TaskRequest,
    DeleteRequest,
    PingRequest,
    EnqueueRequest,
    EnqueueResponse,
    DequeueResponse,
    CloneRequest,
    AddOrUpdateArtifactsRequest,
    GetTypesRequest,
    ResetRequest,
    GetHyperParamsRequest,
    EditHyperParamsRequest,
    DeleteHyperParamsRequest,
    GetConfigurationsRequest,
    EditConfigurationRequest,
    DeleteConfigurationRequest,
    GetConfigurationNamesRequest,
    DeleteArtifactsRequest,
    ArchiveResponse,
    ArchiveRequest,
    AddUpdateModelRequest,
    DeleteModelsRequest,
    ModelItemType,
)
from apiserver.bll.event import EventBLL
from apiserver.bll.organization import OrgBLL, Tags
from apiserver.bll.project import ProjectBLL, project_ids_with_children
from apiserver.bll.queue import QueueBLL
from apiserver.bll.task import (
    TaskBLL,
    ChangeStatusRequest,
    update_project_time,
)
from apiserver.bll.task.artifacts import (
    artifacts_prepare_for_save,
    artifacts_unprepare_from_saved,
    Artifacts,
)
from apiserver.bll.task.hyperparams import HyperParams
from apiserver.bll.task.non_responsive_tasks_watchdog import NonResponsiveTasksWatchdog
from apiserver.bll.task.param_utils import (
    params_prepare_for_save,
    params_unprepare_from_saved,
    escape_paths,
)
from apiserver.bll.task.task_cleanup import cleanup_task
from apiserver.bll.task.utils import update_task, deleted_prefix
from apiserver.bll.util import SetFieldsResolver
from apiserver.database.errors import translate_errors_context
from apiserver.database.model import EntityVisibility
from apiserver.database.model.task.output import Output
from apiserver.database.model.task.task import (
    Task,
    TaskStatus,
    Script,
    DEFAULT_LAST_ITERATION,
    Execution,
    ArtifactModes,
    ModelItem,
)
from apiserver.database.utils import get_fields_attr, parse_from_call, get_options
from apiserver.service_repo import APICall, endpoint
from apiserver.services.utils import (
    conform_tag_fields,
    conform_output_tags,
    ModelsBackwardsCompatibility,
    DockerCmdBackwardsCompatibility,
)
from apiserver.timing_context import TimingContext
from apiserver.utilities.partial_version import PartialVersion

task_fields = set(Task.get_fields())
task_script_stripped_fields = set(
    [f for f, v in get_fields_attr(Script, "strip").items() if v]
)

task_bll = TaskBLL()
event_bll = EventBLL()
queue_bll = QueueBLL()
org_bll = OrgBLL()
project_bll = ProjectBLL()

NonResponsiveTasksWatchdog.start()


def set_task_status_from_call(
    request: UpdateRequest, company_id, new_status=None, **set_fields
) -> dict:
    fields_resolver = SetFieldsResolver(set_fields)
    task = TaskBLL.get_task_with_access(
        request.task,
        company_id=company_id,
        only=tuple(
            {"status", "project", "started", "duration"} | fields_resolver.get_names()
        ),
        requires_write_access=True,
    )

    if "duration" not in fields_resolver.get_names():
        if new_status == Task.started:
            fields_resolver.add_fields(min__duration=max(0, task.duration or 0))
        elif new_status in (
            TaskStatus.completed,
            TaskStatus.failed,
            TaskStatus.stopped,
        ):
            fields_resolver.add_fields(
                duration=int((task.started - datetime.utcnow()).total_seconds())
                if task.started
                else 0
            )

    status_reason = request.status_reason
    status_message = request.status_message
    force = request.force
    return ChangeStatusRequest(
        task=task,
        new_status=new_status or task.status,
        status_reason=status_reason,
        status_message=status_message,
        force=force,
    ).execute(**fields_resolver.get_fields(task))


@endpoint("tasks.get_by_id", request_data_model=TaskRequest)
def get_by_id(call: APICall, company_id, req_model: TaskRequest):
    task = TaskBLL.get_task_with_access(
        req_model.task, company_id=company_id, allow_public=True
    )
    task_dict = task.to_proper_dict()
    unprepare_from_saved(call, task_dict)
    call.result.data = {"task": task_dict}


def escape_execution_parameters(call: APICall) -> dict:
    if not call.data:
        return call.data

    keys = list(call.data)
    call_data = {
        safe_key: call.data[key] for key, safe_key in zip(keys, escape_paths(keys))
    }

    projection = Task.get_projection(call_data)
    if projection:
        Task.set_projection(call_data, escape_paths(projection))

    ordering = Task.get_ordering(call_data)
    if ordering:
        Task.set_ordering(call_data, escape_paths(ordering))

    return call_data


def _process_include_subprojects(call_data: dict):
    include_subprojects = call_data.pop("include_subprojects", False)
    project_ids = call_data.get("project")
    if not project_ids or not include_subprojects:
        return

    if not isinstance(project_ids, list):
        project_ids = [project_ids]
    call_data["project"] = project_ids_with_children(project_ids)


@endpoint("tasks.get_all_ex", required_fields=[])
def get_all_ex(call: APICall, company_id, _):
    conform_tag_fields(call, call.data)

    call_data = escape_execution_parameters(call)

    with translate_errors_context():
        with TimingContext("mongo", "task_get_all_ex"):
            _process_include_subprojects(call_data)
            tasks = Task.get_many_with_join(
                company=company_id,
                query_dict=call_data,
                allow_public=True,  # required in case projection is requested for public dataset/versions
            )
        unprepare_from_saved(call, tasks)
        call.result.data = {"tasks": tasks}


@endpoint("tasks.get_by_id_ex", required_fields=["id"])
def get_by_id_ex(call: APICall, company_id, _):
    conform_tag_fields(call, call.data)

    call_data = escape_execution_parameters(call)

    with translate_errors_context():
        with TimingContext("mongo", "task_get_by_id_ex"):
            tasks = Task.get_many_with_join(
                company=company_id, query_dict=call_data, allow_public=True,
            )

        unprepare_from_saved(call, tasks)
        call.result.data = {"tasks": tasks}


@endpoint("tasks.get_all", required_fields=[])
def get_all(call: APICall, company_id, _):
    conform_tag_fields(call, call.data)

    call_data = escape_execution_parameters(call)

    with translate_errors_context():
        with TimingContext("mongo", "task_get_all"):
            tasks = Task.get_many(
                company=company_id,
                parameters=call_data,
                query_dict=call_data,
                allow_public=True,  # required in case projection is requested for public dataset/versions
            )
        unprepare_from_saved(call, tasks)
        call.result.data = {"tasks": tasks}


@endpoint("tasks.get_types", request_data_model=GetTypesRequest)
def get_types(call: APICall, company_id, request: GetTypesRequest):
    call.result.data = {
        "types": list(
            project_bll.get_task_types(company_id, project_ids=request.projects)
        )
    }


@endpoint(
    "tasks.stop", request_data_model=UpdateRequest, response_data_model=UpdateResponse
)
def stop(call: APICall, company_id, req_model: UpdateRequest):
    """
    stop
    :summary: Stop a running task. Requires task status 'in_progress' and
              execution_progress 'running', or force=True.
              Development task is stopped immediately. For a non-development task
              only its status message is set to 'stopping'

    """
    call.result.data_model = UpdateResponse(
        **TaskBLL.stop_task(
            task_id=req_model.task,
            company_id=company_id,
            user_name=call.identity.user_name,
            status_reason=req_model.status_reason,
            force=req_model.force,
        )
    )


@endpoint(
    "tasks.stopped",
    request_data_model=UpdateRequest,
    response_data_model=UpdateResponse,
)
def stopped(call: APICall, company_id, req_model: UpdateRequest):
    call.result.data_model = UpdateResponse(
        **set_task_status_from_call(
            req_model,
            company_id,
            new_status=TaskStatus.stopped,
            completed=datetime.utcnow(),
        )
    )


@endpoint(
    "tasks.started",
    request_data_model=UpdateRequest,
    response_data_model=StartedResponse,
)
def started(call: APICall, company_id, req_model: UpdateRequest):
    res = StartedResponse(
        **set_task_status_from_call(
            req_model,
            company_id,
            new_status=TaskStatus.in_progress,
            min__started=datetime.utcnow(),  # don't override a previous, smaller "started" field value
        )
    )
    res.started = res.updated
    call.result.data_model = res


@endpoint(
    "tasks.failed", request_data_model=UpdateRequest, response_data_model=UpdateResponse
)
def failed(call: APICall, company_id, req_model: UpdateRequest):
    call.result.data_model = UpdateResponse(
        **set_task_status_from_call(req_model, company_id, new_status=TaskStatus.failed)
    )


@endpoint(
    "tasks.close", request_data_model=UpdateRequest, response_data_model=UpdateResponse
)
def close(call: APICall, company_id, req_model: UpdateRequest):
    call.result.data_model = UpdateResponse(
        **set_task_status_from_call(req_model, company_id, new_status=TaskStatus.closed)
    )


create_fields = {
    "name": None,
    "tags": list,
    "system_tags": list,
    "type": None,
    "error": None,
    "comment": None,
    "parent": Task,
    "project": None,
    "input": None,
    "models": None,
    "container": None,
    "output_dest": None,
    "execution": None,
    "hyperparams": None,
    "configuration": None,
    "script": None,
}


def prepare_for_save(call: APICall, fields: dict, previous_task: Task = None):
    conform_tag_fields(call, fields, validate=True)
    params_prepare_for_save(fields, previous_task=previous_task)
    artifacts_prepare_for_save(fields)
    ModelsBackwardsCompatibility.prepare_for_save(call, fields)
    DockerCmdBackwardsCompatibility.prepare_for_save(call, fields)

    # Strip all script fields (remove leading and trailing whitespace chars) to avoid unusable names and paths
    for field in task_script_stripped_fields:
        try:
            path = f"script/{field}"
            value = dpath.get(fields, path)
            if isinstance(value, str):
                value = value.strip()
            dpath.set(fields, path, value)
        except KeyError:
            pass

    return fields


def unprepare_from_saved(call: APICall, tasks_data: Union[Sequence[dict], dict]):
    if isinstance(tasks_data, dict):
        tasks_data = [tasks_data]

    conform_output_tags(call, tasks_data)
    ModelsBackwardsCompatibility.unprepare_from_saved(call, tasks_data)
    DockerCmdBackwardsCompatibility.unprepare_from_saved(call, tasks_data)

    need_legacy_params = call.requested_endpoint_version < PartialVersion("2.9")

    for data in tasks_data:
        params_unprepare_from_saved(
            fields=data, copy_to_legacy=need_legacy_params,
        )
        artifacts_unprepare_from_saved(fields=data)


def prepare_create_fields(
    call: APICall, valid_fields=None, output=None, previous_task: Task = None,
):
    valid_fields = valid_fields if valid_fields is not None else create_fields
    t_fields = task_fields
    t_fields.add("output_dest")

    fields = parse_from_call(call.data, valid_fields, t_fields)

    # Move output_dest to output.destination
    output_dest = fields.get("output_dest")
    if output_dest is not None:
        fields.pop("output_dest")
        if output:
            output.destination = output_dest
        else:
            output = Output(destination=output_dest)
        fields["output"] = output

    # Add models updated time
    models = fields.get("models")
    if models:
        now = datetime.utcnow()
        for field in ("input", "output"):
            field_models = models.get(field)
            if not field_models:
                continue
            for model in field_models:
                model["updated"] = now

    return prepare_for_save(call, fields, previous_task=previous_task)


def _validate_and_get_task_from_call(call: APICall, **kwargs) -> Tuple[Task, dict]:
    with translate_errors_context(
        field_does_not_exist_cls=errors.bad_request.ValidationError
    ), TimingContext("code", "parse_call"):
        fields = prepare_create_fields(call, **kwargs)
        task = task_bll.create(call, fields)

    with TimingContext("code", "validate"):
        task_bll.validate(task)

    return task, fields


@endpoint("tasks.validate", request_data_model=CreateRequest)
def validate(call: APICall, company_id, req_model: CreateRequest):
    parent = call.data.get("parent")
    if parent and parent.startswith(deleted_prefix):
        call.data.pop("parent")
    _validate_and_get_task_from_call(call)


def _update_cached_tags(company: str, project: str, fields: dict):
    org_bll.update_tags(
        company,
        Tags.Task,
        project=project,
        tags=fields.get("tags"),
        system_tags=fields.get("system_tags"),
    )


def _reset_cached_tags(company: str, projects: Sequence[str]):
    org_bll.reset_tags(company, Tags.Task, projects=projects)


@endpoint(
    "tasks.create", request_data_model=CreateRequest, response_data_model=IdResponse
)
def create(call: APICall, company_id, req_model: CreateRequest):
    task, fields = _validate_and_get_task_from_call(call)

    with translate_errors_context(), TimingContext("mongo", "save_task"):
        task.save()
        _update_cached_tags(company_id, project=task.project, fields=fields)
        update_project_time(task.project)

    call.result.data_model = IdResponse(id=task.id)


@endpoint("tasks.clone", request_data_model=CloneRequest)
def clone_task(call: APICall, company_id, request: CloneRequest):
    task, new_project = task_bll.clone_task(
        company_id=company_id,
        user_id=call.identity.user,
        task_id=request.task,
        name=request.new_task_name,
        comment=request.new_task_comment,
        parent=request.new_task_parent,
        project=request.new_task_project,
        tags=request.new_task_tags,
        system_tags=request.new_task_system_tags,
        hyperparams=request.new_task_hyperparams,
        configuration=request.new_task_configuration,
        container=request.new_task_container,
        execution_overrides=request.execution_overrides,
        input_models=request.new_task_input_models,
        validate_references=request.validate_references,
        new_project_name=request.new_project_name,
    )
    call.result.data = {
        "id": task.id,
        **({"new_project": new_project} if new_project else {}),
    }


def prepare_update_fields(call: APICall, task, call_data):
    valid_fields = deepcopy(Task.user_set_allowed())
    update_fields = {k: v for k, v in create_fields.items() if k in valid_fields}
    update_fields["output__error"] = None
    t_fields = task_fields
    t_fields.add("output__error")
    fields = parse_from_call(call_data, update_fields, t_fields)
    return prepare_for_save(call, fields), valid_fields


@endpoint(
    "tasks.update", request_data_model=UpdateRequest, response_data_model=UpdateResponse
)
def update(call: APICall, company_id, req_model: UpdateRequest):
    task_id = req_model.task

    with translate_errors_context():
        task = Task.get_for_writing(
            id=task_id, company=company_id, _only=["id", "project"]
        )
        if not task:
            raise errors.bad_request.InvalidTaskId(id=task_id)

        partial_update_dict, valid_fields = prepare_update_fields(call, task, call.data)

        if not partial_update_dict:
            return UpdateResponse(updated=0)

        updated_count, updated_fields = Task.safe_update(
            company_id=company_id,
            id=task_id,
            partial_update_dict=partial_update_dict,
            injected_update=dict(last_change=datetime.utcnow()),
        )
        if updated_count:
            new_project = updated_fields.get("project", task.project)
            if new_project != task.project:
                _reset_cached_tags(company_id, projects=[new_project, task.project])
            else:
                _update_cached_tags(
                    company_id, project=task.project, fields=updated_fields
                )
            update_project_time(updated_fields.get("project"))
        unprepare_from_saved(call, updated_fields)
        return UpdateResponse(updated=updated_count, fields=updated_fields)


@endpoint(
    "tasks.set_requirements",
    request_data_model=SetRequirementsRequest,
    response_data_model=UpdateResponse,
)
def set_requirements(call: APICall, company_id, req_model: SetRequirementsRequest):
    requirements = req_model.requirements
    with translate_errors_context():
        task = TaskBLL.get_task_with_access(
            req_model.task,
            company_id=company_id,
            only=("status", "script"),
            requires_write_access=True,
        )
        if not task.script:
            raise errors.bad_request.MissingTaskFields(
                "Task has no script field", task=task.id
            )
        res = update_task(task, update_cmds=dict(script__requirements=requirements))
        call.result.data_model = UpdateResponse(updated=res)
        if res:
            call.result.data_model.fields = {"script.requirements": requirements}


@endpoint("tasks.update_batch")
def update_batch(call: APICall, company_id, _):
    items = call.batched_data
    if items is None:
        raise errors.bad_request.BatchContainsNoItems()

    with translate_errors_context():
        items = {i["task"]: i for i in items}
        tasks = {
            t.id: t
            for t in Task.get_many_for_writing(
                company=company_id, query=Q(id__in=list(items))
            )
        }

        if len(tasks) < len(items):
            missing = tuple(set(items).difference(tasks))
            raise errors.bad_request.InvalidTaskId(ids=missing)

        now = datetime.utcnow()

        bulk_ops = []
        updated_projects = set()
        for id, data in items.items():
            task = tasks[id]
            fields, valid_fields = prepare_update_fields(call, task, data)
            partial_update_dict = Task.get_safe_update_dict(fields)
            if not partial_update_dict:
                continue
            partial_update_dict.update(last_change=now)
            update_op = UpdateOne(
                {"_id": id, "company": company_id}, {"$set": partial_update_dict}
            )
            bulk_ops.append(update_op)

            new_project = partial_update_dict.get("project", task.project)
            if new_project != task.project:
                updated_projects.update({new_project, task.project})
            elif any(f in partial_update_dict for f in ("tags", "system_tags")):
                updated_projects.add(task.project)

        updated = 0
        if bulk_ops:
            res = Task._get_collection().bulk_write(bulk_ops)
            updated = res.modified_count

        if updated and updated_projects:
            projects = list(updated_projects)
            _reset_cached_tags(company_id, projects=projects)
            update_project_time(project_ids=projects)

        call.result.data = {"updated": updated}


@endpoint(
    "tasks.edit", request_data_model=UpdateRequest, response_data_model=UpdateResponse
)
def edit(call: APICall, company_id, req_model: UpdateRequest):
    task_id = req_model.task
    force = req_model.force

    with translate_errors_context():
        task = Task.get_for_writing(id=task_id, company=company_id)
        if not task:
            raise errors.bad_request.InvalidTaskId(id=task_id)

        if not force and task.status != TaskStatus.created:
            raise errors.bad_request.InvalidTaskStatus(
                expected=TaskStatus.created, status=task.status
            )

        edit_fields = create_fields.copy()
        edit_fields.update(dict(status=None))

        with translate_errors_context(
            field_does_not_exist_cls=errors.bad_request.ValidationError
        ), TimingContext("code", "parse_and_validate"):
            fields = prepare_create_fields(
                call, valid_fields=edit_fields, output=task.output, previous_task=task
            )

        for key in fields:
            field = getattr(task, key, None)
            value = fields[key]
            if (
                field
                and isinstance(value, dict)
                and isinstance(field, EmbeddedDocument)
            ):
                d = field.to_mongo(use_db_field=False).to_dict()
                d.update(value)
                fields[key] = d

        task_bll.validate(task_bll.create(call, fields))

        # make sure field names do not end in mongoengine comparison operators
        fixed_fields = {
            (k if k not in COMPARISON_OPERATORS else "%s__" % k): v
            for k, v in fields.items()
        }
        if fixed_fields:
            now = datetime.utcnow()
            last_change = dict(last_change=now)
            if not set(fields).issubset(Task.user_set_allowed()):
                last_change.update(last_update=now)
            fields.update(**last_change)
            fixed_fields.update(**last_change)
            updated = task.update(upsert=False, **fixed_fields)
            if updated:
                new_project = fixed_fields.get("project", task.project)
                if new_project != task.project:
                    _reset_cached_tags(company_id, projects=[new_project, task.project])
                else:
                    _update_cached_tags(
                        company_id, project=task.project, fields=fixed_fields
                    )
                update_project_time(fields.get("project"))
            unprepare_from_saved(call, fields)
            call.result.data_model = UpdateResponse(updated=updated, fields=fields)
        else:
            call.result.data_model = UpdateResponse(updated=0)


@endpoint(
    "tasks.get_hyper_params", request_data_model=GetHyperParamsRequest,
)
def get_hyper_params(call: APICall, company_id, request: GetHyperParamsRequest):
    with translate_errors_context():
        tasks_params = HyperParams.get_params(company_id, task_ids=request.tasks)

    call.result.data = {
        "params": [{"task": task, **data} for task, data in tasks_params.items()]
    }


@endpoint("tasks.edit_hyper_params", request_data_model=EditHyperParamsRequest)
def edit_hyper_params(call: APICall, company_id, request: EditHyperParamsRequest):
    with translate_errors_context():
        call.result.data = {
            "updated": HyperParams.edit_params(
                company_id,
                task_id=request.task,
                hyperparams=request.hyperparams,
                replace_hyperparams=request.replace_hyperparams,
                force=request.force,
            )
        }


@endpoint("tasks.delete_hyper_params", request_data_model=DeleteHyperParamsRequest)
def delete_hyper_params(call: APICall, company_id, request: DeleteHyperParamsRequest):
    with translate_errors_context():
        call.result.data = {
            "deleted": HyperParams.delete_params(
                company_id,
                task_id=request.task,
                hyperparams=request.hyperparams,
                force=request.force,
            )
        }


@endpoint(
    "tasks.get_configurations", request_data_model=GetConfigurationsRequest,
)
def get_configurations(call: APICall, company_id, request: GetConfigurationsRequest):
    with translate_errors_context():
        tasks_params = HyperParams.get_configurations(
            company_id, task_ids=request.tasks, names=request.names
        )

    call.result.data = {
        "configurations": [
            {"task": task, **data} for task, data in tasks_params.items()
        ]
    }


@endpoint(
    "tasks.get_configuration_names", request_data_model=GetConfigurationNamesRequest,
)
def get_configuration_names(
    call: APICall, company_id, request: GetConfigurationNamesRequest
):
    with translate_errors_context():
        tasks_params = HyperParams.get_configuration_names(
            company_id, task_ids=request.tasks
        )

    call.result.data = {
        "configurations": [
            {"task": task, **data} for task, data in tasks_params.items()
        ]
    }


@endpoint("tasks.edit_configuration", request_data_model=EditConfigurationRequest)
def edit_configuration(call: APICall, company_id, request: EditConfigurationRequest):
    with translate_errors_context():
        call.result.data = {
            "updated": HyperParams.edit_configuration(
                company_id,
                task_id=request.task,
                configuration=request.configuration,
                replace_configuration=request.replace_configuration,
                force=request.force,
            )
        }


@endpoint("tasks.delete_configuration", request_data_model=DeleteConfigurationRequest)
def delete_configuration(
    call: APICall, company_id, request: DeleteConfigurationRequest
):
    with translate_errors_context():
        call.result.data = {
            "deleted": HyperParams.delete_configuration(
                company_id,
                task_id=request.task,
                configuration=request.configuration,
                force=request.force,
            )
        }


@endpoint(
    "tasks.enqueue",
    request_data_model=EnqueueRequest,
    response_data_model=EnqueueResponse,
)
def enqueue(call: APICall, company_id, req_model: EnqueueRequest):
    task_id = req_model.task
    queue_id = req_model.queue
    status_message = req_model.status_message
    status_reason = req_model.status_reason

    if not queue_id:
        # try to get default queue
        queue_id = queue_bll.get_default(company_id).id

    with translate_errors_context():
        query = dict(id=task_id, company=company_id)
        task = Task.get_for_writing(
            _only=("type", "script", "execution", "status", "project", "id"), **query
        )
        if not task:
            raise errors.bad_request.InvalidTaskId(**query)

        res = EnqueueResponse(
            **ChangeStatusRequest(
                task=task,
                new_status=TaskStatus.queued,
                status_reason=status_reason,
                status_message=status_message,
                allow_same_state_transition=False,
            ).execute()
        )

        try:
            queue_bll.add_task(
                company_id=company_id, queue_id=queue_id, task_id=task.id
            )
        except Exception:
            # failed enqueueing, revert to previous state
            ChangeStatusRequest(
                task=task,
                current_status_override=TaskStatus.queued,
                new_status=task.status,
                force=True,
                status_reason="failed enqueueing",
            ).execute()
            raise

        # set the current queue ID in the task
        if task.execution:
            Task.objects(**query).update(execution__queue=queue_id, multi=False)
        else:
            Task.objects(**query).update(
                execution=Execution(queue=queue_id), multi=False
            )

        res.queued = 1
        res.fields.update(**{"execution.queue": queue_id})

        call.result.data_model = res


@endpoint(
    "tasks.dequeue",
    request_data_model=UpdateRequest,
    response_data_model=DequeueResponse,
)
def dequeue(call: APICall, company_id, request: UpdateRequest):
    task = TaskBLL.get_task_with_access(
        request.task,
        company_id=company_id,
        only=("id", "execution", "status", "project"),
        requires_write_access=True,
    )
    res = DequeueResponse(
        **TaskBLL.dequeue_and_change_status(
            task,
            company_id,
            status_message=request.status_message,
            status_reason=request.status_reason,
        )
    )

    res.dequeued = 1
    call.result.data_model = res


@endpoint(
    "tasks.reset", request_data_model=ResetRequest, response_data_model=ResetResponse
)
def reset(call: APICall, company_id, request: ResetRequest):
    task = TaskBLL.get_task_with_access(
        request.task, company_id=company_id, requires_write_access=True
    )

    force = request.force

    if not force and task.status == TaskStatus.published:
        raise errors.bad_request.InvalidTaskStatus(task_id=task.id, status=task.status)

    api_results = {}
    updates = {}

    try:
        dequeued = TaskBLL.dequeue(task, company_id, silent_fail=True)
    except APIError:
        # dequeue may fail if the task was not enqueued
        pass
    else:
        if dequeued:
            api_results.update(dequeued=dequeued)

    cleaned_up = cleanup_task(
        task,
        force=force,
        update_children=False,
        return_file_urls=request.return_file_urls,
        delete_output_models=request.delete_output_models,
    )
    api_results.update(attr.asdict(cleaned_up))

    updates.update(
        set__last_iteration=DEFAULT_LAST_ITERATION,
        set__last_metrics={},
        set__metric_stats={},
        set__models__output=[],
        unset__output__result=1,
        unset__output__error=1,
        unset__last_worker=1,
        unset__last_worker_report=1,
    )

    if request.clear_all:
        updates.update(
            set__execution=Execution(), unset__script=1,
        )
    else:
        updates.update(unset__execution__queue=1)
        if task.execution and task.execution.artifacts:
            updates.update(
                set__execution__artifacts={
                    key: artifact
                    for key, artifact in task.execution.artifacts.items()
                    if artifact.mode == ArtifactModes.input
                }
            )

    res = ResetResponse(
        **ChangeStatusRequest(
            task=task,
            new_status=TaskStatus.created,
            force=force,
            status_reason="reset",
            status_message="reset",
        ).execute(
            started=None,
            completed=None,
            published=None,
            active_duration=None,
            **updates,
        )
    )

    # do not return artifacts since they are not serializable
    res.fields.pop("execution.artifacts", None)

    for key, value in api_results.items():
        setattr(res, key, value)

    call.result.data_model = res


@endpoint(
    "tasks.archive",
    request_data_model=ArchiveRequest,
    response_data_model=ArchiveResponse,
)
def archive(call: APICall, company_id, request: ArchiveRequest):
    archived = 0
    tasks = TaskBLL.assert_exists(
        company_id,
        task_ids=request.tasks,
        only=("id", "execution", "status", "project", "system_tags"),
    )
    for task in tasks:
        try:
            TaskBLL.dequeue_and_change_status(
                task, company_id, request.status_message, request.status_reason,
            )
        except APIError:
            # dequeue may fail if the task was not enqueued
            pass
        task.update(
            status_message=request.status_message,
            status_reason=request.status_reason,
            system_tags=sorted(
                set(task.system_tags) | {EntityVisibility.archived.value}
            ),
            last_change=datetime.utcnow(),
        )

        archived += 1

    call.result.data_model = ArchiveResponse(archived=archived)


@endpoint("tasks.delete", request_data_model=DeleteRequest)
def delete(call: APICall, company_id, request: DeleteRequest):
    task = TaskBLL.get_task_with_access(
        request.task, company_id=company_id, requires_write_access=True
    )

    move_to_trash = request.move_to_trash
    force = request.force

    if task.status != TaskStatus.created and not force:
        raise errors.bad_request.TaskCannotBeDeleted(
            "due to status, use force=True",
            task=task.id,
            expected=TaskStatus.created,
            current=task.status,
        )

    with translate_errors_context():
        result = cleanup_task(
            task,
            force=force,
            return_file_urls=request.return_file_urls,
            delete_output_models=request.delete_output_models,
        )

        if move_to_trash:
            collection_name = task._get_collection_name()
            archived_collection = "{}__trash".format(collection_name)
            task.switch_collection(archived_collection)
            try:
                # A simple save() won't do due to mongoengine caching (nothing will be saved), so we have to force
                # an insert. However, if for some reason such an ID exists, let's make sure we'll keep going.
                with TimingContext("mongo", "save_task"):
                    task.save(force_insert=True)
            except Exception:
                pass
            task.switch_collection(collection_name)

        task.delete()
        _reset_cached_tags(company_id, projects=[task.project])
        update_project_time(task.project)

        call.result.data = dict(deleted=True, **attr.asdict(result))


@endpoint(
    "tasks.publish",
    request_data_model=PublishRequest,
    response_data_model=PublishResponse,
)
def publish(call: APICall, company_id, req_model: PublishRequest):
    call.result.data_model = PublishResponse(
        **TaskBLL.publish_task(
            task_id=req_model.task,
            company_id=company_id,
            publish_model=req_model.publish_model,
            force=req_model.force,
            status_reason=req_model.status_reason,
            status_message=req_model.status_message,
        )
    )


@endpoint(
    "tasks.completed",
    min_version="2.2",
    request_data_model=UpdateRequest,
    response_data_model=UpdateResponse,
)
def completed(call: APICall, company_id, request: PublishRequest):
    call.result.data_model = UpdateResponse(
        **set_task_status_from_call(
            request,
            company_id,
            new_status=TaskStatus.completed,
            completed=datetime.utcnow(),
        )
    )


@endpoint("tasks.ping", request_data_model=PingRequest)
def ping(_, company_id, request: PingRequest):
    TaskBLL.set_last_update(
        task_ids=[request.task], company_id=company_id, last_update=datetime.utcnow()
    )


@endpoint(
    "tasks.add_or_update_artifacts",
    min_version="2.10",
    request_data_model=AddOrUpdateArtifactsRequest,
)
def add_or_update_artifacts(
    call: APICall, company_id, request: AddOrUpdateArtifactsRequest
):
    with translate_errors_context():
        call.result.data = {
            "updated": Artifacts.add_or_update_artifacts(
                company_id=company_id,
                task_id=request.task,
                artifacts=request.artifacts,
                force=request.force,
            )
        }


@endpoint(
    "tasks.delete_artifacts",
    min_version="2.10",
    request_data_model=DeleteArtifactsRequest,
)
def delete_artifacts(call: APICall, company_id, request: DeleteArtifactsRequest):
    with translate_errors_context():
        call.result.data = {
            "deleted": Artifacts.delete_artifacts(
                company_id=company_id,
                task_id=request.task,
                artifact_ids=request.artifacts,
                force=request.force,
            )
        }


@endpoint("tasks.make_public", min_version="2.9", request_data_model=MakePublicRequest)
def make_public(call: APICall, company_id, request: MakePublicRequest):
    with translate_errors_context():
        call.result.data = Task.set_public(
            company_id, request.ids, invalid_cls=InvalidTaskId, enabled=True
        )


@endpoint("tasks.make_private", min_version="2.9", request_data_model=MakePublicRequest)
def make_public(call: APICall, company_id, request: MakePublicRequest):
    with translate_errors_context():
        call.result.data = Task.set_public(
            company_id, request.ids, invalid_cls=InvalidTaskId, enabled=False
        )


@endpoint("tasks.move", request_data_model=MoveRequest)
def move(call: APICall, company_id: str, request: MoveRequest):
    if not (request.project or request.project_name):
        raise errors.bad_request.MissingRequiredFields(
            "project or project_name is required"
        )

    updated_projects = set(
        t.project for t in Task.objects(id__in=request.ids).only("project") if t.project
    )
    project_id = project_bll.move_under_project(
        entity_cls=Task,
        user=call.identity.user,
        company=company_id,
        ids=request.ids,
        project=request.project,
        project_name=request.project_name,
    )

    projects = list(updated_projects | {project_id})
    _reset_cached_tags(company_id, projects=projects)
    update_project_time(projects)

    return {"project_id": project_id}


@endpoint("tasks.add_or_update_model", min_version="2.13")
def add_or_update_model(_: APICall, company_id: str, request: AddUpdateModelRequest):
    TaskBLL.get_task_with_access(
        request.task, company_id=company_id, requires_write_access=True, only=["id"]
    )

    models_field = f"models__{request.type}"
    model = ModelItem(name=request.name, model=request.model, updated=datetime.utcnow())
    query = {"id": request.task, f"{models_field}__name": request.name}
    updated = Task.objects(**query).update_one(**{f"set__{models_field}__S": model})

    updated = TaskBLL.update_statistics(
        task_id=request.task,
        company_id=company_id,
        last_iteration_max=request.iteration,
        **({f"push__{models_field}": model} if not updated else {}),
    )

    return {"updated": updated}


@endpoint("tasks.delete_models", min_version="2.13")
def delete_models(_: APICall, company_id: str, request: DeleteModelsRequest):
    task = TaskBLL.get_task_with_access(
        request.task, company_id=company_id, requires_write_access=True, only=["id"]
    )

    delete_names = {
        type_: [m.name for m in request.models if m.type == type_]
        for type_ in get_options(ModelItemType)
    }
    commands = {
        f"pull__models__{field}__name__in": names
        for field, names in delete_names.items()
        if names
    }

    updated = task.update(last_change=datetime.utcnow(), **commands,)
    return {"updated": updated}
