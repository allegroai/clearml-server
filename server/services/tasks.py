from copy import deepcopy
from datetime import datetime
from operator import attrgetter
from typing import Sequence, Callable, Type, TypeVar

import attr
import dpath
import mongoengine
import six
from mongoengine import EmbeddedDocument, Q
from mongoengine.queryset.transform import COMPARISON_OPERATORS
from pymongo import UpdateOne

from apierrors import errors
from apimodels.base import UpdateResponse
from apimodels.tasks import (
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
)
from bll.event import EventBLL
from bll.task import TaskBLL, ChangeStatusRequest, update_project_time, split_by
from database.errors import translate_errors_context
from database.model.model import Model
from database.model.task.output import Output
from database.model.task.task import Task, TaskStatus, Script, DEFAULT_LAST_ITERATION
from database.utils import get_fields, parse_from_call
from service_repo import APICall, endpoint
from services.utils import conform_tag_fields, conform_output_tags
from timing_context import TimingContext
from utilities import safe_get

task_fields = set(Task.get_fields())
task_script_fields = set(get_fields(Script))
get_all_query_options = Task.QueryParameterOptions(
    list_fields=("id", "user", "tags", "system_tags", "type", "status", "project"),
    datetime_fields=("status_changed",),
    pattern_fields=("name", "comment"),
    fields=("parent",),
)

task_bll = TaskBLL()
event_bll = EventBLL()


TaskBLL.start_non_responsive_tasks_watchdog()


def set_task_status_from_call(
    request: UpdateRequest, company_id, new_status=None, **kwargs
) -> dict:
    task = TaskBLL.get_task_with_access(
        request.task,
        company_id=company_id,
        only=("status", "project"),
        requires_write_access=True,
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
    ).execute(**kwargs)


@endpoint("tasks.get_by_id", request_data_model=TaskRequest)
def get_by_id(call: APICall, company_id, req_model: TaskRequest):
    task = TaskBLL.get_task_with_access(
        req_model.task, company_id=company_id, allow_public=True
    )
    task_dict = task.to_proper_dict()
    conform_output_tags(call, task_dict)
    call.result.data = {"task": task_dict}


@endpoint("tasks.get_all_ex", required_fields=[])
def get_all_ex(call: APICall):
    conform_tag_fields(call, call.data)
    with translate_errors_context():
        with TimingContext("mongo", "task_get_all_ex"):
            tasks = Task.get_many_with_join(
                company=call.identity.company,
                query_dict=call.data,
                query_options=get_all_query_options,
                allow_public=True,  # required in case projection is requested for public dataset/versions
                override_none_ordering=True,
            )
        conform_output_tags(call, tasks)
        call.result.data = {"tasks": tasks}


@endpoint("tasks.get_all", required_fields=[])
def get_all(call: APICall):
    conform_tag_fields(call, call.data)
    with translate_errors_context():
        with TimingContext("mongo", "task_get_all"):
            tasks = Task.get_many(
                company=call.identity.company,
                parameters=call.data,
                query_dict=call.data,
                query_options=get_all_query_options,
                allow_public=True,  # required in case projection is requested for public dataset/versions
                override_none_ordering=True,
            )
        conform_output_tags(call, tasks)
        call.result.data = {"tasks": tasks}


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
            started=datetime.utcnow(),
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
    "output_dest": None,
    "execution": None,
    "script": None,
}


def prepare_create_fields(
    call: APICall, valid_fields=None, output=None, previous_task: Task = None
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

    conform_tag_fields(call, fields)

    # Strip all script fields (remove leading and trailing whitespace chars) to avoid unusable names and paths
    for field in task_script_fields:
        try:
            path = "script/%s" % field
            value = dpath.get(fields, path)
            if isinstance(value, six.string_types):
                value = value.strip()
            dpath.set(fields, path, value)
        except KeyError:
            pass

    parameters = safe_get(fields, "execution/parameters")
    if parameters is not None:
        parameters = {k.strip(): v for k, v in parameters.items()}
        dpath.set(fields, "execution/parameters", parameters)

    return fields


def _validate_and_get_task_from_call(call: APICall, **kwargs):
    with translate_errors_context(
        field_does_not_exist_cls=errors.bad_request.ValidationError
    ), TimingContext("code", "parse_call"):
        fields = prepare_create_fields(call, **kwargs)
        task = task_bll.create(call, fields)

    with TimingContext("code", "validate"):
        task_bll.validate(task)

    return task


@endpoint("tasks.validate", request_data_model=CreateRequest)
def validate(call: APICall, company_id, req_model: CreateRequest):
    _validate_and_get_task_from_call(call)


@endpoint("tasks.create", request_data_model=CreateRequest)
def create(call: APICall, company_id, req_model: CreateRequest):
    task = _validate_and_get_task_from_call(call)

    with translate_errors_context(), TimingContext("mongo", "save_task"):
        task.save()
        update_project_time(task.project)

    call.result.data = {"id": task.id}


def prepare_update_fields(call: APICall, task, call_data):
    valid_fields = deepcopy(task.__class__.user_set_allowed())
    update_fields = {k: v for k, v in create_fields.items() if k in valid_fields}
    update_fields["output__error"] = None
    t_fields = task_fields
    t_fields.add("output__error")
    fields = parse_from_call(call_data, update_fields, t_fields)
    conform_tag_fields(call, fields)
    return fields, valid_fields


@endpoint(
    "tasks.update", request_data_model=UpdateRequest, response_data_model=UpdateResponse
)
def update(call: APICall, company_id, req_model: UpdateRequest):
    task_id = req_model.task

    with translate_errors_context():
        task = Task.get_for_writing(id=task_id, company=company_id, _only=["id"])
        if not task:
            raise errors.bad_request.InvalidTaskId(id=task_id)

        partial_update_dict, valid_fields = prepare_update_fields(call, task, call.data)

        if not partial_update_dict:
            return UpdateResponse(updated=0)

        updated_count, updated_fields = Task.safe_update(
            company_id=company_id,
            id=task_id,
            partial_update_dict=partial_update_dict,
            injected_update=dict(last_update=datetime.utcnow()),
        )

        update_project_time(updated_fields.get("project"))
        conform_output_tags(call, updated_fields)
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
        res = task.update(
            script__requirements=requirements, last_update=datetime.utcnow()
        )
        call.result.data_model = UpdateResponse(updated=res)
        if res:
            call.result.data_model.fields = {"script.requirements": requirements}


@endpoint("tasks.update_batch")
def update_batch(call: APICall):
    identity = call.identity

    items = call.batched_data
    if items is None:
        raise errors.bad_request.BatchContainsNoItems()

    with translate_errors_context():
        items = {i["task"]: i for i in items}
        tasks = {
            t.id: t
            for t in Task.get_many_for_writing(
                company=identity.company, query=Q(id__in=list(items))
            )
        }

        if len(tasks) < len(items):
            missing = tuple(set(items).difference(tasks))
            raise errors.bad_request.InvalidTaskId(ids=missing)

        now = datetime.utcnow()

        bulk_ops = []
        for id, data in items.items():
            fields, valid_fields = prepare_update_fields(call, tasks[id], data)
            partial_update_dict = Task.get_safe_update_dict(fields)
            if not partial_update_dict:
                continue
            partial_update_dict.update(last_update=now)
            update_op = UpdateOne(
                {"_id": id, "company": identity.company}, {"$set": partial_update_dict}
            )
            bulk_ops.append(update_op)

        updated = 0
        if bulk_ops:
            res = Task._get_collection().bulk_write(bulk_ops)
            updated = res.modified_count

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
            fields.update(last_update=now)
            fixed_fields.update(last_update=now)
            updated = task.update(upsert=False, **fixed_fields)
            update_project_time(fields.get("project"))
            conform_output_tags(call, fields)
            call.result.data_model = UpdateResponse(updated=updated, fields=fields)
        else:
            call.result.data_model = UpdateResponse(updated=0)


@endpoint(
    "tasks.reset", request_data_model=UpdateRequest, response_data_model=ResetResponse
)
def reset(call: APICall, company_id, req_model: UpdateRequest):
    task = TaskBLL.get_task_with_access(
        req_model.task, company_id=company_id, requires_write_access=True
    )

    force = req_model.force

    if not force and task.status == TaskStatus.published:
        raise errors.bad_request.InvalidTaskStatus(task_id=task.id, status=task.status)

    api_results = {}
    updates = {}

    cleaned_up = cleanup_task(task, force)
    api_results.update(attr.asdict(cleaned_up))

    updates.update(
        set__last_iteration=DEFAULT_LAST_ITERATION,
        set__last_metrics={},
        unset__output__result=1,
        unset__output__model=1,
        __raw__={"$pull": {"execution.artifacts": {"mode": {"$ne": "input"}}}},
    )

    res = ResetResponse(
        **ChangeStatusRequest(
            task=task,
            new_status=TaskStatus.created,
            force=force,
            status_reason="reset",
            status_message="reset",
        ).execute(started=None, completed=None, published=None, **updates)
    )

    for key, value in api_results.items():
        setattr(res, key, value)

    call.result.data_model = res


class DocumentGroup(list):
    """
    Operate on a list of documents as if they were a query result
    """

    def __init__(self, document_type, documents):
        super(DocumentGroup, self).__init__(documents)
        self.type = document_type

    def objects(self, *args, **kwargs):
        return self.type.objects(id__in=[obj.id for obj in self], *args, **kwargs)


T = TypeVar("T")


class TaskOutputs(object):
    """
    Split task outputs of the same type by the ready state
    """

    published = None  # type: DocumentGroup
    draft = None  # type: DocumentGroup

    def __init__(self, is_published, document_type, children):
        # type: (Callable[[T], bool], Type[mongoengine.Document], Sequence[T]) -> ()
        """
        :param is_published: predicate returning whether items is considered published
        :param document_type: type of output
        :param children: output documents
        """
        self.published, self.draft = map(
            lambda x: DocumentGroup(document_type, x), split_by(is_published, children)
        )


@attr.s
class CleanupResult(object):
    """
    Counts of objects modified in task cleanup operation
    """

    updated_children = attr.ib(type=int)
    updated_models = attr.ib(type=int)
    deleted_models = attr.ib(type=int)


def cleanup_task(task, force=False):
    # type: (Task, bool) -> CleanupResult
    """
    Validate task deletion and delete/modify all its output.
    :param task: task object
    :param force: whether to delete task with published outputs
    :return: count of delete and modified items
    """
    models, child_tasks = get_outputs_for_deletion(task, force)
    deleted_task_id = trash_task_id(task.id)
    if child_tasks:
        with TimingContext("mongo", "update_task_children"):
            updated_children = child_tasks.update(parent=deleted_task_id)
    else:
        updated_children = 0

    if models.draft:
        with TimingContext("mongo", "delete_models"):
            deleted_models = models.draft.objects().delete()
    else:
        deleted_models = 0

    if models.published:
        with TimingContext("mongo", "update_task_models"):
            updated_models = models.published.objects().update(task=deleted_task_id)
    else:
        updated_models = 0

    event_bll.delete_task_events(task.company, task.id)

    return CleanupResult(
        deleted_models=deleted_models,
        updated_children=updated_children,
        updated_models=updated_models,
    )


def get_outputs_for_deletion(task, force=False):
    with TimingContext("mongo", "get_task_models"):
        models = TaskOutputs(
            attrgetter("ready"),
            Model,
            Model.objects(task=task.id).only("id", "task", "ready"),
        )
        if not force and models.published:
            raise errors.bad_request.TaskCannotBeDeleted(
                "has output models, use force=True",
                task=task.id,
                models=len(models.published),
            )

    if task.output.model:
        output_model = get_output_model(task, force)
        if output_model:
            if output_model.ready:
                models.published.append(output_model)
            else:
                models.draft.append(output_model)

    with TimingContext("mongo", "get_task_children"):
        tasks = Task.objects(parent=task.id).only("id", "parent", "status")
        published_tasks = [
            task for task in tasks if task.status == TaskStatus.published
        ]
        if not force and published_tasks:
            raise errors.bad_request.TaskCannotBeDeleted(
                "has children, use force=True", task=task.id, children=published_tasks
            )
    return models, tasks


def get_output_model(task, force=False):
    with TimingContext("mongo", "get_task_output_model"):
        output_model = Model.objects(id=task.output.model).first()
    if output_model and output_model.ready and not force:
        raise errors.bad_request.TaskCannotBeDeleted(
            "has output model, use force=True", task=task.id, model=task.output.model
        )
    return output_model


def trash_task_id(task_id):
    return "__DELETED__{}".format(task_id)


@endpoint("tasks.delete", request_data_model=DeleteRequest)
def delete(call: APICall, company_id, req_model: DeleteRequest):
    task = TaskBLL.get_task_with_access(
        req_model.task, company_id=company_id, requires_write_access=True
    )

    move_to_trash = req_model.move_to_trash
    force = req_model.force

    if task.status != TaskStatus.created and not force:
        raise errors.bad_request.TaskCannotBeDeleted(
            "due to status, use force=True",
            task=task.id,
            expected=TaskStatus.created,
            current=task.status,
        )

    with translate_errors_context():
        result = cleanup_task(task, force)

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
