from datetime import datetime
from typing import TypeVar, Callable, Tuple, Sequence, Union

import attr
import six

from apiserver.apierrors import errors
from apiserver.database.errors import translate_errors_context
from apiserver.database.model.project import Project
from apiserver.database.model.task.task import Task, TaskStatus, TaskSystemTags
from apiserver.database.utils import get_options
from apiserver.timing_context import TimingContext
from apiserver.utilities.attrs import typed_attrs

valid_statuses = get_options(TaskStatus)


@typed_attrs
class ChangeStatusRequest(object):
    task = attr.ib(type=Task)
    new_status = attr.ib(
        type=six.string_types, validator=attr.validators.in_(valid_statuses)
    )
    status_reason = attr.ib(type=six.string_types, default="")
    status_message = attr.ib(type=six.string_types, default="")
    force = attr.ib(type=bool, default=False)
    allow_same_state_transition = attr.ib(type=bool, default=True)
    current_status_override = attr.ib(default=None)

    def execute(self, **kwargs):
        current_status = self.current_status_override or self.task.status
        project_id = self.task.project

        # Verify new status is allowed from current status (will throw exception if not valid)
        self.validate_transition(current_status)

        control = dict(upsert=False, multi=False, write_concern=None, full_result=False)

        now = datetime.utcnow()
        fields = dict(
            status=self.new_status,
            status_reason=self.status_reason,
            status_message=self.status_message,
            status_changed=now,
            last_update=now,
            last_change=now,
        )

        if self.new_status == TaskStatus.queued:
            fields["pull__system_tags"] = TaskSystemTags.development

        def safe_mongoengine_key(key):
            return f"__{key}" if key in control else key

        fields.update({safe_mongoengine_key(k): v for k, v in kwargs.items()})

        with translate_errors_context(), TimingContext("mongo", "task_status"):
            # atomic change of task status by querying the task with the EXPECTED status before modifying it
            params = fields.copy()
            params.update(control)
            updated = Task.objects(id=self.task.id, status=current_status).update(
                **params
            )

        if not updated:
            # failed to change status (someone else beat us to it?)
            raise errors.bad_request.FailedChangingTaskStatus(
                task_id=self.task.id,
                current_status=current_status,
                new_status=self.new_status,
            )

        update_project_time(project_id)

        # make sure that _raw_ queries are not returned back to the client
        fields.pop("__raw__", None)

        return dict(updated=updated, fields=fields)

    def validate_transition(self, current_status):
        if self.force:
            return
        if self.new_status != current_status:
            validate_status_change(current_status, self.new_status)
        elif not self.allow_same_state_transition:
            raise errors.bad_request.InvalidTaskStatus(
                "Task already in requested status",
                current_status=current_status,
                new_status=self.new_status,
            )


def validate_status_change(current_status, new_status):
    assert current_status in valid_statuses
    assert new_status in valid_statuses

    allowed_statuses = get_possible_status_changes(current_status)
    if new_status not in allowed_statuses:
        raise errors.bad_request.InvalidTaskStatus(
            "Invalid status change",
            current_status=current_status,
            new_status=new_status,
        )


state_machine = {
    TaskStatus.created: {TaskStatus.queued, TaskStatus.in_progress},
    TaskStatus.queued: {TaskStatus.created, TaskStatus.in_progress},
    TaskStatus.in_progress: {
        TaskStatus.stopped,
        TaskStatus.failed,
        TaskStatus.created,
        TaskStatus.completed,
    },
    TaskStatus.stopped: {
        TaskStatus.closed,
        TaskStatus.created,
        TaskStatus.failed,
        TaskStatus.in_progress,
        TaskStatus.published,
        TaskStatus.publishing,
        TaskStatus.completed,
    },
    TaskStatus.closed: {
        TaskStatus.created,
        TaskStatus.failed,
        TaskStatus.published,
        TaskStatus.publishing,
        TaskStatus.stopped,
    },
    TaskStatus.failed: {TaskStatus.created, TaskStatus.stopped, TaskStatus.published},
    TaskStatus.publishing: {TaskStatus.published},
    TaskStatus.published: set(),
    TaskStatus.completed: {
        TaskStatus.published,
        TaskStatus.in_progress,
        TaskStatus.created,
    },
}


def get_possible_status_changes(current_status):
    """
    :param current_status:
    :return possible states from current state
    """
    possible = state_machine.get(current_status)
    if possible is None:
        raise errors.server_error.InternalError(
            f"Current status {current_status} not supported by state machine"
        )

    return possible


def update_project_time(project_ids: Union[str, Sequence[str]]):
    if not project_ids:
        return

    if isinstance(project_ids, str):
        project_ids = [project_ids]

    return Project.objects(id__in=project_ids).update(last_update=datetime.utcnow())


T = TypeVar("T")


def split_by(
    condition: Callable[[T], bool], items: Sequence[T]
) -> Tuple[Sequence[T], Sequence[T]]:
    """
    split "items" to two lists by "condition"
    """
    applied = zip(map(condition, items), items)
    return (
        [item for cond, item in applied if cond],
        [item for cond, item in applied if not cond],
    )


def get_task_for_update(
    company_id: str, task_id: str, allow_all_statuses: bool = False, force: bool = False
) -> Task:
    """
    Loads only task id and return the task only if it is updatable (status == 'created')
    """
    task = Task.get_for_writing(company=company_id, id=task_id, _only=("id", "status"))
    if not task:
        raise errors.bad_request.InvalidTaskId(id=task_id)

    if allow_all_statuses:
        return task

    allowed_statuses = (
        [TaskStatus.created, TaskStatus.in_progress] if force else [TaskStatus.created]
    )
    if task.status not in allowed_statuses:
        raise errors.bad_request.InvalidTaskStatus(
            expected=TaskStatus.created, status=task.status
        )
    return task


def update_task(task: Task, update_cmds: dict, set_last_update: bool = True):
    now = datetime.utcnow()
    last_updates = dict(last_change=now)
    if set_last_update:
        last_updates.update(last_update=now)
    return task.update(**update_cmds, **last_updates)
