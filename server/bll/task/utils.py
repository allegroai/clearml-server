from datetime import datetime
from typing import TypeVar, Callable, Tuple, Sequence

import attr
import six

from apierrors import errors
from database.errors import translate_errors_context
from database.model.project import Project
from database.model.task.task import Task, TaskStatus
from database.utils import get_options
from timing_context import TimingContext
from utilities.attrs import typed_attrs

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

    def execute(self, **kwargs):
        current_status = self.task.status
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
        )

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
    TaskStatus.created: {TaskStatus.in_progress},
    TaskStatus.in_progress: {TaskStatus.stopped, TaskStatus.failed, TaskStatus.created},
    TaskStatus.stopped: {
        TaskStatus.closed,
        TaskStatus.created,
        TaskStatus.failed,
        TaskStatus.in_progress,
        TaskStatus.published,
        TaskStatus.publishing,
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
}


def get_possible_status_changes(current_status):
    """
    :param current_status:
    :return possible states from current state
    """
    possible = state_machine.get(current_status)
    assert (
        possible is not None
    ), f"Current status {current_status} not supported by state machine"
    return possible


def update_project_time(project_id):
    if project_id:
        Project.objects(id=project_id).update(last_update=datetime.utcnow())


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
