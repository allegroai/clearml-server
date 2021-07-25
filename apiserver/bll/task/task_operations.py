from datetime import datetime
from typing import Callable, Any, Tuple, Union

from apiserver.apierrors import errors, APIError
from apiserver.bll.queue import QueueBLL
from apiserver.bll.task import (
    TaskBLL,
    validate_status_change,
    ChangeStatusRequest,
    update_project_time,
)
from apiserver.bll.task.task_cleanup import cleanup_task, CleanupResult
from apiserver.config_repo import config
from apiserver.database.model import EntityVisibility
from apiserver.database.model.model import Model
from apiserver.database.model.task.output import Output
from apiserver.database.model.task.task import (
    TaskStatus,
    Task,
    TaskSystemTags,
    TaskStatusMessage,
    ArtifactModes,
    Execution,
    DEFAULT_LAST_ITERATION,
)
from apiserver.utilities.dicts import nested_set

queue_bll = QueueBLL()


def archive_task(
    task: Union[str, Task], company_id: str, status_message: str, status_reason: str,
) -> int:
    """
    Deque and archive task
    Return 1 if successful
    """
    if isinstance(task, str):
        task = TaskBLL.get_task_with_access(
            task,
            company_id=company_id,
            only=(
                "id",
                "execution",
                "status",
                "project",
                "system_tags",
                "enqueue_status",
            ),
            requires_write_access=True,
        )
    try:
        TaskBLL.dequeue_and_change_status(
            task, company_id, status_message, status_reason,
        )
    except APIError:
        # dequeue may fail if the task was not enqueued
        pass

    return task.update(
        status_message=status_message,
        status_reason=status_reason,
        add_to_set__system_tags=EntityVisibility.archived.value,
        last_change=datetime.utcnow(),
    )


def unarchive_task(
    task: str, company_id: str, status_message: str, status_reason: str,
) -> int:
    """
    Unarchive task. Return 1 if successful
    """
    task = TaskBLL.get_task_with_access(
        task, company_id=company_id, only=("id",), requires_write_access=True,
    )
    return task.update(
        status_message=status_message,
        status_reason=status_reason,
        pull__system_tags=EntityVisibility.archived.value,
        last_change=datetime.utcnow(),
    )


def dequeue_task(
    task_id: str,
    company_id: str,
    status_message: str,
    status_reason: str,
) -> Tuple[int, dict]:
    query = dict(id=task_id, company=company_id)
    task = Task.get_for_writing(**query)
    if not task:
        raise errors.bad_request.InvalidTaskId(**query)

    res = TaskBLL.dequeue_and_change_status(
        task,
        company_id,
        status_message=status_message,
        status_reason=status_reason,
    )
    return 1, res


def enqueue_task(
    task_id: str,
    company_id: str,
    queue_id: str,
    status_message: str,
    status_reason: str,
    validate: bool = False,
    force: bool = False,
) -> Tuple[int, dict]:
    if not queue_id:
        # try to get default queue
        queue_id = queue_bll.get_default(company_id).id

    query = dict(id=task_id, company=company_id)
    task = Task.get_for_writing(**query)
    if not task:
        raise errors.bad_request.InvalidTaskId(**query)

    if validate:
        TaskBLL.validate(task)

    res = ChangeStatusRequest(
        task=task,
        new_status=TaskStatus.queued,
        status_reason=status_reason,
        status_message=status_message,
        allow_same_state_transition=False,
        force=force,
    ).execute(enqueue_status=task.status)

    try:
        queue_bll.add_task(company_id=company_id, queue_id=queue_id, task_id=task.id)
    except Exception:
        # failed enqueueing, revert to previous state
        ChangeStatusRequest(
            task=task,
            current_status_override=TaskStatus.queued,
            new_status=task.status,
            force=True,
            status_reason="failed enqueueing",
        ).execute(enqueue_status=None)
        raise

    # set the current queue ID in the task
    if task.execution:
        Task.objects(**query).update(execution__queue=queue_id, multi=False)
    else:
        Task.objects(**query).update(execution=Execution(queue=queue_id), multi=False)

    nested_set(res, ("fields", "execution.queue"), queue_id)
    return 1, res


def delete_task(
    task_id: str,
    company_id: str,
    move_to_trash: bool,
    force: bool,
    return_file_urls: bool,
    delete_output_models: bool,
) -> Tuple[int, Task, CleanupResult]:
    task = TaskBLL.get_task_with_access(
        task_id, company_id=company_id, requires_write_access=True
    )

    if (
        task.status != TaskStatus.created
        and EntityVisibility.archived.value not in task.system_tags
        and not force
    ):
        raise errors.bad_request.TaskCannotBeDeleted(
            "due to status, use force=True",
            task=task.id,
            expected=TaskStatus.created,
            current=task.status,
        )

    cleanup_res = cleanup_task(
        task,
        force=force,
        return_file_urls=return_file_urls,
        delete_output_models=delete_output_models,
    )

    if move_to_trash:
        collection_name = task._get_collection_name()
        archived_collection = "{}__trash".format(collection_name)
        task.switch_collection(archived_collection)
        try:
            # A simple save() won't do due to mongoengine caching (nothing will be saved), so we have to force
            # an insert. However, if for some reason such an ID exists, let's make sure we'll keep going.
            task.save(force_insert=True)
        except Exception:
            pass
        task.switch_collection(collection_name)

    task.delete()
    update_project_time(task.project)
    return 1, task, cleanup_res


def reset_task(
    task_id: str,
    company_id: str,
    force: bool,
    return_file_urls: bool,
    delete_output_models: bool,
    clear_all: bool,
) -> Tuple[dict, CleanupResult, dict]:
    task = TaskBLL.get_task_with_access(
        task_id, company_id=company_id, requires_write_access=True
    )

    if not force and task.status == TaskStatus.published:
        raise errors.bad_request.InvalidTaskStatus(task_id=task.id, status=task.status)

    dequeued = {}
    updates = {}

    try:
        dequeued = TaskBLL.dequeue(task, company_id, silent_fail=True)
    except APIError:
        # dequeue may fail if the task was not enqueued
        pass

    cleaned_up = cleanup_task(
        task,
        force=force,
        update_children=False,
        return_file_urls=return_file_urls,
        delete_output_models=delete_output_models,
    )

    updates.update(
        set__last_iteration=DEFAULT_LAST_ITERATION,
        set__last_metrics={},
        set__metric_stats={},
        set__models__output=[],
        set__runtime={},
        unset__output__result=1,
        unset__output__error=1,
        unset__last_worker=1,
        unset__last_worker_report=1,
    )

    if clear_all:
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

    res = ChangeStatusRequest(
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
        enqueue_status=None,
        **updates,
    )

    return dequeued, cleaned_up, res


def publish_task(
    task_id: str,
    company_id: str,
    force: bool,
    publish_model_func: Callable[[str, str], Any] = None,
    status_message: str = "",
    status_reason: str = "",
) -> dict:
    task = TaskBLL.get_task_with_access(
        task_id, company_id=company_id, requires_write_access=True
    )
    if not force:
        validate_status_change(task.status, TaskStatus.published)

    previous_task_status = task.status
    output = task.output or Output()
    publish_failed = False

    try:
        # set state to publishing
        task.status = TaskStatus.publishing
        task.save()

        # publish task models
        if task.models and task.models.output and publish_model_func:
            model_id = task.models.output[-1].model
            model = (
                Model.objects(id=model_id, company=company_id)
                .only("id", "ready")
                .first()
            )
            if model and not model.ready:
                publish_model_func(model.id, company_id)

        # set task status to published, and update (or set) it's new output (view and models)
        return ChangeStatusRequest(
            task=task,
            new_status=TaskStatus.published,
            force=force,
            status_reason=status_reason,
            status_message=status_message,
        ).execute(published=datetime.utcnow(), output=output)

    except Exception as ex:
        publish_failed = True
        raise ex
    finally:
        if publish_failed:
            task.status = previous_task_status
            task.save()


def stop_task(
    task_id: str, company_id: str, user_name: str, status_reason: str, force: bool,
) -> dict:
    """
    Stop a running task. Requires task status 'in_progress' and
    execution_progress 'running', or force=True. Development task or
    task that has no associated worker is stopped immediately.
    For a non-development task with worker only the status message
    is set to 'stopping' to allow the worker to stop the task and report by itself
    :return: updated task fields
    """

    task = TaskBLL.get_task_with_access(
        task_id,
        company_id=company_id,
        only=(
            "status",
            "project",
            "tags",
            "system_tags",
            "last_worker",
            "last_update",
        ),
        requires_write_access=True,
    )

    def is_run_by_worker(t: Task) -> bool:
        """Checks if there is an active worker running the task"""
        update_timeout = config.get("apiserver.workers.task_update_timeout", 600)
        return (
            t.last_worker
            and t.last_update
            and (datetime.utcnow() - t.last_update).total_seconds() < update_timeout
        )

    is_queued = task.status == TaskStatus.queued
    set_stopped = (
        is_queued
        or TaskSystemTags.development in task.system_tags
        or not is_run_by_worker(task)
    )

    if set_stopped:
        if is_queued:
            try:
                TaskBLL.dequeue(task, company_id=company_id, silent_fail=True)
            except APIError:
                # dequeue may fail if the task was not enqueued
                pass

        new_status = TaskStatus.stopped
        status_message = f"Stopped by {user_name}"
    else:
        new_status = task.status
        status_message = TaskStatusMessage.stopping

    return ChangeStatusRequest(
        task=task,
        new_status=new_status,
        status_reason=status_reason,
        status_message=status_message,
        force=force,
    ).execute()
