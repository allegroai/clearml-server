from datetime import datetime
from typing import Callable, Any, Tuple, Union, Sequence

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
from apiserver.database.utils import get_options
from apiserver.utilities.dicts import nested_set

log = config.logger(__file__)
queue_bll = QueueBLL()


def archive_task(
    task: Union[str, Task],
    company_id: str,
    user_id: str,
    status_message: str,
    status_reason: str,
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
                "company",
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
            task,
            company_id=company_id,
            user_id=user_id,
            status_message=status_message,
            status_reason=status_reason,
            remove_from_all_queues=True,
        )
    except APIError:
        # dequeue may fail if the task was not enqueued
        pass

    return task.update(
        status_message=status_message,
        status_reason=status_reason,
        add_to_set__system_tags=EntityVisibility.archived.value,
        last_change=datetime.utcnow(),
        last_changed_by=user_id,
    )


def unarchive_task(
    task: str, company_id: str, user_id: str, status_message: str, status_reason: str,
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
        last_changed_by=user_id,
    )


def dequeue_task(
    task_id: str,
    company_id: str,
    user_id: str,
    status_message: str,
    status_reason: str,
    remove_from_all_queues: bool = False,
    new_status=None,
) -> Tuple[int, dict]:
    if new_status and new_status not in get_options(TaskStatus):
        raise errors.bad_request.ValidationError(f"Invalid task status: {new_status}")

    # get the task without write access to make sure that it actually exists
    task = Task.get(
        id=task_id,
        company=company_id,
        _only=(
            "id",
            "company",
            "execution",
            "status",
            "project",
            "enqueue_status",
        ),
        include_public=True,
    )
    if not task:
        TaskBLL.remove_task_from_all_queues(company_id, task_id=task_id)
        return 1, {"updated": 0}

    res = TaskBLL.dequeue_and_change_status(
        task,
        company_id=company_id,
        user_id=user_id,
        status_message=status_message,
        status_reason=status_reason,
        remove_from_all_queues=remove_from_all_queues,
        new_status=new_status,
    )
    return 1, res


def enqueue_task(
    task_id: str,
    company_id: str,
    user_id: str,
    queue_id: str,
    status_message: str,
    status_reason: str,
    queue_name: str = None,
    validate: bool = False,
    force: bool = False,
) -> Tuple[int, dict]:
    if queue_id and queue_name:
        raise errors.bad_request.ValidationError(
            "Either queue id or queue name should be provided"
        )

    if queue_name:
        queue = queue_bll.get_by_name(
            company_id=company_id, queue_name=queue_name, only=("id",)
        )
        if not queue:
            queue = queue_bll.create(company_id=company_id, name=queue_name)
        queue_id = queue.id

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
        user_id=user_id,
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
            user_id=user_id,
        ).execute(enqueue_status=None)
        raise

    # set the current queue ID in the task
    if task.execution:
        Task.objects(**query).update(execution__queue=queue_id, multi=False)
    else:
        Task.objects(**query).update(execution=Execution(queue=queue_id), multi=False)

    nested_set(res, ("fields", "execution.queue"), queue_id)
    return 1, res


def move_tasks_to_trash(tasks: Sequence[str]) -> int:
    try:
        collection_name = Task._get_collection_name()
        trash_collection_name = f"{collection_name}__trash"
        Task.aggregate(
            [
                {"$match": {"_id": {"$in": tasks}}},
                {
                    "$merge": {
                        "into": trash_collection_name,
                        "on": "_id",
                        "whenMatched": "replace",
                        "whenNotMatched": "insert",
                    }
                },
            ],
            allow_disk_use=True,
        )
    except Exception as ex:
        log.error(f"Error copying tasks to trash {str(ex)}")

    return Task.objects(id__in=tasks).delete()


def delete_task(
    task_id: str,
    company_id: str,
    user_id: str,
    move_to_trash: bool,
    force: bool,
    return_file_urls: bool,
    delete_output_models: bool,
    status_message: str,
    status_reason: str,
    delete_external_artifacts: bool,
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

    try:
        TaskBLL.dequeue_and_change_status(
            task,
            company_id=company_id,
            user_id=user_id,
            status_message=status_message,
            status_reason=status_reason,
            remove_from_all_queues=True,
        )
    except APIError:
        # dequeue may fail if the task was not enqueued
        pass

    cleanup_res = cleanup_task(
        company=company_id,
        user=user_id,
        task=task,
        force=force,
        return_file_urls=return_file_urls,
        delete_output_models=delete_output_models,
        delete_external_artifacts=delete_external_artifacts,
    )

    if move_to_trash:
        # make sure that whatever changes were done to the task are saved
        # the task itself will be deleted later in the move_tasks_to_trash operation
        task.last_update = datetime.utcnow()
        task.save()
    else:
        task.delete()

    update_project_time(task.project)
    return 1, task, cleanup_res


def reset_task(
    task_id: str,
    company_id: str,
    user_id: str,
    force: bool,
    return_file_urls: bool,
    delete_output_models: bool,
    clear_all: bool,
    delete_external_artifacts: bool,
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

    TaskBLL.remove_task_from_all_queues(company_id=company_id, task_id=task.id)

    cleaned_up = cleanup_task(
        company=company_id,
        user=user_id,
        task=task,
        force=force,
        update_children=False,
        return_file_urls=return_file_urls,
        delete_output_models=delete_output_models,
        delete_external_artifacts=delete_external_artifacts,
    )

    updates.update(
        set__last_iteration=DEFAULT_LAST_ITERATION,
        set__last_metrics={},
        set__unique_metrics=[],
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
        user_id=user_id,
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
    user_id: str,
    force: bool,
    publish_model_func: Callable[[str, str, str], Any] = None,
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
                publish_model_func(model.id, company_id, user_id)

        # set task status to published, and update (or set) it's new output (view and models)
        return ChangeStatusRequest(
            task=task,
            new_status=TaskStatus.published,
            force=force,
            status_reason=status_reason,
            status_message=status_message,
            user_id=user_id,
        ).execute(published=datetime.utcnow(), output=output)

    except Exception as ex:
        publish_failed = True
        raise ex
    finally:
        if publish_failed:
            task.status = previous_task_status
            task.save()


def stop_task(
    task_id: str,
    company_id: str,
    user_id: str,
    user_name: str,
    status_reason: str,
    force: bool,
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
            "execution.queue",
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
        user_id=user_id,
    ).execute()
