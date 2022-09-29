from datetime import datetime
from itertools import chain
from operator import attrgetter
from typing import Sequence, Set, Tuple

import attr
from boltons.iterutils import partition, bucketize, first
from mongoengine import NotUniqueError
from pymongo.errors import DuplicateKeyError

from apiserver.apierrors import errors
from apiserver.bll.event import EventBLL
from apiserver.bll.event.event_bll import PlotFields
from apiserver.bll.task.utils import deleted_prefix
from apiserver.config_repo import config
from apiserver.database.model.model import Model
from apiserver.database.model.task.task import Task, TaskStatus, ArtifactModes
from apiserver.database.model.url_to_delete import StorageType, UrlToDelete, FileType, DeletionStatus
from apiserver.timing_context import TimingContext
from apiserver.database.utils import id as db_id

event_bll = EventBLL()


@attr.s(auto_attribs=True)
class TaskUrls:
    model_urls: Sequence[str]
    event_urls: Sequence[str]
    artifact_urls: Sequence[str]

    def __add__(self, other: "TaskUrls"):
        if not other:
            return self

        return TaskUrls(
            model_urls=list(set(self.model_urls) | set(other.model_urls)),
            event_urls=list(set(self.event_urls) | set(other.event_urls)),
            artifact_urls=list(set(self.artifact_urls) | set(other.artifact_urls)),
        )


@attr.s(auto_attribs=True)
class CleanupResult:
    """
    Counts of objects modified in task cleanup operation
    """

    updated_children: int
    updated_models: int
    deleted_models: int
    urls: TaskUrls = None

    def __add__(self, other: "CleanupResult"):
        if not other:
            return self

        return CleanupResult(
            updated_children=self.updated_children + other.updated_children,
            updated_models=self.updated_models + other.updated_models,
            deleted_models=self.deleted_models + other.deleted_models,
            urls=self.urls + other.urls if self.urls else other.urls,
        )


def collect_plot_image_urls(company: str, task: str) -> Set[str]:
    urls = set()
    next_scroll_id = None
    with TimingContext("es", "collect_plot_image_urls"):
        while True:
            events, next_scroll_id = event_bll.get_plot_image_urls(
                company_id=company, task_id=task, scroll_id=next_scroll_id
            )
            if not events:
                break
            for event in events:
                event_urls = event.get(PlotFields.source_urls)
                if event_urls:
                    urls.update(set(event_urls))

    return urls


def collect_debug_image_urls(company: str, task: str) -> Set[str]:
    """
    Return the set of unique image urls
    Uses DebugImagesIterator to make sure that we do not retrieve recycled urls
    """
    after_key = None
    urls = set()
    while True:
        res, after_key = event_bll.get_debug_image_urls(
            company_id=company,
            task_id=task,
            after_key=after_key,
        )
        urls.update(res)
        if not after_key:
            break

    return urls


supported_storage_types = {
    "https://files": StorageType.fileserver,
}


def _schedule_for_delete(
    company: str, user: str, task_id: str, urls: Set[str], can_delete_folders: bool,
) -> Set[str]:
    urls_per_storage = bucketize(
        urls,
        key=lambda u: first(
            type_
            for prefix, type_ in supported_storage_types.items()
            if u.startswith(prefix)
        ),
    )
    urls_per_storage.pop(None, None)

    processed_urls = set()
    for storage_type, storage_urls in urls_per_storage.items():
        delete_folders = (storage_type == StorageType.fileserver) and can_delete_folders
        scheduled_to_delete = set()
        for url in storage_urls:
            folder = None
            if delete_folders:
                folder, _, _ = url.rpartition("/")

            to_delete = folder or url
            if to_delete in scheduled_to_delete:
                processed_urls.add(url)
                continue

            try:
                UrlToDelete(
                    id=db_id(),
                    company=company,
                    user=user,
                    url=to_delete,
                    task=task_id,
                    created=datetime.utcnow(),
                    storage_type=storage_type,
                    type=FileType.folder if folder else FileType.file,
                ).save()
            except (DuplicateKeyError, NotUniqueError):
                existing = UrlToDelete.objects(company=company, url=to_delete).first()
                if existing:
                    existing.update(
                        user=user,
                        task=task_id,
                        created=datetime.utcnow(),
                        retry_count=0,
                        unset__last_failure_time=1,
                        unset__last_failure_reason=1,
                        status=DeletionStatus.created,
                    )
            processed_urls.add(url)
            scheduled_to_delete.add(to_delete)

    return processed_urls


def cleanup_task(
    company: str,
    user: str,
    task: Task,
    force: bool = False,
    update_children=True,
    return_file_urls=False,
    delete_output_models=True,
    delete_external_artifacts=True,
) -> CleanupResult:
    """
    Validate task deletion and delete/modify all its output.
    :param task: task object
    :param force: whether to delete task with published outputs
    :return: count of delete and modified items
    """
    published_models, draft_models, in_use_model_ids = verify_task_children_and_ouptuts(
        task, force
    )

    event_urls, artifact_urls, model_urls = set(), set(), set()
    if return_file_urls:
        event_urls = collect_debug_image_urls(task.company, task.id)
        event_urls.update(collect_plot_image_urls(task.company, task.id))
        if task.execution and task.execution.artifacts:
            artifact_urls = {
                a.uri
                for a in task.execution.artifacts.values()
                if a.mode == ArtifactModes.output and a.uri
            }
        model_urls = {
            m.uri for m in draft_models if m.uri and m.id not in in_use_model_ids
        }

    deleted_task_id = f"{deleted_prefix}{task.id}"
    updated_children = 0
    if update_children:
        with TimingContext("mongo", "update_task_children"):
            updated_children = Task.objects(parent=task.id).update(
                parent=deleted_task_id
            )

    deleted_models = 0
    updated_models = 0
    for models, allow_delete in ((draft_models, True), (published_models, False)):
        if not models:
            continue
        if delete_output_models and allow_delete:
            deleted_models += Model.objects(
                id__in=[m.id for m in models if m.id not in in_use_model_ids]
            ).delete()
            if in_use_model_ids:
                Model.objects(id__in=list(in_use_model_ids)).update(unset__task=1)
            continue

        if update_children:
            updated_models += Model.objects(id__in=[m.id for m in models]).update(
                task=deleted_task_id
            )
        else:
            Model.objects(id__in=[m.id for m in models]).update(unset__task=1)

    event_bll.delete_task_events(task.company, task.id, allow_locked=force)

    if delete_external_artifacts and config.get(
        "services.async_urls_delete.enabled", False
    ):
        scheduled = _schedule_for_delete(
            task_id=task.id,
            company=company,
            user=user,
            urls=event_urls | model_urls | artifact_urls,
            can_delete_folders=not in_use_model_ids and not published_models,
        )
        for urls in (event_urls, model_urls, artifact_urls):
            urls.difference_update(scheduled)

    return CleanupResult(
        deleted_models=deleted_models,
        updated_children=updated_children,
        updated_models=updated_models,
        urls=TaskUrls(
            event_urls=list(event_urls),
            artifact_urls=list(artifact_urls),
            model_urls=list(model_urls),
        )
        if return_file_urls
        else None,
    )


def verify_task_children_and_ouptuts(
    task, force: bool
) -> Tuple[Sequence[Model], Sequence[Model], Set[str]]:
    if not force:
        with TimingContext("mongo", "count_published_children"):
            published_children_count = Task.objects(
                parent=task.id, status=TaskStatus.published
            ).count()
            if published_children_count:
                raise errors.bad_request.TaskCannotBeDeleted(
                    "has children, use force=True",
                    task=task.id,
                    children=published_children_count,
                )

    model_fields = ["id", "ready", "uri"]
    published_models, draft_models = partition(
        Model.objects(task=task.id).only(*model_fields), key=attrgetter("ready"),
    )
    if not force and published_models:
        raise errors.bad_request.TaskCannotBeDeleted(
            "has output models, use force=True",
            task=task.id,
            models=len(published_models),
        )

    if task.models and task.models.output:
        model_ids = [m.model for m in task.models.output]
        for output_model in Model.objects(id__in=model_ids).only(*model_fields):
            if output_model.ready:
                if not force:
                    raise errors.bad_request.TaskCannotBeDeleted(
                        "has output model, use force=True",
                        task=task.id,
                        model=output_model.id,
                    )
                published_models.append(output_model)
            else:
                draft_models.append(output_model)

    in_use_model_ids = {}
    if draft_models:
        model_ids = {m.id for m in draft_models}
        dependent_tasks = Task.objects(models__input__model__in=list(model_ids)).only(
            "id", "models"
        )
        in_use_model_ids = model_ids & {
            m.model
            for m in chain.from_iterable(
                t.models.input for t in dependent_tasks if t.models
            )
        }

    return published_models, draft_models, in_use_model_ids
