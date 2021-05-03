import itertools
from collections import defaultdict
from operator import attrgetter
from typing import Sequence, Generic, Callable, Type, Iterable, TypeVar, List, Set

import attr
from boltons.iterutils import partition
from mongoengine import QuerySet, Document

from apiserver.apierrors import errors
from apiserver.bll.event import EventBLL
from apiserver.bll.event.event_bll import PlotFields
from apiserver.bll.event.event_common import EventType
from apiserver.bll.task.utils import task_deleted_prefix
from apiserver.database.model.model import Model
from apiserver.database.model.task.task import Task, TaskStatus, ArtifactModes
from apiserver.timing_context import TimingContext

event_bll = EventBLL()
T = TypeVar("T", bound=Document)


class DocumentGroup(List[T]):
    """
    Operate on a list of documents as if they were a query result
    """

    def __init__(self, document_type: Type[T], documents: Iterable[T]):
        super(DocumentGroup, self).__init__(documents)
        self.type = document_type

    @property
    def ids(self) -> Set[str]:
        return {obj.id for obj in self}

    def objects(self, *args, **kwargs) -> QuerySet:
        return self.type.objects(id__in=self.ids, *args, **kwargs)


class TaskOutputs(Generic[T]):
    """
    Split task outputs of the same type by the ready state
    """

    published: DocumentGroup[T]
    draft: DocumentGroup[T]

    def __init__(
        self,
        is_published: Callable[[T], bool],
        document_type: Type[T],
        children: Iterable[T],
    ):
        """
        :param is_published: predicate returning whether items is considered published
        :param document_type: type of output
        :param children: output documents
        """
        self.published, self.draft = map(
            lambda x: DocumentGroup(document_type, x),
            partition(children, key=is_published),
        )


@attr.s(auto_attribs=True)
class TaskUrls:
    model_urls: Sequence[str]
    event_urls: Sequence[str]
    artifact_urls: Sequence[str]


@attr.s(auto_attribs=True)
class CleanupResult:
    """
    Counts of objects modified in task cleanup operation
    """

    updated_children: int
    updated_models: int
    deleted_models: int
    urls: TaskUrls = None


def _collect_plot_image_urls(company: str, task: str) -> Set[str]:
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


def _collect_debug_image_urls(company: str, task: str) -> Set[str]:
    """
    Return the set of unique image urls
    Uses DebugImagesIterator to make sure that we do not retrieve recycled urls
    """
    metrics = event_bll.get_metrics_and_variants(
        company_id=company, task_id=task, event_type=EventType.metrics_image
    )
    if not metrics:
        return set()

    task_metrics = [(task, metric) for metric in metrics]
    scroll_id = None
    urls = defaultdict(set)
    while True:
        res = event_bll.debug_images_iterator.get_task_events(
            company_id=company, metrics=task_metrics, iter_count=100, state_id=scroll_id
        )
        if not res.metric_events or not any(
            events for _, _, events in res.metric_events
        ):
            break

        scroll_id = res.next_scroll_id
        for _, metric, iterations in res.metric_events:
            metric_urls = set(ev.get("url") for it in iterations for ev in it["events"])
            metric_urls.discard(None)
            urls[metric].update(metric_urls)

    return set(itertools.chain.from_iterable(urls.values()))


def cleanup_task(
    task: Task, force: bool = False, update_children=True, return_file_urls=False
) -> CleanupResult:
    """
    Validate task deletion and delete/modify all its output.
    :param task: task object
    :param force: whether to delete task with published outputs
    :return: count of delete and modified items
    """
    models = verify_task_children_and_ouptuts(task, force)

    event_urls, artifact_urls, model_urls = set(), set(), set()
    if return_file_urls:
        event_urls = _collect_debug_image_urls(task.company, task.id)
        event_urls.update(_collect_plot_image_urls(task.company, task.id))
        if task.execution and task.execution.artifacts:
            artifact_urls = {
                a.uri
                for a in task.execution.artifacts.values()
                if a.mode == ArtifactModes.output and a.uri
            }
        model_urls = {m.uri for m in models.draft.objects().only("uri") if m.uri}

    deleted_task_id = f"{task_deleted_prefix}{task.id}"
    if update_children:
        with TimingContext("mongo", "update_task_children"):
            updated_children = Task.objects(parent=task.id).update(
                parent=deleted_task_id
            )
    else:
        updated_children = 0

    if models.draft:
        with TimingContext("mongo", "delete_models"):
            deleted_models = models.draft.objects().delete()
    else:
        deleted_models = 0

    if models.published and update_children:
        with TimingContext("mongo", "update_task_models"):
            updated_models = models.published.objects().update(task=deleted_task_id)
    else:
        updated_models = 0

    event_bll.delete_task_events(task.company, task.id, allow_locked=force)

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


def verify_task_children_and_ouptuts(task, force: bool) -> TaskOutputs[Model]:
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
        with TimingContext("mongo", "get_task_output_model"):
            output_model = Model.objects(id=task.output.model).first()
            if output_model:
                if output_model.ready:
                    if not force:
                        raise errors.bad_request.TaskCannotBeDeleted(
                            "has output model, use force=True",
                            task=task.id,
                            model=task.output.model,
                        )
                    models.published.append(output_model)
                else:
                    models.draft.append(output_model)

    if models.draft:
        with TimingContext("mongo", "get_execution_models"):
            model_ids = models.draft.ids
            dependent_tasks = Task.objects(execution__model__in=model_ids).only(
                "id", "execution.model"
            )
            input_models = [t.execution.model for t in dependent_tasks]
            if input_models:
                models.draft = DocumentGroup(
                    Model, (m for m in models.draft if m.id not in input_models)
                )

    return models
