from datetime import datetime
from typing import Tuple, Set

import attr

from apiserver.apierrors import errors
from apiserver.bll.event import EventBLL
from apiserver.bll.task.task_cleanup import (
    collect_debug_image_urls,
    collect_plot_image_urls,
    TaskUrls,
)
from apiserver.config_repo import config
from apiserver.database.model import EntityVisibility
from apiserver.database.model.model import Model
from apiserver.database.model.project import Project
from apiserver.database.model.task.task import Task, ArtifactModes
from apiserver.timing_context import TimingContext

log = config.logger(__file__)
event_bll = EventBLL()


@attr.s(auto_attribs=True)
class DeleteProjectResult:
    deleted: int = 0
    disassociated_tasks: int = 0
    deleted_models: int = 0
    deleted_tasks: int = 0
    urls: TaskUrls = None


def delete_project(
    company: str, project_id: str, force: bool, delete_contents: bool
) -> DeleteProjectResult:
    project = Project.get_for_writing(company=company, id=project_id)
    if not project:
        raise errors.bad_request.InvalidProjectId(id=project_id)

    if not force:
        for cls, error in (
            (Task, errors.bad_request.ProjectHasTasks),
            (Model, errors.bad_request.ProjectHasModels),
        ):
            non_archived = cls.objects(
                project=project_id, system_tags__nin=[EntityVisibility.archived.value],
            ).only("id")
            if non_archived:
                raise error("use force=true to delete", id=project_id)

    if not delete_contents:
        with TimingContext("mongo", "update_children"):
            for cls in (Model, Task):
                updated_count = cls.objects(project=project_id).update(project=None)
        res = DeleteProjectResult(disassociated_tasks=updated_count)
    else:
        deleted_models, model_urls = _delete_models(project=project_id)
        deleted_tasks, event_urls, artifact_urls = _delete_tasks(
            company=company, project=project_id
        )
        res = DeleteProjectResult(
            deleted_tasks=deleted_tasks,
            deleted_models=deleted_models,
            urls=TaskUrls(
                model_urls=list(model_urls),
                event_urls=list(event_urls),
                artifact_urls=list(artifact_urls),
            ),
        )

    res.deleted = Project.objects(id=project_id).delete()
    return res


def _delete_tasks(company: str, project: str) -> Tuple[int, Set, Set]:
    """
    Delete only the task themselves and their non published version.
    Child models under the same project are deleted separately.
    Children tasks should be deleted in the same api call.
    If any child entities are left in another projects then updated their parent task to None
    """
    tasks = Task.objects(project=project).only("id", "execution__artifacts")
    if not tasks:
        return 0, set(), set()

    task_ids = {t.id for t in tasks}
    with TimingContext("mongo", "delete_tasks_update_children"):
        Task.objects(parent__in=task_ids, project__ne=project).update(parent=None)
        Model.objects(task__in=task_ids, project__ne=project).update(task=None)

    event_urls, artifact_urls = set(), set()
    for task in tasks:
        event_urls.update(collect_debug_image_urls(company, task.id))
        event_urls.update(collect_plot_image_urls(company, task.id))
        if task.execution and task.execution.artifacts:
            artifact_urls.update(
                {
                    a.uri
                    for a in task.execution.artifacts.values()
                    if a.mode == ArtifactModes.output and a.uri
                }
            )

    event_bll.delete_multi_task_events(company, list(task_ids))
    deleted = tasks.delete()
    return deleted, event_urls, artifact_urls


def _delete_models(project: str) -> Tuple[int, Set[str]]:
    """
    Delete project models and update the tasks from other projects
    that reference them to reference None.
    """
    with TimingContext("mongo", "delete_models"):
        models = Model.objects(project=project).only("task", "id", "uri")
        if not models:
            return 0, set()

        model_ids = {m.id for m in models}
        Task.objects(execution__model__in=model_ids, project__ne=project).update(
            execution__model=None
        )

        model_tasks = {m.task for m in models if m.task}
        if model_tasks:
            now = datetime.utcnow()
            Task.objects(
                id__in=model_tasks, project__ne=project, output__model__in=model_ids
            ).update(
                output__model=None,
                output__error=f"model deleted on {now.isoformat()}",
                last_change=now,
            )

        urls = {m.uri for m in models if m.uri}
        deleted = models.delete()
        return deleted, urls
