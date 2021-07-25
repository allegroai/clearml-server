from typing import Tuple, Set, Sequence

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
from .sub_projects import _ids_with_children

log = config.logger(__file__)
event_bll = EventBLL()


@attr.s(auto_attribs=True)
class DeleteProjectResult:
    deleted: int = 0
    disassociated_tasks: int = 0
    deleted_models: int = 0
    deleted_tasks: int = 0
    urls: TaskUrls = None


def validate_project_delete(company: str, project_id: str):
    project = Project.get_for_writing(
        company=company, id=project_id, _only=("id", "path")
    )
    if not project:
        raise errors.bad_request.InvalidProjectId(id=project_id)

    project_ids = _ids_with_children([project_id])
    ret = {}
    for cls in (Task, Model):
        ret[f"{cls.__name__.lower()}s"] = cls.objects(
            project__in=project_ids,
        ).count()
    for cls in (Task, Model):
        ret[f"non_archived_{cls.__name__.lower()}s"] = cls.objects(
            project__in=project_ids,
            system_tags__nin=[EntityVisibility.archived.value],
        ).count()

    return ret


def delete_project(
    company: str, project_id: str, force: bool, delete_contents: bool
) -> Tuple[DeleteProjectResult, Set[str]]:
    project = Project.get_for_writing(
        company=company, id=project_id, _only=("id", "path")
    )
    if not project:
        raise errors.bad_request.InvalidProjectId(id=project_id)

    project_ids = _ids_with_children([project_id])
    if not force:
        for cls, error in (
            (Task, errors.bad_request.ProjectHasTasks),
            (Model, errors.bad_request.ProjectHasModels),
        ):
            non_archived = cls.objects(
                project__in=project_ids,
                system_tags__nin=[EntityVisibility.archived.value],
            ).only("id")
            if non_archived:
                raise error("use force=true to delete", id=project_id)

    if not delete_contents:
        with TimingContext("mongo", "update_children"):
            for cls in (Model, Task):
                updated_count = cls.objects(project__in=project_ids).update(
                    project=None
                )
        res = DeleteProjectResult(disassociated_tasks=updated_count)
    else:
        deleted_models, model_urls = _delete_models(projects=project_ids)
        deleted_tasks, event_urls, artifact_urls = _delete_tasks(
            company=company, projects=project_ids
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

    affected = {*project_ids, *(project.path or [])}
    res.deleted = Project.objects(id__in=project_ids).delete()

    return res, affected


def _delete_tasks(company: str, projects: Sequence[str]) -> Tuple[int, Set, Set]:
    """
    Delete only the task themselves and their non published version.
    Child models under the same project are deleted separately.
    Children tasks should be deleted in the same api call.
    If any child entities are left in another projects then updated their parent task to None
    """
    tasks = Task.objects(project__in=projects).only("id", "execution__artifacts")
    if not tasks:
        return 0, set(), set()

    task_ids = {t.id for t in tasks}
    with TimingContext("mongo", "delete_tasks_update_children"):
        Task.objects(parent__in=task_ids, project__nin=projects).update(parent=None)
        Model.objects(task__in=task_ids, project__nin=projects).update(task=None)

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


def _delete_models(projects: Sequence[str]) -> Tuple[int, Set[str]]:
    """
    Delete project models and update the tasks from other projects
    that reference them to reference None.
    """
    with TimingContext("mongo", "delete_models"):
        models = Model.objects(project__in=projects).only("task", "id", "uri")
        if not models:
            return 0, set()

        model_ids = list({m.id for m in models})

        Task._get_collection().update_many(
            filter={
                "project": {"$nin": projects},
                "models.input.model": {"$in": model_ids},
            },
            update={"$set": {"models.input.$[elem].model": None}},
            array_filters=[{"elem.model": {"$in": model_ids}}],
            upsert=False,
        )

        model_tasks = list({m.task for m in models if m.task})
        if model_tasks:
            Task._get_collection().update_many(
                filter={
                    "_id": {"$in": model_tasks},
                    "project": {"$nin": projects},
                    "models.output.model": {"$in": model_ids},
                },
                update={"$set": {"models.output.$[elem].model": None}},
                array_filters=[{"elem.model": {"$in": model_ids}}],
                upsert=False,
            )

        urls = {m.uri for m in models if m.uri}
        deleted = models.delete()
        return deleted, urls
