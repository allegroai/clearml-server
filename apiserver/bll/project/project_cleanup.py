from collections import defaultdict
from typing import Tuple, Set, Sequence

import attr

from apiserver.apierrors import errors
from apiserver.bll.event import EventBLL
from apiserver.bll.task.task_cleanup import (
    collect_debug_image_urls,
    collect_plot_image_urls,
    TaskUrls,
    _schedule_for_delete,
)
from apiserver.config_repo import config
from apiserver.database.model import EntityVisibility
from apiserver.database.model.model import Model
from apiserver.database.model.project import Project
from apiserver.database.model.task.task import Task, ArtifactModes, TaskType
from .project_bll import ProjectBLL
from .sub_projects import _ids_with_children

log = config.logger(__file__)
event_bll = EventBLL()
async_events_delete = config.get("services.tasks.async_events_delete", False)


@attr.s(auto_attribs=True)
class DeleteProjectResult:
    deleted: int = 0
    disassociated_tasks: int = 0
    deleted_models: int = 0
    deleted_tasks: int = 0
    urls: TaskUrls = None


def validate_project_delete(company: str, project_id: str):
    project = Project.get_for_writing(
        company=company, id=project_id, _only=("id", "path", "system_tags")
    )
    if not project:
        raise errors.bad_request.InvalidProjectId(id=project_id)
    is_pipeline = "pipeline" in (project.system_tags or [])
    project_ids = _ids_with_children([project_id])
    ret = {}
    for cls in ProjectBLL.child_classes:
        ret[f"{cls.__name__.lower()}s"] = cls.objects(project__in=project_ids).count()
    for cls in ProjectBLL.child_classes:
        query = dict(
            project__in=project_ids, system_tags__nin=[EntityVisibility.archived.value]
        )
        name = f"non_archived_{cls.__name__.lower()}s"
        if not is_pipeline:
            ret[name] = cls.objects(**query).count()
        else:
            ret[name] = (
                cls.objects(**query, type=TaskType.controller).count()
                if cls == Task
                else 0
            )

    return ret


def delete_project(
    company: str,
    user: str,
    project_id: str,
    force: bool,
    delete_contents: bool,
    delete_external_artifacts=True,
) -> Tuple[DeleteProjectResult, Set[str]]:
    project = Project.get_for_writing(
        company=company, id=project_id, _only=("id", "path", "system_tags")
    )
    if not project:
        raise errors.bad_request.InvalidProjectId(id=project_id)

    delete_external_artifacts = delete_external_artifacts and config.get(
        "services.async_urls_delete.enabled", False
    )
    is_pipeline = "pipeline" in (project.system_tags or [])
    project_ids = _ids_with_children([project_id])
    if not force:
        query = dict(
            project__in=project_ids, system_tags__nin=[EntityVisibility.archived.value]
        )
        if not is_pipeline:
            for cls, error in (
                (Task, errors.bad_request.ProjectHasTasks),
                (Model, errors.bad_request.ProjectHasModels),
            ):
                non_archived = cls.objects(**query).only("id")
                if non_archived:
                    raise error("use force=true to delete", id=project_id)
        else:
            non_archived = Task.objects(**query, type=TaskType.controller).only("id")
            if non_archived:
                raise errors.bad_request.ProjectHasTasks(
                    "please archive all the runs inside the project", id=project_id
                )

    if not delete_contents:
        disassociated = defaultdict(int)
        for cls in ProjectBLL.child_classes:
            disassociated[cls] = cls.objects(project__in=project_ids).update(project=None)
        res = DeleteProjectResult(disassociated_tasks=disassociated[Task])
    else:
        deleted_models, model_event_urls, model_urls = _delete_models(
            company=company, projects=project_ids
        )
        deleted_tasks, task_event_urls, artifact_urls = _delete_tasks(
            company=company, projects=project_ids
        )
        event_urls = task_event_urls | model_event_urls
        if delete_external_artifacts:
            scheduled = _schedule_for_delete(
                task_id=project_id,
                company=company,
                user=user,
                urls=event_urls | model_urls | artifact_urls,
                can_delete_folders=True,
            )
            for urls in (event_urls, model_urls, artifact_urls):
                urls.difference_update(scheduled)
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

    event_bll.delete_multi_task_events(
        company, list(task_ids), async_delete=async_events_delete
    )
    deleted = tasks.delete()
    return deleted, event_urls, artifact_urls


def _delete_models(
    company: str, projects: Sequence[str]
) -> Tuple[int, Set[str], Set[str]]:
    """
    Delete project models and update the tasks from other projects
    that reference them to reference None.
    """
    models = Model.objects(project__in=projects).only("task", "id", "uri")
    if not models:
        return 0, set(), set()

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

    event_urls, model_urls = set(), set()
    for m in models:
        event_urls.update(collect_debug_image_urls(company, m.id))
        event_urls.update(collect_plot_image_urls(company, m.id))
        if m.uri:
            model_urls.add(m.uri)

    event_bll.delete_multi_task_events(
        company, model_ids, async_delete=async_events_delete
    )
    deleted = models.delete()
    return deleted, event_urls, model_urls
