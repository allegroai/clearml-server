import re
from functools import partial

import attr

from apiserver.apierrors.errors.bad_request import CannotRemoveAllRuns
from apiserver.apimodels.pipelines import (
    StartPipelineResponse,
    StartPipelineRequest,
    DeleteRunsRequest,
)
from apiserver.bll.organization import OrgBLL
from apiserver.bll.project import ProjectBLL
from apiserver.bll.task import TaskBLL
from apiserver.bll.task.task_operations import enqueue_task, delete_task
from apiserver.bll.util import run_batch_operation
from apiserver.database.model.project import Project
from apiserver.database.model.task.task import Task, TaskType
from apiserver.service_repo import APICall, endpoint

org_bll = OrgBLL()
project_bll = ProjectBLL()
task_bll = TaskBLL()


def _update_task_name(task: Task):
    if not task or not task.project:
        return

    project = Project.objects(id=task.project).only("name").first()
    if not project:
        return

    _, _, name_prefix = project.name.rpartition("/")
    name_mask = re.compile(rf"{re.escape(name_prefix)}( #\d+)?$")
    count = Task.objects(
        project=task.project, system_tags__in=["pipeline"], name=name_mask
    ).count()
    new_name = f"{name_prefix} #{count}" if count > 0 else name_prefix
    task.update(name=new_name)


@endpoint("pipelines.delete_runs")
def delete_runs(call: APICall, company_id: str, request: DeleteRunsRequest):
    existing_runs = set(
        Task.objects(project=request.project, type=TaskType.controller).scalar("id")
    )
    if not existing_runs.difference(request.ids):
        raise CannotRemoveAllRuns(project=request.project)

    # make sure that only controller tasks are deleted
    ids = existing_runs.intersection(request.ids)
    if not ids:
        return dict(succeeded=[], failed=[])

    results, failures = run_batch_operation(
        func=partial(
            delete_task,
            company_id=company_id,
            user_id=call.identity.user,
            move_to_trash=False,
            force=True,
            return_file_urls=False,
            delete_output_models=True,
            status_message="",
            status_reason="Pipeline run deleted",
            delete_external_artifacts=True,
        ),
        ids=list(ids),
    )

    succeeded = []
    if results:
        for _id, (deleted, task, cleanup_res) in results:
            succeeded.append(
                dict(id=_id, deleted=bool(deleted), **attr.asdict(cleanup_res))
            )

    call.result.data = dict(succeeded=succeeded, failed=failures)


@endpoint(
    "pipelines.start_pipeline", response_data_model=StartPipelineResponse,
)
def start_pipeline(call: APICall, company_id: str, request: StartPipelineRequest):
    hyperparams = None
    if request.args:
        hyperparams = {
            "Args": {
                str(arg.name): {
                    "section": "Args",
                    "name": str(arg.name),
                    "value": str(arg.value),
                }
                for arg in request.args or []
            }
        }

    task, _ = task_bll.clone_task(
        company_id=company_id,
        user_id=call.identity.user,
        task_id=request.task,
        hyperparams=hyperparams,
    )

    _update_task_name(task)

    queued, res = enqueue_task(
        task_id=task.id,
        company_id=company_id,
        user_id=call.identity.user,
        queue_id=request.queue,
        status_message="Starting pipeline",
        status_reason="",
    )

    return StartPipelineResponse(pipeline=task.id, enqueued=bool(queued))
