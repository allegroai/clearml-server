import re

from apiserver.apimodels.pipelines import StartPipelineResponse, StartPipelineRequest
from apiserver.bll.organization import OrgBLL
from apiserver.bll.project import ProjectBLL
from apiserver.bll.task import TaskBLL
from apiserver.bll.task.task_operations import enqueue_task
from apiserver.database.model.project import Project
from apiserver.database.model.task.task import Task
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
