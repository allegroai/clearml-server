import re
from functools import partial

import attr
from apiserver.apierrors import errors
from apiserver.apierrors.errors.bad_request import CannotRemoveAllRuns
from apiserver.apimodels.pipelines import (
    StartPipelineRequest,
    DeleteRunsRequest,
)
from apiserver.bll.organization import OrgBLL
from apiserver.bll.project import ProjectBLL
from apiserver.bll.queue import QueueBLL
from apiserver.bll.task import TaskBLL
from apiserver.bll.Pipeline import PipelineBLL
from apiserver.bll.task.task_operations import enqueue_task, delete_task
from apiserver.bll.util import run_batch_operation
from apiserver.database.model.project import Project
from apiserver.database.model.pipeline import Pipeline, PipelineStep
from apiserver.database.model.task.task import Task, TaskType
from apiserver.service_repo import APICall, endpoint
from apiserver.utilities.dicts import nested_get
from apiserver.apimodels.base import IdResponse
from apiserver.services.utils import (
    conform_tag_fields,
    conform_output_tags,
    get_tags_filter_dictionary,
    sort_tags_response,
)
from apiserver.database.utils import (
    parse_from_call,
    get_company_or_none_constraint,
)
from mongoengine import Q
from apiserver.database.errors import translate_errors_context


org_bll = OrgBLL()
project_bll = ProjectBLL()
task_bll = TaskBLL()
queue_bll = QueueBLL()


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
            identity=call.identity,
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


@endpoint("pipelines.start_pipeline")
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
        identity=call.identity,
        queue_id=request.queue,
        status_message="Starting pipeline",
        status_reason="",
    )
    extra = {}
    if request.verify_watched_queue and queued:
        res_queue = nested_get(res, ("fields", "execution.queue"))
        if res_queue:
            extra["queue_watched"] = queue_bll.check_for_workers(company_id, res_queue)

    call.result.data = dict(
        pipeline=task.id,
        enqueued=bool(queued),
        **extra,
    )

create_fields = {
    "name": None,
    "description": None,
    "tags": list,
    "system_tags": list,
    "default_output_destination": None,
    "parameters" : list,
    "project": None,
    "flow_display" : dict
}
@endpoint(
    "pipelines.create_pipeline", required_fields=["name"], response_data_model=IdResponse,
)
def create(call: APICall):
    identity = call.identity
    with translate_errors_context():
        fields = parse_from_call(call.data, create_fields, Pipeline.get_fields())
        conform_tag_fields(call, fields, validate=True)
        results =PipelineBLL.create(
                user=identity.user, company=identity.company, **fields,
            )
        call.result.data ={'id':results[0],"project_id":results[1]}
    

@endpoint("pipelines.get_by_id", required_fields=["pipeline"])
def get_by_id(call):
    assert isinstance(call, APICall)
    project_id = call.data["pipeline"]

    with translate_errors_context():
        query = Q(project=project_id) & get_company_or_none_constraint(call.identity.company)
        pipeline = Pipeline.objects(query).first()
        if not pipeline:
            raise errors.bad_request.InvalidPipelineId(id=project_id)

        pipeline_dict = pipeline.to_proper_dict()
        conform_output_tags(call, pipeline_dict)

        call.result.data = {"pipeline": pipeline_dict}

@endpoint("pipelines.get_all", required_fields=["project"])
def get_by_id(call):
    assert isinstance(call, APICall)
    project_id = call.data["project"]

    with translate_errors_context():
        query = Q(project=project_id) & get_company_or_none_constraint(call.identity.company)
        pipelines = Pipeline.objects(query).all()
        pipelines_data=[]
        for pipeline in pipelines:
            pipelines_data.append(pipeline.to_proper_dict())

        call.result.data = {"pipelines": pipelines_data}


create_step_fields = {
    "name": None,
    "description": None,
    "tags": list,
    "system_tags": list,
    "default_output_destination": None,
    "parameters" : list,
    "pipeline_id": None,
    "experiment" : None

}
@endpoint(
    "pipelines.create_step", required_fields=["name","experiment"], response_data_model=IdResponse,
)
def create_step(call: APICall):
 
    identity = call.identity
    with translate_errors_context():
        fields = parse_from_call(call.data, create_step_fields, PipelineStep.get_fields())
        conform_tag_fields(call, fields, validate=True)
        return IdResponse(
            id=PipelineBLL.create_step(
                user=identity.user, company=identity.company, **fields,
            )
            )

@endpoint("pipelines.step_get_by_id", required_fields=["step"])
def get_by_id(call):
    assert isinstance(call, APICall)
    step_id = call.data["step"]

    with translate_errors_context():
        query = Q(id=step_id) & get_company_or_none_constraint(call.identity.company)
        step = PipelineStep.objects(query).first()
        if not step:
            raise errors.bad_request.InvalidStepId(id=step_id)

        step_dict = step.to_proper_dict()
        conform_output_tags(call, step_dict)

        call.result.data = {"step": step_dict}


@endpoint("pipelines.update_pipeline", required_fields=["pipeline_id"])
def pipeline_update(call:APICall):

    query = Q(id=call.data["pipeline_id"])
    pipeline = Pipeline.objects(query).first()
    pipeline.flow_display = call.data["flow_display"]
    pipeline.parameters = call.data["parameters"]
    pipeline.save()
    pipeline_data = pipeline.to_proper_dict()
    call.result.data = {"pipelines": pipeline_data}


@endpoint("pipelines.update_node", required_fields=["node_id"])
def node_update(call:APICall):

    query = Q(id=call.data["node_id"])
    pipeline_step = PipelineStep.objects(query).first()
    pipeline_step.parameters = call.data["parameters"]
    pipeline_step.save()
    pipeline_step_data = pipeline_step.to_proper_dict()
    call.result.data = {"stepdata": pipeline_step_data}


@endpoint("pipelines.compile", required_fields=[])
def compile_pipeline(call:APICall):

    compiled = PipelineBLL.compile(**call.data)
    if compiled:
        call.result.data = {"msg": "Pipeline compiled Successfully."}
    else:
        call.result.data = {"msg": "Pipeline compilation failed."}
    
@endpoint("pipelines.run", required_fields=['pipeline_id'])
def run_pipeline(call:APICall):

    pipeline_id = call.data['pipeline_id']
    run = PipelineBLL.run(pipeline_id)
    if run:
        call.result.data = {"msg": "Pipeline execution successfully."}
    else:
        call.result.data = {"msg": "Pipeline execution unsuccessfully."}