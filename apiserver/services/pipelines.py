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
from apiserver.database.model.pipeline import  PipelineNode, Projectextendpipeline
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
    "flow_display" : dict,
    "pipeline_setting": dict
}
@endpoint(
    "pipelines.create_pipeline", required_fields=["name"], response_data_model=IdResponse,
)
def create(call: APICall):
    identity = call.identity
    with translate_errors_context():
        fields = parse_from_call(call.data, create_fields, Projectextendpipeline.get_fields())
        conform_tag_fields(call, fields, validate=True)
        pipeline_id =PipelineBLL.create(
                user=identity.user, company=identity.company,project=call.data.get("project"), **fields,
            )
        call.result.data ={'id':pipeline_id}
    

@endpoint("pipelines.get_by_id", required_fields=["pipeline"])
def get_by_id(call):
    assert isinstance(call, APICall)
    project_id = call.data["pipeline"]

    with translate_errors_context():
        query = Q(id=project_id) & get_company_or_none_constraint(call.identity.company)
        pipeline = Projectextendpipeline.objects(query).exclude("nodes").first()
        if not pipeline:
            raise errors.bad_request.InvalidPipelineId(id=project_id)
        # print(dir(pipeline))
        pipeline_dict = pipeline.to_proper_dict()
        conform_output_tags(call, pipeline_dict)
        call.result.data = {"pipeline": pipeline_dict}



create_step_fields = {
    "name": None,
    "description": None,
    "tags": list,
    "system_tags": list,
    "default_output_destination": None,
    "parameters" : list,
    "pipeline": None,
    "experiment" : None,
    "experiment_details":{},
    "code":""

}
@endpoint(
    "pipelines.create_node", required_fields=["name","experiment","pipeline_id"], response_data_model=IdResponse,
)
def create_step(call: APICall):
    
    PipelineBLL.verify_node_name(call.data.get("name"),call.data.get("pipeline"))
    return IdResponse(
            id=PipelineBLL.create_step(
                **call.data
            )
            )

@endpoint("pipelines.node_get_by_id", required_fields=["pipeline","node"])
def get_by_id(call):
    assert isinstance(call, APICall)
    pipeline = call.data["pipeline"]
    node_id = call.data["node"]
    pipeline = Projectextendpipeline.objects.get(id=pipeline).nodes.filter(id=node_id)
    if not pipeline:
        raise errors.bad_request.InvalidNodeId(id=node_id)
    node_dict = pipeline[0].to_mongo()
    node_dict['id']= node_dict['_id']
    call.result.data = {"node": node_dict}


@endpoint("pipelines.update_pipeline", required_fields=["pipeline"])
def pipeline_update(call:APICall):

    query = Q(id=call.data["pipeline"])
    pipeline = Projectextendpipeline.objects(query).first()
    if not pipeline:
        raise errors.bad_request.InvalidPipelineId(id=call.data["pipeline"])
    pipeline.flow_display = call.data["flow_display"]
    pipeline.parameters = call.data["parameters"]
    pipeline.pipeline_setting = call.data['pipeline_setting']
    pipeline.save()
    pipeline_data = pipeline.to_proper_dict()
    call.result.data = {"pipelines": pipeline_data}


@endpoint("pipelines.update_node", required_fields=["pipeline","node"])
def node_update(call:APICall):

    pipeline_node_data=PipelineBLL.update_node(**call.data)
    call.result.data = {"stepdata": pipeline_node_data}


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

@endpoint("pipelines.delete_node", required_fields=['pipeline','node'])
def delete_step(call:APICall):

    PipelineBLL.delete_step(**call.data)
    call.result.data =  {'msg': 'Successfully deleted the step.'}