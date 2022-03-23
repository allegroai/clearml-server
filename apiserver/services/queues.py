from apiserver.apimodels.base import UpdateResponse
from apiserver.apimodels.queues import (
    GetDefaultResp,
    CreateRequest,
    DeleteRequest,
    UpdateRequest,
    MoveTaskRequest,
    MoveTaskResponse,
    TaskRequest,
    QueueRequest,
    GetMetricsRequest,
    GetMetricsResponse,
    QueueMetrics,
    AddOrUpdateMetadataRequest,
    DeleteMetadataRequest,
    GetNextTaskRequest,
)
from apiserver.bll.model import Metadata
from apiserver.bll.queue import QueueBLL
from apiserver.bll.workers import WorkerBLL
from apiserver.database.model.task.task import Task
from apiserver.service_repo import APICall, endpoint
from apiserver.services.utils import (
    conform_tag_fields,
    conform_output_tags,
    conform_tags,
    escape_metadata,
    unescape_metadata,
)
from apiserver.utilities import extract_properties_to_lists

worker_bll = WorkerBLL()
queue_bll = QueueBLL(worker_bll)


@endpoint("queues.get_by_id", min_version="2.4", request_data_model=QueueRequest)
def get_by_id(call: APICall, company_id, req_model: QueueRequest):
    queue = queue_bll.get_by_id(company_id, req_model.queue)
    queue_dict = queue.to_proper_dict()
    conform_output_tags(call, queue_dict)
    unescape_metadata(call, queue_dict)
    call.result.data = {"queue": queue_dict}


@endpoint("queues.get_default", min_version="2.4", response_data_model=GetDefaultResp)
def get_by_id(call: APICall):
    queue = queue_bll.get_default(call.identity.company)
    call.result.data_model = GetDefaultResp(id=queue.id, name=queue.name)


@endpoint("queues.get_all_ex", min_version="2.4")
def get_all_ex(call: APICall):
    conform_tag_fields(call, call.data)
    ret_params = {}

    Metadata.escape_query_parameters(call)
    queues = queue_bll.get_queue_infos(
        company_id=call.identity.company, query_dict=call.data, ret_params=ret_params,
    )
    conform_output_tags(call, queues)
    unescape_metadata(call, queues)
    call.result.data = {"queues": queues, **ret_params}


@endpoint("queues.get_all", min_version="2.4")
def get_all(call: APICall):
    conform_tag_fields(call, call.data)
    ret_params = {}
    Metadata.escape_query_parameters(call)
    queues = queue_bll.get_all(
        company_id=call.identity.company, query_dict=call.data, ret_params=ret_params,
    )
    conform_output_tags(call, queues)
    unescape_metadata(call, queues)
    call.result.data = {"queues": queues, **ret_params}


@endpoint("queues.create", min_version="2.4", request_data_model=CreateRequest)
def create(call: APICall, company_id, request: CreateRequest):
    tags, system_tags = conform_tags(
        call, request.tags, request.system_tags, validate=True
    )
    queue = queue_bll.create(
        company_id=company_id,
        name=request.name,
        tags=tags,
        system_tags=system_tags,
        metadata=Metadata.metadata_from_api(request.metadata),
    )
    call.result.data = {"id": queue.id}


@endpoint(
    "queues.update",
    min_version="2.4",
    request_data_model=UpdateRequest,
    response_data_model=UpdateResponse,
)
def update(call: APICall, company_id, req_model: UpdateRequest):
    data = call.data_model_for_partial_update
    conform_tag_fields(call, data, validate=True)
    escape_metadata(data)
    updated, fields = queue_bll.update(
        company_id=company_id, queue_id=req_model.queue, **data
    )
    conform_output_tags(call, fields)
    unescape_metadata(call, fields)
    call.result.data_model = UpdateResponse(updated=updated, fields=fields)


@endpoint("queues.delete", min_version="2.4", request_data_model=DeleteRequest)
def delete(call: APICall, company_id, req_model: DeleteRequest):
    queue_bll.delete(
        company_id=company_id, queue_id=req_model.queue, force=req_model.force
    )
    call.result.data = {"deleted": 1}


@endpoint("queues.add_task", min_version="2.4", request_data_model=TaskRequest)
def add_task(call: APICall, company_id, req_model: TaskRequest):
    call.result.data = {
        "added": queue_bll.add_task(
            company_id=company_id, queue_id=req_model.queue, task_id=req_model.task
        )
    }


@endpoint("queues.get_next_task", request_data_model=GetNextTaskRequest)
def get_next_task(call: APICall, company_id, req_model: GetNextTaskRequest):
    entry = queue_bll.get_next_task(
        company_id=company_id, queue_id=req_model.queue
    )
    if entry:
        data = {"entry": entry.to_proper_dict()}
        if req_model.get_task_info:
            task = Task.objects(id=entry.task).first()
            if task:
                data["task_info"] = {"company": task.company, "user": task.user}

        call.result.data = data


@endpoint("queues.remove_task", min_version="2.4", request_data_model=TaskRequest)
def remove_task(call: APICall, company_id, req_model: TaskRequest):
    call.result.data = {
        "removed": queue_bll.remove_task(
            company_id=company_id, queue_id=req_model.queue, task_id=req_model.task
        )
    }


@endpoint(
    "queues.move_task_forward",
    min_version="2.4",
    request_data_model=MoveTaskRequest,
    response_data_model=MoveTaskResponse,
)
def move_task_forward(call: APICall, company_id, req_model: MoveTaskRequest):
    call.result.data_model = MoveTaskResponse(
        position=queue_bll.reposition_task(
            company_id=company_id,
            queue_id=req_model.queue,
            task_id=req_model.task,
            pos_func=lambda p: max(0, p - req_model.count),
        )
    )


@endpoint(
    "queues.move_task_backward",
    min_version="2.4",
    request_data_model=MoveTaskRequest,
    response_data_model=MoveTaskResponse,
)
def move_task_backward(call: APICall, company_id, req_model: MoveTaskRequest):
    call.result.data_model = MoveTaskResponse(
        position=queue_bll.reposition_task(
            company_id=company_id,
            queue_id=req_model.queue,
            task_id=req_model.task,
            pos_func=lambda p: max(0, p + req_model.count),
        )
    )


@endpoint(
    "queues.move_task_to_front",
    min_version="2.4",
    request_data_model=TaskRequest,
    response_data_model=MoveTaskResponse,
)
def move_task_to_front(call: APICall, company_id, req_model: TaskRequest):
    call.result.data_model = MoveTaskResponse(
        position=queue_bll.reposition_task(
            company_id=company_id,
            queue_id=req_model.queue,
            task_id=req_model.task,
            pos_func=lambda p: 0,
        )
    )


@endpoint(
    "queues.move_task_to_back",
    min_version="2.4",
    request_data_model=TaskRequest,
    response_data_model=MoveTaskResponse,
)
def move_task_to_back(call: APICall, company_id, req_model: TaskRequest):
    call.result.data_model = MoveTaskResponse(
        position=queue_bll.reposition_task(
            company_id=company_id,
            queue_id=req_model.queue,
            task_id=req_model.task,
            pos_func=lambda p: -1,
        )
    )


@endpoint(
    "queues.get_queue_metrics",
    min_version="2.4",
    request_data_model=GetMetricsRequest,
    response_data_model=GetMetricsResponse,
)
def get_queue_metrics(
    call: APICall, company_id, req_model: GetMetricsRequest
) -> GetMetricsResponse:
    ret = queue_bll.metrics.get_queue_metrics(
        company_id=company_id,
        from_date=req_model.from_date,
        to_date=req_model.to_date,
        interval=req_model.interval,
        queue_ids=req_model.queue_ids,
    )

    queue_dicts = {
        queue: extract_properties_to_lists(
            ["date", "avg_waiting_time", "queue_length"], data
        )
        for queue, data in ret.items()
    }
    return GetMetricsResponse(
        queues=[
            QueueMetrics(
                queue=queue,
                dates=data["date"],
                avg_waiting_times=data["avg_waiting_time"],
                queue_lengths=data["queue_length"],
            )
            if data
            else QueueMetrics(queue=queue)
            for queue, data in queue_dicts.items()
        ]
    )


@endpoint("queues.add_or_update_metadata", min_version="2.13")
def add_or_update_metadata(
    call: APICall, company_id: str, request: AddOrUpdateMetadataRequest
):
    queue_id = request.queue
    queue = queue_bll.get_by_id(company_id=company_id, queue_id=queue_id, only=("id",))
    return {
        "updated": Metadata.edit_metadata(
            queue, items=request.metadata, replace_metadata=request.replace_metadata
        )
    }


@endpoint("queues.delete_metadata", min_version="2.13")
def delete_metadata(call: APICall, company_id: str, request: DeleteMetadataRequest):
    queue_id = request.queue
    queue = queue_bll.get_by_id(company_id=company_id, queue_id=queue_id, only=("id",))
    return {"updated": Metadata.delete_metadata(queue, keys=request.keys)}
