import itertools
import math
from collections import defaultdict
from operator import itemgetter
from typing import Sequence, Optional

import attr
import jsonmodels.fields
from boltons.iterutils import bucketize

from apiserver.apierrors import errors
from apiserver.apimodels.events import (
    MultiTaskScalarMetricsIterHistogramRequest,
    ScalarMetricsIterHistogramRequest,
    DebugImagesRequest,
    DebugImageResponse,
    MetricEvents,
    IterationEvents,
    TaskMetricsRequest,
    LogEventsRequest,
    LogOrderEnum,
    GetDebugImageSampleRequest,
    NextDebugImageSampleRequest,
    MetricVariants as ApiMetrics,
    TaskPlotsRequest,
    TaskEventsRequest,
    ScalarMetricsIterRawRequest,
)
from apiserver.bll.event import EventBLL
from apiserver.bll.event.event_common import EventType, MetricVariants
from apiserver.bll.event.events_iterator import Scroll
from apiserver.bll.event.scalar_key import ScalarKeyEnum, ScalarKey
from apiserver.bll.task import TaskBLL
from apiserver.config_repo import config
from apiserver.service_repo import APICall, endpoint
from apiserver.utilities import json, extract_properties_to_lists

task_bll = TaskBLL()
event_bll = EventBLL()


@endpoint("events.add")
def add(call: APICall, company_id, _):
    data = call.data.copy()
    allow_locked = data.pop("allow_locked", False)
    added, err_count, err_info = event_bll.add_events(
        company_id, [data], call.worker, allow_locked_tasks=allow_locked
    )
    call.result.data = dict(added=added, errors=err_count, errors_info=err_info)


@endpoint("events.add_batch")
def add_batch(call: APICall, company_id, _):
    events = call.batched_data
    if events is None or len(events) == 0:
        raise errors.bad_request.BatchContainsNoItems()

    added, err_count, err_info = event_bll.add_events(company_id, events, call.worker)
    call.result.data = dict(added=added, errors=err_count, errors_info=err_info)


@endpoint("events.get_task_log", required_fields=["task"])
def get_task_log_v1_5(call, company_id, _):
    task_id = call.data["task"]
    task = task_bll.assert_exists(
        company_id, task_id, allow_public=True, only=("company", "company_origin")
    )[0]
    order = call.data.get("order") or "desc"
    scroll_id = call.data.get("scroll_id")
    batch_size = int(call.data.get("batch_size") or 500)
    events, scroll_id, total_events = event_bll.scroll_task_events(
        task.get_index_company(),
        task_id,
        order,
        event_type=EventType.task_log,
        batch_size=batch_size,
        scroll_id=scroll_id,
    )
    call.result.data = dict(
        events=events, returned=len(events), total=total_events, scroll_id=scroll_id
    )


@endpoint("events.get_task_log", min_version="1.7", required_fields=["task"])
def get_task_log_v1_7(call, company_id, _):
    task_id = call.data["task"]
    task = task_bll.assert_exists(
        company_id, task_id, allow_public=True, only=("company", "company_origin")
    )[0]

    order = call.data.get("order") or "desc"
    from_ = call.data.get("from") or "head"
    scroll_id = call.data.get("scroll_id")
    batch_size = int(call.data.get("batch_size") or 500)

    scroll_order = "asc" if (from_ == "head") else "desc"

    events, scroll_id, total_events = event_bll.scroll_task_events(
        company_id=task.get_index_company(),
        task_id=task_id,
        order=scroll_order,
        event_type=EventType.task_log,
        batch_size=batch_size,
        scroll_id=scroll_id,
    )

    if scroll_order != order:
        events = events[::-1]

    call.result.data = dict(
        events=events, returned=len(events), total=total_events, scroll_id=scroll_id
    )


@endpoint("events.get_task_log", min_version="2.9", request_data_model=LogEventsRequest)
def get_task_log(call, company_id, request: LogEventsRequest):
    task_id = request.task
    task = task_bll.assert_exists(
        company_id, task_id, allow_public=True, only=("company", "company_origin")
    )[0]

    res = event_bll.events_iterator.get_task_events(
        event_type=EventType.task_log,
        company_id=task.get_index_company(),
        task_id=task_id,
        batch_size=request.batch_size,
        navigate_earlier=request.navigate_earlier,
        from_timestamp=request.from_timestamp,
    )

    if request.order and (
        (request.navigate_earlier and request.order == LogOrderEnum.asc)
        or (not request.navigate_earlier and request.order == LogOrderEnum.desc)
    ):
        res.events.reverse()

    call.result.data = dict(
        events=res.events, returned=len(res.events), total=res.total_events
    )


@endpoint("events.download_task_log", required_fields=["task"])
def download_task_log(call, company_id, _):
    task_id = call.data["task"]
    task = task_bll.assert_exists(
        company_id, task_id, allow_public=True, only=("company", "company_origin")
    )[0]

    line_type = call.data.get("line_type", "json").lower()
    line_format = str(call.data.get("line_format", "{asctime} {worker} {level} {msg}"))

    is_json = line_type == "json"
    if not is_json:
        if not line_format:
            raise errors.bad_request.MissingRequiredFields(
                "line_format is required for plain text lines"
            )

        # validate line format placeholders
        valid_task_log_fields = {"asctime", "timestamp", "level", "worker", "msg"}

        invalid_placeholders = set()
        while True:
            try:
                line_format.format(
                    **dict.fromkeys(valid_task_log_fields | invalid_placeholders)
                )
                break
            except KeyError as e:
                invalid_placeholders.add(e.args[0])
            except Exception as e:
                raise errors.bad_request.FieldsValueError(
                    "invalid line format", error=e.args[0]
                )

        if invalid_placeholders:
            raise errors.bad_request.FieldsValueError(
                "undefined placeholders in line format",
                placeholders=invalid_placeholders,
            )

        # make sure line_format has a trailing newline
        line_format = line_format.rstrip("\n") + "\n"

    def generate():
        scroll_id = None
        batch_size = 1000
        while True:
            log_events, scroll_id, _ = event_bll.scroll_task_events(
                task.get_index_company(),
                task_id,
                order="asc",
                event_type=EventType.task_log,
                batch_size=batch_size,
                scroll_id=scroll_id,
            )
            if not log_events:
                break
            for ev in log_events:
                ev["asctime"] = ev.pop("timestamp")
                if is_json:
                    ev.pop("type")
                    ev.pop("task")
                    yield json.dumps(ev) + "\n"
                else:
                    try:
                        yield line_format.format(**ev)
                    except KeyError as ex:
                        raise errors.bad_request.FieldsValueError(
                            "undefined placeholders in line format",
                            placeholders=[str(ex)],
                        )

            if len(log_events) < batch_size:
                break

    call.result.filename = "task_%s.log" % task_id
    call.result.content_type = "text/plain"
    call.result.raw_data = generate()


@endpoint("events.get_vector_metrics_and_variants", required_fields=["task"])
def get_vector_metrics_and_variants(call, company_id, _):
    task_id = call.data["task"]
    task = task_bll.assert_exists(
        company_id, task_id, allow_public=True, only=("company", "company_origin")
    )[0]
    call.result.data = dict(
        metrics=event_bll.get_metrics_and_variants(
            task.get_index_company(), task_id, EventType.metrics_vector
        )
    )


@endpoint("events.get_scalar_metrics_and_variants", required_fields=["task"])
def get_scalar_metrics_and_variants(call, company_id, _):
    task_id = call.data["task"]
    task = task_bll.assert_exists(
        company_id, task_id, allow_public=True, only=("company", "company_origin")
    )[0]
    call.result.data = dict(
        metrics=event_bll.get_metrics_and_variants(
            task.get_index_company(), task_id, EventType.metrics_scalar
        )
    )


# todo: !!! currently returning 10,000 records. should decide on a better way to control it
@endpoint(
    "events.vector_metrics_iter_histogram",
    required_fields=["task", "metric", "variant"],
)
def vector_metrics_iter_histogram(call, company_id, _):
    task_id = call.data["task"]
    task = task_bll.assert_exists(
        company_id, task_id, allow_public=True, only=("company", "company_origin")
    )[0]
    metric = call.data["metric"]
    variant = call.data["variant"]
    iterations, vectors = event_bll.get_vector_metrics_per_iter(
        task.get_index_company(), task_id, metric, variant
    )
    call.result.data = dict(
        metric=metric, variant=variant, vectors=vectors, iterations=iterations
    )


class GetTaskEventsScroll(Scroll):
    from_key_value = jsonmodels.fields.StringField()
    total = jsonmodels.fields.IntField()
    request: TaskEventsRequest = jsonmodels.fields.EmbeddedField(TaskEventsRequest)


def make_response(
    total: int, returned: int = 0, scroll_id: str = None, **kwargs
) -> dict:
    return {
        "returned": returned,
        "total": total,
        "scroll_id": scroll_id,
        **kwargs,
    }


@endpoint("events.get_task_events", request_data_model=TaskEventsRequest)
def get_task_events(call, company_id, request: TaskEventsRequest):
    task_id = request.task

    task = task_bll.assert_exists(
        company_id, task_id, allow_public=True, only=("company",),
    )[0]

    key = ScalarKeyEnum.iter
    scalar_key = ScalarKey.resolve(key)

    if not request.scroll_id:
        from_key_value = None if (request.order == LogOrderEnum.desc) else 0
        total = None
    else:
        try:
            scroll = GetTaskEventsScroll.from_scroll_id(request.scroll_id)
        except ValueError:
            raise errors.bad_request.InvalidScrollId(scroll_id=request.scroll_id)

        if scroll.from_key_value is None:
            return make_response(
                scroll_id=request.scroll_id, total=scroll.total, events=[]
            )

        from_key_value = scalar_key.cast_value(scroll.from_key_value)
        total = scroll.total

        scroll.request.batch_size = request.batch_size or scroll.request.batch_size
        request = scroll.request

    navigate_earlier = request.order == LogOrderEnum.desc
    metric_variants = _get_metric_variants_from_request(request.metrics)

    if request.count_total and total is None:
        total = event_bll.events_iterator.count_task_events(
            event_type=request.event_type,
            company_id=task.company,
            task_id=task_id,
            metric_variants=metric_variants,
        )

    batch_size = min(
        request.batch_size,
        int(
            config.get("services.events.events_retrieval.max_raw_scalars_size", 10_000)
        ),
    )

    res = event_bll.events_iterator.get_task_events(
        event_type=request.event_type,
        company_id=task.company,
        task_id=task_id,
        batch_size=batch_size,
        key=ScalarKeyEnum.iter,
        navigate_earlier=navigate_earlier,
        from_key_value=from_key_value,
        metric_variants=metric_variants,
    )

    scroll = GetTaskEventsScroll(
        from_key_value=str(res.events[-1][scalar_key.field]) if res.events else None,
        total=total,
        request=request,
    )

    return make_response(
        returned=len(res.events),
        total=total,
        scroll_id=scroll.get_scroll_id(),
        events=res.events,
    )


@endpoint("events.get_scalar_metric_data", required_fields=["task", "metric"])
def get_scalar_metric_data(call, company_id, _):
    task_id = call.data["task"]
    metric = call.data["metric"]
    scroll_id = call.data.get("scroll_id")
    no_scroll = call.data.get("no_scroll", False)

    task = task_bll.assert_exists(
        company_id, task_id, allow_public=True, only=("company", "company_origin")
    )[0]
    result = event_bll.get_task_events(
        task.get_index_company(),
        task_id,
        event_type=EventType.metrics_scalar,
        sort=[{"iter": {"order": "desc"}}],
        metric=metric,
        scroll_id=scroll_id,
        no_scroll=no_scroll,
    )

    call.result.data = dict(
        events=result.events,
        returned=len(result.events),
        total=result.total_events,
        scroll_id=result.next_scroll_id,
    )


@endpoint("events.get_task_latest_scalar_values", required_fields=["task"])
def get_task_latest_scalar_values(call, company_id, _):
    task_id = call.data["task"]
    task = task_bll.assert_exists(
        company_id, task_id, allow_public=True, only=("company", "company_origin")
    )[0]
    index_company = task.get_index_company()
    metrics, last_timestamp = event_bll.get_task_latest_scalar_values(
        index_company, task_id
    )
    last_iters = event_bll.get_last_iters(
        company_id=company_id, event_type=EventType.all, task_id=task_id, iters=1
    ).get(task_id)
    call.result.data = dict(
        metrics=metrics,
        last_iter=last_iters[0] if last_iters else 0,
        name=task.name,
        status=task.status,
        last_timestamp=last_timestamp,
    )


# todo: should not repeat iter (x-axis) for each metric/variant, JS client should get raw data and fill gaps if needed
@endpoint(
    "events.scalar_metrics_iter_histogram",
    request_data_model=ScalarMetricsIterHistogramRequest,
)
def scalar_metrics_iter_histogram(
    call, company_id, request: ScalarMetricsIterHistogramRequest
):
    task = task_bll.assert_exists(
        company_id, request.task, allow_public=True, only=("company", "company_origin")
    )[0]
    metrics = event_bll.metrics.get_scalar_metrics_average_per_iter(
        task.get_index_company(),
        task_id=request.task,
        samples=request.samples,
        key=request.key,
    )
    call.result.data = metrics


@endpoint(
    "events.multi_task_scalar_metrics_iter_histogram",
    request_data_model=MultiTaskScalarMetricsIterHistogramRequest,
)
def multi_task_scalar_metrics_iter_histogram(
    call, company_id, req_model: MultiTaskScalarMetricsIterHistogramRequest
):
    task_ids = req_model.tasks
    if isinstance(task_ids, str):
        task_ids = [s.strip() for s in task_ids.split(",")]
    # Note, bll already validates task ids as it needs their names
    call.result.data = dict(
        metrics=event_bll.metrics.compare_scalar_metrics_average_per_iter(
            company_id,
            task_ids=task_ids,
            samples=req_model.samples,
            allow_public=True,
            key=req_model.key,
        )
    )


@endpoint("events.get_multi_task_plots", required_fields=["tasks"])
def get_multi_task_plots_v1_7(call, company_id, _):
    task_ids = call.data["tasks"]
    iters = call.data.get("iters", 1)
    scroll_id = call.data.get("scroll_id")

    tasks = task_bll.assert_exists(
        company_id=company_id,
        only=("id", "name", "company", "company_origin"),
        task_ids=task_ids,
        allow_public=True,
    )

    companies = {t.get_index_company() for t in tasks}
    if len(companies) > 1:
        raise errors.bad_request.InvalidTaskId(
            "only tasks from the same company are supported"
        )

    # Get last 10K events by iteration and group them by unique metric+variant, returning top events for combination
    result = event_bll.get_task_events(
        next(iter(companies)),
        task_ids,
        event_type=EventType.metrics_plot,
        sort=[{"iter": {"order": "desc"}}],
        size=10000,
        scroll_id=scroll_id,
    )

    tasks = {t.id: t.name for t in tasks}

    return_events = _get_top_iter_unique_events_per_task(
        result.events, max_iters=iters, tasks=tasks
    )

    call.result.data = dict(
        plots=return_events,
        returned=len(return_events),
        total=result.total_events,
        scroll_id=result.next_scroll_id,
    )


@endpoint("events.get_multi_task_plots", min_version="1.8", required_fields=["tasks"])
def get_multi_task_plots(call, company_id, req_model):
    task_ids = call.data["tasks"]
    iters = call.data.get("iters", 1)
    scroll_id = call.data.get("scroll_id")
    no_scroll = call.data.get("no_scroll", False)

    tasks = task_bll.assert_exists(
        company_id=call.identity.company,
        only=("id", "name", "company", "company_origin"),
        task_ids=task_ids,
        allow_public=True,
    )

    companies = {t.get_index_company() for t in tasks}
    if len(companies) > 1:
        raise errors.bad_request.InvalidTaskId(
            "only tasks from the same company are supported"
        )

    result = event_bll.get_task_events(
        next(iter(companies)),
        task_ids,
        event_type=EventType.metrics_plot,
        sort=[{"iter": {"order": "desc"}}],
        last_iter_count=iters,
        scroll_id=scroll_id,
        no_scroll=no_scroll,
    )

    tasks = {t.id: t.name for t in tasks}

    return_events = _get_top_iter_unique_events_per_task(
        result.events, max_iters=iters, tasks=tasks
    )

    call.result.data = dict(
        plots=return_events,
        returned=len(return_events),
        total=result.total_events,
        scroll_id=result.next_scroll_id,
    )


@endpoint("events.get_task_plots", required_fields=["task"])
def get_task_plots_v1_7(call, company_id, _):
    task_id = call.data["task"]
    iters = call.data.get("iters", 1)
    scroll_id = call.data.get("scroll_id")

    task = task_bll.assert_exists(
        company_id, task_id, allow_public=True, only=("company", "company_origin")
    )[0]
    # events, next_scroll_id, total_events = event_bll.get_task_events(
    #     company, task_id,
    #     event_type="plot",
    #     sort=[{"iter": {"order": "desc"}}],
    #     last_iter_count=iters,
    #     scroll_id=scroll_id)

    # get last 10K events by iteration and group them by unique metric+variant, returning top events for combination
    result = event_bll.get_task_events(
        task.get_index_company(),
        task_id,
        event_type=EventType.metrics_plot,
        sort=[{"iter": {"order": "desc"}}],
        size=10000,
        scroll_id=scroll_id,
    )

    return_events = _get_top_iter_unique_events(result.events, max_iters=iters)

    call.result.data = dict(
        plots=return_events,
        returned=len(return_events),
        total=result.total_events,
        scroll_id=result.next_scroll_id,
    )


def _get_metric_variants_from_request(
    req_metrics: Sequence[ApiMetrics],
) -> Optional[MetricVariants]:
    if not req_metrics:
        return None

    return {m.metric: m.variants for m in req_metrics}


@endpoint(
    "events.get_task_plots", min_version="1.8", request_data_model=TaskPlotsRequest
)
def get_task_plots(call, company_id, request: TaskPlotsRequest):
    task_id = request.task
    iters = request.iters
    scroll_id = request.scroll_id

    task = task_bll.assert_exists(
        company_id, task_id, allow_public=True, only=("company", "company_origin")
    )[0]
    result = event_bll.get_task_plots(
        task.get_index_company(),
        tasks=[task_id],
        sort=[{"iter": {"order": "desc"}}],
        last_iterations_per_plot=iters,
        scroll_id=scroll_id,
        no_scroll=request.no_scroll,
        metric_variants=_get_metric_variants_from_request(request.metrics),
    )

    return_events = result.events

    call.result.data = dict(
        plots=return_events,
        returned=len(return_events),
        total=result.total_events,
        scroll_id=result.next_scroll_id,
    )


@endpoint("events.debug_images", required_fields=["task"])
def get_debug_images_v1_7(call, company_id, _):
    task_id = call.data["task"]
    iters = call.data.get("iters") or 1
    scroll_id = call.data.get("scroll_id")

    task = task_bll.assert_exists(
        company_id, task_id, allow_public=True, only=("company", "company_origin")
    )[0]
    # events, next_scroll_id, total_events = event_bll.get_task_events(
    #     company, task_id,
    #     event_type="training_debug_image",
    #     sort=[{"iter": {"order": "desc"}}],
    #     last_iter_count=iters,
    #     scroll_id=scroll_id)

    # get last 10K events by iteration and group them by unique metric+variant, returning top events for combination
    result = event_bll.get_task_events(
        task.get_index_company(),
        task_id,
        event_type=EventType.metrics_image,
        sort=[{"iter": {"order": "desc"}}],
        size=10000,
        scroll_id=scroll_id,
    )

    return_events = _get_top_iter_unique_events(result.events, max_iters=iters)

    call.result.data = dict(
        task=task_id,
        images=return_events,
        returned=len(return_events),
        total=result.total_events,
        scroll_id=result.next_scroll_id,
    )


@endpoint("events.debug_images", min_version="1.8", required_fields=["task"])
def get_debug_images_v1_8(call, company_id, _):
    task_id = call.data["task"]
    iters = call.data.get("iters") or 1
    scroll_id = call.data.get("scroll_id")

    task = task_bll.assert_exists(
        company_id, task_id, allow_public=True, only=("company", "company_origin")
    )[0]
    result = event_bll.get_task_events(
        task.get_index_company(),
        task_id,
        event_type=EventType.metrics_image,
        sort=[{"iter": {"order": "desc"}}],
        last_iter_count=iters,
        scroll_id=scroll_id,
    )

    return_events = result.events

    call.result.data = dict(
        task=task_id,
        images=return_events,
        returned=len(return_events),
        total=result.total_events,
        scroll_id=result.next_scroll_id,
    )


@endpoint(
    "events.debug_images",
    min_version="2.7",
    request_data_model=DebugImagesRequest,
    response_data_model=DebugImageResponse,
)
def get_debug_images(call, company_id, request: DebugImagesRequest):
    task_metrics = defaultdict(dict)
    for tm in request.metrics:
        task_metrics[tm.task][tm.metric] = tm.variants
    for metrics in task_metrics.values():
        if None in metrics:
            metrics.clear()

    tasks = task_bll.assert_exists(
        company_id,
        task_ids=list(task_metrics),
        allow_public=True,
        only=("company", "company_origin"),
    )

    companies = {t.get_index_company() for t in tasks}
    if len(companies) > 1:
        raise errors.bad_request.InvalidTaskId(
            "only tasks from the same company are supported"
        )

    result = event_bll.debug_images_iterator.get_task_events(
        company_id=next(iter(companies)),
        task_metrics=task_metrics,
        iter_count=request.iters,
        navigate_earlier=request.navigate_earlier,
        refresh=request.refresh,
        state_id=request.scroll_id,
    )

    call.result.data_model = DebugImageResponse(
        scroll_id=result.next_scroll_id,
        metrics=[
            MetricEvents(
                task=task,
                iterations=[
                    IterationEvents(iter=iteration["iter"], events=iteration["events"])
                    for iteration in iterations
                ],
            )
            for (task, iterations) in result.metric_events
        ],
    )


@endpoint(
    "events.get_debug_image_sample",
    min_version="2.12",
    request_data_model=GetDebugImageSampleRequest,
)
def get_debug_image_sample(call, company_id, request: GetDebugImageSampleRequest):
    task = task_bll.assert_exists(
        company_id, task_ids=[request.task], allow_public=True, only=("company",)
    )[0]
    res = event_bll.debug_sample_history.get_debug_image_for_variant(
        company_id=task.company,
        task=request.task,
        metric=request.metric,
        variant=request.variant,
        iteration=request.iteration,
        refresh=request.refresh,
        state_id=request.scroll_id,
    )
    call.result.data = attr.asdict(res, recurse=False)


@endpoint(
    "events.next_debug_image_sample",
    min_version="2.12",
    request_data_model=NextDebugImageSampleRequest,
)
def next_debug_image_sample(call, company_id, request: NextDebugImageSampleRequest):
    task = task_bll.assert_exists(
        company_id, task_ids=[request.task], allow_public=True, only=("company",)
    )[0]
    res = event_bll.debug_sample_history.get_next_debug_image(
        company_id=task.company,
        task=request.task,
        state_id=request.scroll_id,
        navigate_earlier=request.navigate_earlier,
    )
    call.result.data = attr.asdict(res, recurse=False)


@endpoint("events.get_task_metrics", request_data_model=TaskMetricsRequest)
def get_tasks_metrics(call: APICall, company_id, request: TaskMetricsRequest):
    task = task_bll.assert_exists(
        company_id,
        task_ids=request.tasks,
        allow_public=True,
        only=("company", "company_origin"),
    )[0]
    res = event_bll.metrics.get_tasks_metrics(
        task.get_index_company(), task_ids=request.tasks, event_type=request.event_type
    )
    call.result.data = {
        "metrics": [{"task": task, "metrics": metrics} for (task, metrics) in res]
    }


@endpoint("events.delete_for_task", required_fields=["task"])
def delete_for_task(call, company_id, req_model):
    task_id = call.data["task"]
    allow_locked = call.data.get("allow_locked", False)

    task_bll.assert_exists(company_id, task_id, return_tasks=False)
    call.result.data = dict(
        deleted=event_bll.delete_task_events(
            company_id, task_id, allow_locked=allow_locked
        )
    )


def _get_top_iter_unique_events_per_task(events, max_iters, tasks):
    key = itemgetter("metric", "variant", "task", "iter")

    unique_events = itertools.chain.from_iterable(
        itertools.islice(group, max_iters)
        for _, group in itertools.groupby(
            sorted(events, key=key, reverse=True), key=key
        )
    )

    def collect(evs, fields):
        if not fields:
            evs = list(evs)
            return {"name": tasks.get(evs[0].get("task")), "plots": evs}
        return {
            str(k): collect(group, fields[1:])
            for k, group in itertools.groupby(evs, key=itemgetter(fields[0]))
        }

    collect_fields = ("metric", "variant", "task", "iter")
    return collect(
        sorted(unique_events, key=itemgetter(*collect_fields), reverse=True),
        collect_fields,
    )


def _get_top_iter_unique_events(events, max_iters):
    top_unique_events = defaultdict(lambda: [])
    for ev in events:
        key = ev.get("metric", "") + ev.get("variant", "")
        evs = top_unique_events[key]
        if len(evs) < max_iters:
            evs.append(ev)
    unique_events = list(
        itertools.chain.from_iterable(list(top_unique_events.values()))
    )
    unique_events.sort(key=lambda e: e["iter"], reverse=True)
    return unique_events


class ScalarMetricsIterRawScroll(Scroll):
    from_key_value = jsonmodels.fields.StringField()
    total = jsonmodels.fields.IntField()
    request: ScalarMetricsIterRawRequest = jsonmodels.fields.EmbeddedField(
        ScalarMetricsIterRawRequest
    )


@endpoint("events.scalar_metrics_iter_raw", min_version="2.16")
def scalar_metrics_iter_raw(
    call: APICall, company_id: str, request: ScalarMetricsIterRawRequest
):
    key = request.key or ScalarKeyEnum.iter
    scalar_key = ScalarKey.resolve(key)
    if request.batch_size and request.batch_size < 0:
        raise errors.bad_request.ValidationError(
            "batch_size should be non negative number"
        )

    if not request.scroll_id:
        from_key_value = None
        total = None
        request.batch_size = request.batch_size or 10_000
    else:
        try:
            scroll = ScalarMetricsIterRawScroll.from_scroll_id(request.scroll_id)
        except ValueError:
            raise errors.bad_request.InvalidScrollId(scroll_id=request.scroll_id)

        if scroll.from_key_value is None:
            return make_response(
                scroll_id=request.scroll_id, total=scroll.total, variants={}
            )

        from_key_value = scalar_key.cast_value(scroll.from_key_value)
        total = scroll.total
        request.batch_size = request.batch_size or scroll.request.batch_size

    task_id = request.task

    task = task_bll.assert_exists(
        company_id, task_id, allow_public=True, only=("company",),
    )[0]

    metric_variants = _get_metric_variants_from_request([request.metric])

    if request.count_total and total is None:
        total = event_bll.events_iterator.count_task_events(
            event_type=EventType.metrics_scalar,
            company_id=task.company,
            task_id=task_id,
            metric_variants=metric_variants,
        )

    batch_size = min(
        request.batch_size,
        int(
            config.get("services.events.events_retrieval.max_raw_scalars_size", 200_000)
        ),
    )

    events = []
    for iteration in range(0, math.ceil(batch_size / 10_000)):
        res = event_bll.events_iterator.get_task_events(
            event_type=EventType.metrics_scalar,
            company_id=task.company,
            task_id=task_id,
            batch_size=min(batch_size, 10_000),
            navigate_earlier=False,
            from_key_value=from_key_value,
            metric_variants=metric_variants,
            key=key,
        )
        if not res.events:
            break
        events.extend(res.events)
        from_key_value = str(events[-1][scalar_key.field])

    key = str(key)
    variants = {
        variant: extract_properties_to_lists(
            ["value", scalar_key.field], events, target_keys=["y", key]
        )
        for variant, events in bucketize(events, key=itemgetter("variant")).items()
    }

    call.kpis["events"] = len(events)

    scroll = ScalarMetricsIterRawScroll(
        from_key_value=str(events[-1][scalar_key.field]) if events else None,
        total=total,
        request=request,
    )

    return make_response(
        returned=len(events),
        total=total,
        scroll_id=scroll.get_scroll_id(),
        variants=variants,
    )
