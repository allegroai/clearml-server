import itertools
from operator import attrgetter
from typing import Optional, Sequence, Union

from boltons.iterutils import bucketize

from apiserver.apierrors.errors import bad_request
from apiserver.apimodels.workers import (
    WorkerRequest,
    StatusReportRequest,
    GetAllRequest,
    GetAllResponse,
    RegisterRequest,
    GetStatsRequest,
    MetricCategory,
    GetMetricKeysRequest,
    GetMetricKeysResponse,
    GetStatsResponse,
    WorkerStatistics,
    MetricStats,
    AggregationStats,
    GetActivityReportRequest,
    GetActivityReportResponse,
    ActivityReportSeries,
    GetCountRequest,
)
from apiserver.bll.workers import WorkerBLL
from apiserver.config_repo import config
from apiserver.service_repo import APICall, endpoint
from apiserver.utilities import extract_properties_to_lists

log = config.logger(__file__)

worker_bll = WorkerBLL()


@endpoint(
    "workers.get_all",
    min_version="2.4",
    request_data_model=GetAllRequest,
    response_data_model=GetAllResponse,
)
def get_all(call: APICall, company_id: str, request: GetAllRequest):
    call.result.data_model = GetAllResponse(
        workers=worker_bll.get_all_with_projection(
            company_id,
            request.last_seen,
            tags=request.tags,
            system_tags=request.system_tags,
        )
    )


@endpoint(
    "workers.get_count", request_data_model=GetCountRequest,
)
def get_all(call: APICall, company_id: str, request: GetCountRequest):
    call.result.data = {
        "count": worker_bll.get_count(
            company_id,
            request.last_seen,
            tags=request.tags,
            system_tags=request.system_tags,
        )
    }


@endpoint("workers.register", min_version="2.4", request_data_model=RegisterRequest)
def register(call: APICall, company_id, request: RegisterRequest):
    worker = request.worker
    timeout = request.timeout
    queues = request.queues

    if not timeout:
        timeout = config.get("apiserver.workers.default_timeout", 10 * 60)

    if not timeout or timeout <= 0:
        raise bad_request.WorkerRegistrationFailed(
            "invalid timeout", timeout=timeout, worker=worker
        )

    worker_bll.register_worker(
        company_id=company_id,
        user_id=call.identity.user,
        worker=worker,
        ip=call.real_ip,
        queues=queues,
        timeout=timeout,
        tags=request.tags,
        system_tags=request.system_tags,
    )


@endpoint("workers.unregister", min_version="2.4", request_data_model=WorkerRequest)
def unregister(call: APICall, company_id, req_model: WorkerRequest):
    worker_bll.unregister_worker(company_id, call.identity.user, req_model.worker)


@endpoint(
    "workers.status_report", min_version="2.4", request_data_model=StatusReportRequest
)
def status_report(call: APICall, company_id, request: StatusReportRequest):
    worker_bll.status_report(
        company_id=company_id,
        user_id=call.identity.user,
        ip=call.real_ip,
        report=request,
        tags=request.tags,
        system_tags=request.system_tags,
    )


@endpoint(
    "workers.get_metric_keys",
    min_version="2.4",
    request_data_model=GetMetricKeysRequest,
    response_data_model=GetMetricKeysResponse,
    validate_schema=True,
)
def get_metric_keys(
    call: APICall, company_id, req_model: GetMetricKeysRequest
) -> GetMetricKeysResponse:
    ret = worker_bll.stats.get_worker_stats_keys(
        company_id, worker_ids=req_model.worker_ids
    )
    return GetMetricKeysResponse(
        categories=[MetricCategory(name=k, metric_keys=v) for k, v in ret.items()]
    )


@endpoint(
    "workers.get_activity_report",
    min_version="2.4",
    request_data_model=GetActivityReportRequest,
    response_data_model=GetActivityReportResponse,
    validate_schema=True,
)
def get_activity_report(
    call: APICall, company_id, req_model: GetActivityReportRequest
) -> GetActivityReportResponse:
    def get_activity_series(active_only: bool = False) -> ActivityReportSeries:
        ret = worker_bll.stats.get_activity_report(
            company_id=company_id,
            from_date=req_model.from_date,
            to_date=req_model.to_date,
            interval=req_model.interval,
            active_only=active_only,
        )
        if not ret:
            return ActivityReportSeries(dates=[], counts=[])
        count_by_date = extract_properties_to_lists(["date", "count"], ret)
        return ActivityReportSeries(
            dates=count_by_date["date"], counts=count_by_date["count"]
        )

    return GetActivityReportResponse(
        total=get_activity_series(), active=get_activity_series(active_only=True)
    )


@endpoint(
    "workers.get_stats",
    min_version="2.4",
    request_data_model=GetStatsRequest,
    response_data_model=GetStatsResponse,
    validate_schema=True,
)
def get_stats(call: APICall, company_id, request: GetStatsRequest):
    ret = worker_bll.stats.get_worker_stats(company_id, request)

    def _get_variant_metric_stats(
        metric: str,
        agg_names: Sequence[str],
        stats: Sequence[dict],
        variant: Optional[str] = None,
    ) -> MetricStats:
        stat_by_name = extract_properties_to_lists(agg_names, stats)
        return MetricStats(
            metric=metric,
            variant=variant,
            dates=stat_by_name["date"],
            stats=[
                AggregationStats(aggregation=name, values=aggs)
                for name, aggs in stat_by_name.items()
                if name != "date"
            ],
        )

    def _get_metric_stats(
        metric: str, stats: Union[dict, Sequence[dict]], agg_types: Sequence[str]
    ) -> Sequence[MetricStats]:
        """
        Return statistics for a certain metric or a list of statistic for
        metric variants if break_by_variant was requested
        """
        agg_names = ["date"] + list(set(agg_types))
        if not isinstance(stats, dict):
            # no variants were requested
            return [_get_variant_metric_stats(metric, agg_names, stats)]

        return [
            _get_variant_metric_stats(metric, agg_names, variant_stats, variant)
            for variant, variant_stats in stats.items()
        ]

    def _get_worker_metrics(stats: dict) -> Sequence[MetricStats]:
        """
        Convert the worker statistics data from the internal format of lists of structs
        to a more "compact" format for json transfer (arrays of dates and arrays of values)
        """
        # removed metrics that were requested but for some reason
        # do not exist in stats data
        metrics = [metric for metric in request.items if metric.key in stats]

        aggs_by_metric = bucketize(
            metrics, key=attrgetter("key"), value_transform=attrgetter("aggregation")
        )

        return list(
            itertools.chain.from_iterable(
                _get_metric_stats(metric, metric_stats, aggs_by_metric[metric])
                for metric, metric_stats in stats.items()
            )
        )

    return GetStatsResponse(
        workers=[
            WorkerStatistics(worker=worker, metrics=_get_worker_metrics(stats))
            for worker, stats in ret.items()
        ]
    )
