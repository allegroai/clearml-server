import itertools
import math
from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from operator import itemgetter
from typing import Sequence, Tuple

from elasticsearch import Elasticsearch
from mongoengine import Q

from apiserver.apierrors import errors
from apiserver.bll.event.event_common import (
    EventType,
    EventSettings,
    search_company_events,
    check_empty_data,
    MetricVariants,
    get_metric_variants_condition,
)
from apiserver.bll.event.scalar_key import ScalarKey, ScalarKeyEnum
from apiserver.config_repo import config
from apiserver.database.errors import translate_errors_context
from apiserver.database.model.task.task import Task
from apiserver.timing_context import TimingContext
from apiserver.tools import safe_get

log = config.logger(__file__)


class EventMetrics:
    MAX_AGGS_ELEMENTS_COUNT = 50
    MAX_SAMPLE_BUCKETS = 6000

    def __init__(self, es: Elasticsearch):
        self.es = es

    def get_scalar_metrics_average_per_iter(
        self,
        company_id: str,
        task_id: str,
        samples: int,
        key: ScalarKeyEnum,
        metric_variants: MetricVariants = None,
    ) -> dict:
        """
        Get scalar metric histogram per metric and variant
        The amount of points in each histogram should not exceed
        the requested samples
        """
        event_type = EventType.metrics_scalar
        if check_empty_data(self.es, company_id=company_id, event_type=event_type):
            return {}

        return self._get_scalar_average_per_iter_core(
            task_id=task_id,
            company_id=company_id,
            event_type=event_type,
            samples=samples,
            key=ScalarKey.resolve(key),
            metric_variants=metric_variants,
        )

    def _get_scalar_average_per_iter_core(
        self,
        task_id: str,
        company_id: str,
        event_type: EventType,
        samples: int,
        key: ScalarKey,
        run_parallel: bool = True,
        metric_variants: MetricVariants = None,
    ) -> dict:
        intervals = self._get_task_metric_intervals(
            company_id=company_id,
            event_type=event_type,
            task_id=task_id,
            samples=samples,
            field=key.field,
            metric_variants=metric_variants,
        )
        if not intervals:
            return {}
        interval_groups = self._group_task_metric_intervals(intervals)

        get_scalar_average = partial(
            self._get_scalar_average,
            task_id=task_id,
            company_id=company_id,
            event_type=event_type,
            key=key,
        )
        if run_parallel:
            with ThreadPoolExecutor(max_workers=EventSettings.max_workers) as pool:
                metrics = itertools.chain.from_iterable(
                    pool.map(get_scalar_average, interval_groups)
                )
        else:
            metrics = itertools.chain.from_iterable(
                get_scalar_average(group) for group in interval_groups
            )

        ret = defaultdict(dict)
        for metric_key, metric_values in metrics:
            ret[metric_key].update(metric_values)

        return ret

    def compare_scalar_metrics_average_per_iter(
        self,
        company_id,
        task_ids: Sequence[str],
        samples,
        key: ScalarKeyEnum,
        allow_public=True,
    ):
        """
        Compare scalar metrics for different tasks per metric and variant
        The amount of points in each histogram should not exceed the requested samples
        """
        task_name_by_id = {}
        with translate_errors_context():
            task_objs = Task.get_many(
                company=company_id,
                query=Q(id__in=task_ids),
                allow_public=allow_public,
                override_projection=("id", "name", "company", "company_origin"),
                return_dicts=False,
            )
            if len(task_objs) < len(task_ids):
                invalid = tuple(set(task_ids) - set(r.id for r in task_objs))
                raise errors.bad_request.InvalidTaskId(company=company_id, ids=invalid)
            task_name_by_id = {t.id: t.name for t in task_objs}

        companies = {t.get_index_company() for t in task_objs}
        if len(companies) > 1:
            raise errors.bad_request.InvalidTaskId(
                "only tasks from the same company are supported"
            )

        event_type = EventType.metrics_scalar
        company_id = next(iter(companies))
        if check_empty_data(self.es, company_id=company_id, event_type=event_type):
            return {}

        get_scalar_average_per_iter = partial(
            self._get_scalar_average_per_iter_core,
            company_id=company_id,
            event_type=event_type,
            samples=samples,
            key=ScalarKey.resolve(key),
            run_parallel=False,
        )
        with ThreadPoolExecutor(max_workers=EventSettings.max_workers) as pool:
            task_metrics = zip(
                task_ids, pool.map(get_scalar_average_per_iter, task_ids)
            )

        res = defaultdict(lambda: defaultdict(dict))
        for task_id, task_data in task_metrics:
            task_name = task_name_by_id[task_id]
            for metric_key, metric_data in task_data.items():
                for variant_key, variant_data in metric_data.items():
                    variant_data["name"] = task_name
                    res[metric_key][variant_key][task_id] = variant_data

        return res

    MetricInterval = Tuple[str, str, int, int]
    MetricIntervalGroup = Tuple[int, Sequence[Tuple[str, str]]]

    @classmethod
    def _group_task_metric_intervals(
        cls, intervals: Sequence[MetricInterval]
    ) -> Sequence[MetricIntervalGroup]:
        """
        Group task metric intervals so that the following conditions are meat:
            - All the metrics in the same group have the same interval (with 10% rounding)
            - The amount of metrics in the group does not exceed MAX_AGGS_ELEMENTS_COUNT
            - The total count of samples in the group does not exceed MAX_SAMPLE_BUCKETS
        """
        metric_interval_groups = []
        interval_group = []
        group_interval_upper_bound = 0
        group_max_interval = 0
        group_samples = 0
        for metric, variant, interval, size in sorted(intervals, key=itemgetter(2)):
            if (
                interval > group_interval_upper_bound
                or (group_samples + size) > cls.MAX_SAMPLE_BUCKETS
                or len(interval_group) >= cls.MAX_AGGS_ELEMENTS_COUNT
            ):
                if interval_group:
                    metric_interval_groups.append((group_max_interval, interval_group))
                    interval_group = []
                group_max_interval = interval
                group_interval_upper_bound = interval + int(interval * 0.1)
                group_samples = 0
            interval_group.append((metric, variant))
            group_samples += size
            group_max_interval = max(group_max_interval, interval)
        if interval_group:
            metric_interval_groups.append((group_max_interval, interval_group))

        return metric_interval_groups

    def _get_task_metric_intervals(
        self,
        company_id: str,
        event_type: EventType,
        task_id: str,
        samples: int,
        field: str = "iter",
        metric_variants: MetricVariants = None,
    ) -> Sequence[MetricInterval]:
        """
        Calculate interval per task metric variant so that the resulting
        amount of points does not exceed sample.
        Return the list og metric variant intervals as the following tuple:
        (metric, variant, interval, samples)
        """
        must = [{"term": {"task": task_id}}]
        if metric_variants:
            must.append(get_metric_variants_condition(metric_variants))
        query = {"bool": {"must": must}}

        es_req = {
            "size": 0,
            "query": query,
            "aggs": {
                "metrics": {
                    "terms": {
                        "field": "metric",
                        "size": EventSettings.max_metrics_count,
                        "order": {"_key": "asc"},
                    },
                    "aggs": {
                        "variants": {
                            "terms": {
                                "field": "variant",
                                "size": EventSettings.max_variants_count,
                                "order": {"_key": "asc"},
                            },
                            "aggs": {
                                "count": {"value_count": {"field": field}},
                                "min_index": {"min": {"field": field}},
                                "max_index": {"max": {"field": field}},
                            },
                        }
                    },
                }
            },
        }

        with translate_errors_context(), TimingContext("es", "task_stats_get_interval"):
            es_res = search_company_events(
                self.es, company_id=company_id, event_type=event_type, body=es_req,
            )

        aggs_result = es_res.get("aggregations")
        if not aggs_result:
            return []

        return [
            self._build_metric_interval(metric["key"], variant["key"], variant, samples)
            for metric in aggs_result["metrics"]["buckets"]
            for variant in metric["variants"]["buckets"]
        ]

    @staticmethod
    def _build_metric_interval(
        metric: str, variant: str, data: dict, samples: int
    ) -> Tuple[str, str, int, int]:
        """
        Calculate index interval per metric_variant variant so that the
        total amount of intervals does not exceeds the samples
        Return the interval and resulting amount of intervals
        """
        count = safe_get(data, "count/value", default=0)
        if count < samples:
            return metric, variant, 1, count

        min_index = safe_get(data, "min_index/value", default=0)
        max_index = safe_get(data, "max_index/value", default=min_index)
        index_range = max_index - min_index + 1
        interval = max(1, math.ceil(float(index_range) / samples))
        max_samples = math.ceil(float(index_range) / interval)
        return (
            metric,
            variant,
            interval,
            max_samples,
        )

    MetricData = Tuple[str, dict]

    def _get_scalar_average(
        self,
        metrics_interval: MetricIntervalGroup,
        task_id: str,
        company_id: str,
        event_type: EventType,
        key: ScalarKey,
    ) -> Sequence[MetricData]:
        """
        Retrieve scalar histograms per several metric variants that share the same interval
        """
        interval, metrics = metrics_interval
        aggregation = self._add_aggregation_average(key.get_aggregation(interval))
        aggs = {
            "metrics": {
                "terms": {
                    "field": "metric",
                    "size": EventSettings.max_metrics_count,
                    "order": {"_key": "asc"},
                },
                "aggs": {
                    "variants": {
                        "terms": {
                            "field": "variant",
                            "size": EventSettings.max_variants_count,
                            "order": {"_key": "asc"},
                        },
                        "aggs": aggregation,
                    }
                },
            }
        }
        aggs_result = self._query_aggregation_for_task_metrics(
            company_id=company_id,
            event_type=event_type,
            aggs=aggs,
            task_id=task_id,
            metrics=metrics,
        )

        if not aggs_result:
            return {}

        metrics = [
            (
                metric["key"],
                {
                    variant["key"]: {
                        "name": variant["key"],
                        **key.get_iterations_data(variant),
                    }
                    for variant in metric["variants"]["buckets"]
                },
            )
            for metric in aggs_result["metrics"]["buckets"]
        ]
        return metrics

    @staticmethod
    def _add_aggregation_average(aggregation):
        average_agg = {"avg_val": {"avg": {"field": "value"}}}
        return {
            key: {**value, "aggs": {**value.get("aggs", {}), **average_agg}}
            for key, value in aggregation.items()
        }

    def _query_aggregation_for_task_metrics(
        self,
        company_id: str,
        event_type: EventType,
        aggs: dict,
        task_id: str,
        metrics: Sequence[Tuple[str, str]],
    ) -> dict:
        """
        Return the result of elastic search query for the given aggregation filtered
        by the given task_ids and metrics
        """
        must = [{"term": {"task": task_id}}]
        if metrics:
            should = [
                {
                    "bool": {
                        "must": [
                            {"term": {"metric": metric}},
                            {"term": {"variant": variant}},
                        ]
                    }
                }
                for metric, variant in metrics
            ]
            must.append({"bool": {"should": should}})

        es_req = {
            "size": 0,
            "query": {"bool": {"must": must}},
            "aggs": aggs,
        }

        with translate_errors_context(), TimingContext("es", "task_stats_scalar"):
            es_res = search_company_events(
                self.es, company_id=company_id, event_type=event_type, body=es_req,
            )

        return es_res.get("aggregations")

    def get_tasks_metrics(
        self, company_id, task_ids: Sequence, event_type: EventType
    ) -> Sequence:
        """
        For the requested tasks return all the metrics that
        reported events of the requested types
        """
        if check_empty_data(self.es, company_id, event_type):
            return {}

        with ThreadPoolExecutor(EventSettings.max_workers) as pool:
            res = pool.map(
                partial(
                    self._get_task_metrics,
                    company_id=company_id,
                    event_type=event_type,
                ),
                task_ids,
            )
        return list(zip(task_ids, res))

    def _get_task_metrics(
        self, task_id: str, company_id: str, event_type: EventType
    ) -> Sequence:
        es_req = {
            "size": 0,
            "query": {"bool": {"must": [{"term": {"task": task_id}}]}},
            "aggs": {
                "metrics": {
                    "terms": {
                        "field": "metric",
                        "size": EventSettings.max_metrics_count,
                        "order": {"_key": "asc"},
                    }
                }
            },
        }

        with translate_errors_context(), TimingContext("es", "_get_task_metrics"):
            es_res = search_company_events(
                self.es, company_id=company_id, event_type=event_type, body=es_req
            )

        return [
            metric["key"]
            for metric in safe_get(es_res, "aggregations/metrics/buckets", default=[])
        ]
