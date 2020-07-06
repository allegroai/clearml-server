import itertools
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from functools import partial
from operator import itemgetter
from typing import Sequence, Tuple, Callable, Iterable

from boltons.iterutils import bucketize
from elasticsearch import Elasticsearch
from mongoengine import Q

from apierrors import errors
from bll.event.scalar_key import ScalarKey, ScalarKeyEnum
from config import config
from database.errors import translate_errors_context
from database.model.task.task import Task
from timing_context import TimingContext
from utilities import safe_get

log = config.logger(__file__)


class EventType(Enum):
    metrics_scalar = "training_stats_scalar"
    metrics_vector = "training_stats_vector"
    metrics_image = "training_debug_image"
    metrics_plot = "plot"
    task_log = "log"


class EventMetrics:
    MAX_TASKS_COUNT = 50
    MAX_METRICS_COUNT = 200
    MAX_VARIANTS_COUNT = 500
    MAX_AGGS_ELEMENTS_COUNT = 50

    def __init__(self, es: Elasticsearch):
        self.es = es

    @staticmethod
    def get_index_name(company_id, event_type):
        event_type = event_type.lower().replace(" ", "_")
        return f"events-{event_type}-{company_id}"

    def get_scalar_metrics_average_per_iter(
        self, company_id: str, task_id: str, samples: int, key: ScalarKeyEnum
    ) -> dict:
        """
        Get scalar metric histogram per metric and variant
        The amount of points in each histogram should not exceed
        the requested samples
        """

        return self._run_get_scalar_metrics_as_parallel(
            company_id,
            task_ids=[task_id],
            samples=samples,
            key=ScalarKey.resolve(key),
            get_func=self._get_scalar_average,
        )

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
        if len(task_ids) > self.MAX_TASKS_COUNT:
            raise errors.BadRequest(
                f"Up to {self.MAX_TASKS_COUNT} tasks supported for comparison",
                len(task_ids),
            )

        task_name_by_id = {}
        with translate_errors_context():
            task_objs = Task.get_many(
                company=company_id,
                query=Q(id__in=task_ids),
                allow_public=allow_public,
                override_projection=("id", "name", "company"),
                return_dicts=False,
            )
            if len(task_objs) < len(task_ids):
                invalid = tuple(set(task_ids) - set(r.id for r in task_objs))
                raise errors.bad_request.InvalidTaskId(company=company_id, ids=invalid)

            task_name_by_id = {t.id: t.name for t in task_objs}

        companies = {t.company for t in task_objs}
        if len(companies) > 1:
            raise errors.bad_request.InvalidTaskId(
                "only tasks from the same company are supported"
            )

        ret = self._run_get_scalar_metrics_as_parallel(
            next(iter(companies)),
            task_ids=task_ids,
            samples=samples,
            key=ScalarKey.resolve(key),
            get_func=self._get_scalar_average_per_task,
        )

        for metric_data in ret.values():
            for variant_data in metric_data.values():
                for task_id, task_data in variant_data.items():
                    task_data["name"] = task_name_by_id[task_id]

        return ret

    TaskMetric = Tuple[str, str, str]

    MetricInterval = Tuple[int, Sequence[TaskMetric]]
    MetricData = Tuple[str, dict]

    def _split_metrics_by_max_aggs_count(
        self, task_metrics: Sequence[TaskMetric]
    ) -> Iterable[Sequence[TaskMetric]]:
        """
        Return task metrics in groups where amount of task metrics in each group
        is roughly limited by MAX_AGGS_ELEMENTS_COUNT. The split is done on metrics and
        variants while always preserving all their tasks in the same group
        """
        if len(task_metrics) < self.MAX_AGGS_ELEMENTS_COUNT:
            yield task_metrics
            return

        tm_grouped = bucketize(task_metrics, key=itemgetter(1, 2))
        groups = []
        for group in tm_grouped.values():
            groups.append(group)
            if sum(map(len, groups)) >= self.MAX_AGGS_ELEMENTS_COUNT:
                yield list(itertools.chain(*groups))
                groups = []

        if groups:
            yield list(itertools.chain(*groups))

        return

    def _run_get_scalar_metrics_as_parallel(
        self,
        company_id: str,
        task_ids: Sequence[str],
        samples: int,
        key: ScalarKey,
        get_func: Callable[
            [MetricInterval, Sequence[str], str, ScalarKey], Sequence[MetricData]
        ],
    ) -> dict:
        """
        Group metrics per interval length and execute get_func for each group in parallel
        :param company_id: id of the company
        :params task_ids: ids of the tasks to collect data for
        :param samples: maximum number of samples per metric
        :param get_func: callable that given metric names for the same interval
        performs histogram aggregation for the metrics and return the aggregated data
        """
        es_index = self.get_index_name(company_id, "training_stats_scalar")
        if not self.es.indices.exists(es_index):
            return {}

        intervals = self._get_metric_intervals(
            es_index=es_index, task_ids=task_ids, samples=samples, field=key.field
        )

        if not intervals:
            return {}

        intervals = list(
            itertools.chain.from_iterable(
                zip(itertools.repeat(i), self._split_metrics_by_max_aggs_count(tms))
                for i, tms in intervals
            )
        )
        max_concurrency = config.get("services.events.max_metrics_concurrency", 4)
        with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
            metrics = itertools.chain.from_iterable(
                pool.map(
                    partial(get_func, task_ids=task_ids, es_index=es_index, key=key),
                    intervals,
                )
            )

        ret = defaultdict(dict)
        for metric_key, metric_values in metrics:
            ret[metric_key].update(metric_values)

        return ret

    def _get_metric_intervals(
        self, es_index, task_ids: Sequence[str], samples: int, field: str = "iter"
    ) -> Sequence[MetricInterval]:
        """
        Calculate interval per task metric variant so that the resulting
        amount of points does not exceed sample.
        Return metric variants grouped by interval value with 10% rounding
        For samples==0 return empty list
        """
        default_intervals = [(1, [])]
        if not samples:
            return default_intervals

        es_req = {
            "size": 0,
            "query": {"terms": {"task": task_ids}},
            "aggs": {
                "tasks": {
                    "terms": {"field": "task", "size": self.MAX_TASKS_COUNT},
                    "aggs": {
                        "metrics": {
                            "terms": {
                                "field": "metric",
                                "size": self.MAX_METRICS_COUNT,
                            },
                            "aggs": {
                                "variants": {
                                    "terms": {
                                        "field": "variant",
                                        "size": self.MAX_VARIANTS_COUNT,
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
            },
        }

        with translate_errors_context(), TimingContext("es", "task_stats_get_interval"):
            es_res = self.es.search(
                index=es_index, body=es_req, routing=",".join(task_ids)
            )

        aggs_result = es_res.get("aggregations")
        if not aggs_result:
            return default_intervals

        intervals = [
            (
                task["key"],
                metric["key"],
                variant["key"],
                self._calculate_metric_interval(variant, samples),
            )
            for task in aggs_result["tasks"]["buckets"]
            for metric in task["metrics"]["buckets"]
            for variant in metric["variants"]["buckets"]
        ]

        metric_intervals = []
        upper_border = 0
        interval_metrics = None
        for task, metric, variant, interval in sorted(intervals, key=itemgetter(3)):
            if not interval_metrics or interval > upper_border:
                interval_metrics = []
                metric_intervals.append((interval, interval_metrics))
                upper_border = interval + int(interval * 0.1)
            interval_metrics.append((task, metric, variant))

        return metric_intervals

    @staticmethod
    def _calculate_metric_interval(metric_variant: dict, samples: int) -> int:
        """
        Calculate index interval per metric_variant variant so that the
        total amount of intervals does not exceeds the samples
        """
        count = safe_get(metric_variant, "count/value")
        if not count or count < samples:
            return 1

        min_index = safe_get(metric_variant, "min_index/value", default=0)
        max_index = safe_get(metric_variant, "max_index/value", default=min_index)
        return max(1, int(max_index - min_index + 1) // samples)

    def _get_scalar_average(
        self,
        metrics_interval: MetricInterval,
        task_ids: Sequence[str],
        es_index: str,
        key: ScalarKey,
    ) -> Sequence[MetricData]:
        """
        Retrieve scalar histograms per several metric variants that share the same interval
        Note: the function works with a single task only
        """

        assert len(task_ids) == 1
        interval, task_metrics = metrics_interval
        aggregation = self._add_aggregation_average(key.get_aggregation(interval))
        aggs = {
            "metrics": {
                "terms": {
                    "field": "metric",
                    "size": self.MAX_METRICS_COUNT,
                    "order": {"_term": "desc"},
                },
                "aggs": {
                    "variants": {
                        "terms": {
                            "field": "variant",
                            "size": self.MAX_VARIANTS_COUNT,
                            "order": {"_term": "desc"},
                        },
                        "aggs": aggregation,
                    }
                },
            }
        }
        aggs_result = self._query_aggregation_for_metrics_and_tasks(
            es_index, aggs=aggs, task_ids=task_ids, task_metrics=task_metrics
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

    def _get_scalar_average_per_task(
        self,
        metrics_interval: MetricInterval,
        task_ids: Sequence[str],
        es_index: str,
        key: ScalarKey,
    ) -> Sequence[MetricData]:
        """
        Retrieve scalar histograms per several metric variants that share the same interval
        """
        interval, task_metrics = metrics_interval

        aggregation = self._add_aggregation_average(key.get_aggregation(interval))
        aggs = {
            "metrics": {
                "terms": {"field": "metric", "size": self.MAX_METRICS_COUNT},
                "aggs": {
                    "variants": {
                        "terms": {"field": "variant", "size": self.MAX_VARIANTS_COUNT},
                        "aggs": {
                            "tasks": {
                                "terms": {
                                    "field": "task",
                                    "size": self.MAX_TASKS_COUNT,
                                },
                                "aggs": aggregation,
                            }
                        },
                    }
                },
            }
        }

        aggs_result = self._query_aggregation_for_metrics_and_tasks(
            es_index, aggs=aggs, task_ids=task_ids, task_metrics=task_metrics
        )

        if not aggs_result:
            return {}

        metrics = [
            (
                metric["key"],
                {
                    variant["key"]: {
                        task["key"]: key.get_iterations_data(task)
                        for task in variant["tasks"]["buckets"]
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

    def _query_aggregation_for_metrics_and_tasks(
        self,
        es_index: str,
        aggs: dict,
        task_ids: Sequence[str],
        task_metrics: Sequence[TaskMetric],
    ) -> dict:
        """
        Return the result of elastic search query for the given aggregation filtered
        by the given task_ids and metrics
        """
        if task_metrics:
            condition = {
                "should": [
                    self._build_metric_terms(task, metric, variant)
                    for task, metric, variant in task_metrics
                ]
            }
        else:
            condition = {"must": [{"terms": {"task": task_ids}}]}
        es_req = {
            "size": 0,
            "_source": {"excludes": []},
            "query": {"bool": condition},
            "aggs": aggs,
            "version": True,
        }

        with translate_errors_context(), TimingContext("es", "task_stats_scalar"):
            es_res = self.es.search(
                index=es_index, body=es_req, routing=",".join(task_ids)
            )

        return es_res.get("aggregations")

    @staticmethod
    def _build_metric_terms(task: str, metric: str, variant: str) -> dict:
        """
        Build query term for a metric + variant
        """
        return {
            "bool": {
                "must": [
                    {"term": {"task": task}},
                    {"term": {"metric": metric}},
                    {"term": {"variant": variant}},
                ]
            }
        }

    def get_tasks_metrics(
        self, company_id, task_ids: Sequence, event_type: EventType
    ) -> Sequence[Tuple]:
        """
        For the requested tasks return all the metrics that
        reported events of the requested types
        """
        es_index = EventMetrics.get_index_name(company_id, event_type.value)
        if not self.es.indices.exists(es_index):
            return [(tid, []) for tid in task_ids]

        max_concurrency = config.get("services.events.max_metrics_concurrency", 4)
        with ThreadPoolExecutor(max_concurrency) as pool:
            res = pool.map(
                partial(
                    self._get_task_metrics, es_index=es_index, event_type=event_type,
                ),
                task_ids,
            )
        return list(zip(task_ids, res))

    def _get_task_metrics(self, task_id, es_index, event_type: EventType) -> Sequence:
        es_req = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"task": task_id}},
                        {"term": {"type": event_type.value}},
                    ]
                }
            },
            "aggs": {
                "metrics": {
                    "terms": {"field": "metric", "size": self.MAX_METRICS_COUNT}
                }
            },
        }

        with translate_errors_context(), TimingContext("es", "_get_task_metrics"):
            es_res = self.es.search(index=es_index, body=es_req, routing=task_id)

        return [
            metric["key"]
            for metric in safe_get(es_res, "aggregations/metrics/buckets", default=[])
        ]
