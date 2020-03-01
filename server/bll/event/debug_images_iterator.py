from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from itertools import chain
from operator import attrgetter, itemgetter

import attr
import dpath
from boltons.iterutils import bucketize
from elasticsearch import Elasticsearch
from redis import StrictRedis
from typing import Sequence, Tuple, Optional, Mapping

import database
from apierrors import errors
from bll.redis_cache_manager import RedisCacheManager
from bll.event.event_metrics import EventMetrics
from config import config
from database.errors import translate_errors_context
from jsonmodels.models import Base
from jsonmodels.fields import StringField, ListField, IntField

from database.model.task.metrics import MetricEventStats
from database.model.task.task import Task
from timing_context import TimingContext
from utilities.json import loads, dumps


class VariantScrollState(Base):
    name: str = StringField(required=True)
    recycle_url_marker: str = StringField()
    last_invalid_iteration: int = IntField()


class MetricScrollState(Base):
    task: str = StringField(required=True)
    name: str = StringField(required=True)
    last_min_iter: Optional[int] = IntField()
    last_max_iter: Optional[int] = IntField()
    timestamp: int = IntField(default=0)
    variants: Sequence[VariantScrollState] = ListField([VariantScrollState])

    def reset(self):
        """Reset the scrolling state for the metric"""
        self.last_min_iter = self.last_max_iter = None


class DebugImageEventsScrollState(Base):
    id: str = StringField(required=True)
    metrics: Sequence[MetricScrollState] = ListField([MetricScrollState])

    def to_json(self):
        return dumps(self.to_struct())

    @classmethod
    def from_json(cls, s):
        return cls(**loads(s))


@attr.s(auto_attribs=True)
class DebugImagesResult(object):
    metric_events: Sequence[tuple] = []
    next_scroll_id: str = None


class DebugImagesIterator:
    EVENT_TYPE = "training_debug_image"
    STATE_EXPIRATION_SECONDS = 3600

    @property
    def _max_workers(self):
        return config.get("services.events.max_metrics_concurrency", 4)

    def __init__(self, redis: StrictRedis, es: Elasticsearch):
        self.es = es
        self.cache_manager = RedisCacheManager(
            state_class=DebugImageEventsScrollState,
            redis=redis,
            expiration_interval=self.STATE_EXPIRATION_SECONDS,
        )

    def get_task_events(
        self,
        company_id: str,
        metrics: Sequence[Tuple[str, str]],
        iter_count: int,
        navigate_earlier: bool = True,
        refresh: bool = False,
        state_id: str = None,
    ) -> DebugImagesResult:
        es_index = EventMetrics.get_index_name(company_id, self.EVENT_TYPE)
        if not self.es.indices.exists(es_index):
            return DebugImagesResult()

        unique_metrics = set(metrics)
        state = self.cache_manager.get_state(state_id) if state_id else None
        if not state:
            state = DebugImageEventsScrollState(
                id=database.utils.id(),
                metrics=self._init_metric_states(es_index, list(unique_metrics)),
            )
        else:
            state_metrics = set((m.task, m.name) for m in state.metrics)
            if state_metrics != unique_metrics:
                raise errors.bad_request.InvalidScrollId(
                    "while getting debug images events", scroll_id=state_id
                )

            if refresh:
                self._reinit_outdated_metric_states(company_id, es_index, state)
                for metric_state in state.metrics:
                    metric_state.reset()

        res = DebugImagesResult(next_scroll_id=state.id)
        try:
            with ThreadPoolExecutor(self._max_workers) as pool:
                res.metric_events = list(
                    pool.map(
                        partial(
                            self._get_task_metric_events,
                            es_index=es_index,
                            iter_count=iter_count,
                            navigate_earlier=navigate_earlier,
                        ),
                        state.metrics,
                    )
                )
        finally:
            self.cache_manager.set_state(state)

        return res

    def _reinit_outdated_metric_states(
        self, company_id, es_index, state: DebugImageEventsScrollState
    ):
        """
        Determines the metrics for which new debug image events were added
        since their states were initialized and reinits these states
        """
        task_ids = set(metric.task for metric in state.metrics)
        tasks = Task.objects(id__in=list(task_ids), company=company_id).only(
            "id", "metric_stats"
        )

        def get_last_update_times_for_task_metrics(task: Task) -> Sequence[Tuple]:
            """For metrics that reported debug image events get tuples of task_id/metric_name and last update times"""
            metric_stats: Mapping[str, MetricEventStats] = task.metric_stats
            if not metric_stats:
                return []

            return [
                (
                    (task.id, stats.metric),
                    stats.event_stats_by_type[self.EVENT_TYPE].last_update,
                )
                for stats in metric_stats.values()
                if self.EVENT_TYPE in stats.event_stats_by_type
            ]

        update_times = dict(
            chain.from_iterable(
                get_last_update_times_for_task_metrics(task) for task in tasks
            )
        )
        outdated_metrics = [
            metric
            for metric in state.metrics
            if (metric.task, metric.name) in update_times
            and update_times[metric.task, metric.name] > metric.timestamp
        ]
        state.metrics = [
            *(metric for metric in state.metrics if metric not in outdated_metrics),
            *(
                self._init_metric_states(
                    es_index,
                    [(metric.task, metric.name) for metric in outdated_metrics],
                )
            ),
        ]

    def _init_metric_states(
        self, es_index, metrics: Sequence[Tuple[str, str]]
    ) -> Sequence[MetricScrollState]:
        """
        Returned initialized metric scroll stated for the requested task metrics
        """
        tasks = defaultdict(list)
        for (task, metric) in metrics:
            tasks[task].append(metric)

        with ThreadPoolExecutor(self._max_workers) as pool:
            return list(
                chain.from_iterable(
                    pool.map(
                        partial(self._init_metric_states_for_task, es_index=es_index),
                        tasks.items(),
                    )
                )
            )

    def _init_metric_states_for_task(
        self, task_metrics: Tuple[str, Sequence[str]], es_index
    ) -> Sequence[MetricScrollState]:
        """
        Return metric scroll states for the task filled with the variant states
        for the variants that reported any debug images
        """
        task, metrics = task_metrics
        es_req: dict = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [{"term": {"task": task}}, {"terms": {"metric": metrics}}]
                }
            },
            "aggs": {
                "metrics": {
                    "terms": {
                        "field": "metric",
                        "size": EventMetrics.MAX_METRICS_COUNT,
                    },
                    "aggs": {
                        "last_event_timestamp": {"max": {"field": "timestamp"}},
                        "variants": {
                            "terms": {
                                "field": "variant",
                                "size": EventMetrics.MAX_VARIANTS_COUNT,
                            },
                            "aggs": {
                                "urls": {
                                    "terms": {
                                        "field": "url",
                                        "order": {"max_iter": "desc"},
                                        "size": 1,  # we need only one url from the most recent iteration
                                    },
                                    "aggs": {
                                        "max_iter": {"max": {"field": "iter"}},
                                        "iters": {
                                            "top_hits": {
                                                "sort": {"iter": {"order": "desc"}},
                                                "size": 2,  # need two last iterations so that we can take
                                                # the second one as invalid
                                                "_source": "iter",
                                            }
                                        },
                                    },
                                }
                            },
                        },
                    },
                }
            },
        }

        with translate_errors_context(), TimingContext("es", "_init_metric_states"):
            es_res = self.es.search(index=es_index, body=es_req, routing=task)
        if "aggregations" not in es_res:
            return []

        def init_variant_scroll_state(variant: dict):
            """
            Return new variant scroll state for the passed variant bucket
            If the image urls get recycled then fill the last_invalid_iteration field
            """
            state = VariantScrollState(name=variant["key"])
            top_iter_url = dpath.get(variant, "urls/buckets")[0]
            iters = dpath.get(top_iter_url, "iters/hits/hits")
            if len(iters) > 1:
                state.last_invalid_iteration = dpath.get(iters[1], "_source/iter")
            return state

        return [
            MetricScrollState(
                task=task,
                name=metric["key"],
                variants=[
                    init_variant_scroll_state(variant)
                    for variant in dpath.get(metric, "variants/buckets")
                ],
                timestamp=dpath.get(metric, "last_event_timestamp/value"),
            )
            for metric in dpath.get(es_res, "aggregations/metrics/buckets")
        ]

    def _get_task_metric_events(
        self,
        metric: MetricScrollState,
        es_index: str,
        iter_count: int,
        navigate_earlier: bool,
    ) -> Tuple:
        """
        Return task metric events grouped by iterations
        Update metric scroll state
        """
        if metric.last_max_iter is None:
            # the first fetch is always from the latest iteration to the earlier ones
            navigate_earlier = True

        must_conditions = [
            {"term": {"task": metric.task}},
            {"term": {"metric": metric.name}},
        ]
        must_not_conditions = []

        range_condition = None
        if navigate_earlier and metric.last_min_iter is not None:
            range_condition = {"lt": metric.last_min_iter}
        elif not navigate_earlier and metric.last_max_iter is not None:
            range_condition = {"gt": metric.last_max_iter}
        if range_condition:
            must_conditions.append({"range": {"iter": range_condition}})

        if navigate_earlier:
            """
            When navigating to earlier iterations consider only
            variants whose invalid iterations border is lower than
            our starting iteration. For these variants make sure
            that only events from the valid iterations are returned 
            """
            if not metric.last_min_iter:
                variants = metric.variants
            else:
                variants = list(
                    v
                    for v in metric.variants
                    if v.last_invalid_iteration is None
                    or v.last_invalid_iteration < metric.last_min_iter
                )
                if not variants:
                    return metric.task, metric.name, []
                must_conditions.append(
                    {"terms": {"variant": list(v.name for v in variants)}}
                )
        else:
            """
            When navigating to later iterations all variants may be relevant.
            For the variants whose invalid border is higher than our starting 
            iteration make sure that only events from valid iterations are returned 
            """
            variants = list(
                v
                for v in metric.variants
                if v.last_invalid_iteration is not None
                and v.last_invalid_iteration > metric.last_max_iter
            )

        variants_conditions = [
            {
                "bool": {
                    "must": [
                        {"term": {"variant": v.name}},
                        {"range": {"iter": {"lte": v.last_invalid_iteration}}},
                    ]
                }
            }
            for v in variants
            if v.last_invalid_iteration is not None
        ]
        if variants_conditions:
            must_not_conditions.append({"bool": {"should": variants_conditions}})

        es_req = {
            "size": 0,
            "query": {
                "bool": {"must": must_conditions, "must_not": must_not_conditions}
            },
            "aggs": {
                "iters": {
                    "terms": {
                        "field": "iter",
                        "size": iter_count,
                        "order": {"_term": "desc" if navigate_earlier else "asc"},
                    },
                    "aggs": {
                        "variants": {
                            "terms": {
                                "field": "variant",
                                "size": EventMetrics.MAX_VARIANTS_COUNT,
                            },
                            "aggs": {
                                "events": {
                                    "top_hits": {"sort": {"url": {"order": "desc"}}}
                                }
                            },
                        }
                    },
                }
            },
        }
        with translate_errors_context(), TimingContext("es", "get_debug_image_events"):
            es_res = self.es.search(index=es_index, body=es_req, routing=metric.task)
        if "aggregations" not in es_res:
            return metric.task, metric.name, []

        def get_iteration_events(variant_buckets: Sequence[dict]) -> Sequence:
            return [
                ev["_source"]
                for v in variant_buckets
                for ev in dpath.get(v, "events/hits/hits")
            ]

        iterations = [
            {
                "iter": it["key"],
                "events": get_iteration_events(dpath.get(it, "variants/buckets")),
            }
            for it in dpath.get(es_res, "aggregations/iters/buckets")
        ]
        if not navigate_earlier:
            iterations.sort(key=itemgetter("iter"), reverse=True)
        if iterations:
            metric.last_max_iter = iterations[0]["iter"]
            metric.last_min_iter = iterations[-1]["iter"]

        # Commented for now since the last invalid iteration is calculated in the beginning
        # if navigate_earlier and any(
        #     variant.last_invalid_iteration is None for variant in variants
        # ):
        #     """
        #     Variants validation flags due to recycling can
        #     be set only on navigation to earlier frames
        #     """
        #     iterations = self._update_variants_invalid_iterations(variants, iterations)

        return metric.task, metric.name, iterations

    @staticmethod
    def _update_variants_invalid_iterations(
        variants: Sequence[VariantScrollState], iterations: Sequence[dict]
    ) -> Sequence[dict]:
        """
        This code is currently not in used since the invalid iterations
        are calculated during MetricState initialization
        For variants that do not have recycle url marker set it from the
        first event
        For variants that do not have last_invalid_iteration set check if the
        recycle marker was reached on a certain iteration and set it to the
        corresponding iteration
        For variants that have a newly set last_invalid_iteration remove
        events from the invalid iterations
        Return the updated iterations list
        """
        variants_lookup = bucketize(variants, attrgetter("name"))
        for it in iterations:
            iteration = it["iter"]
            events_to_remove = []
            for event in it["events"]:
                variant = variants_lookup[event["variant"]][0]
                if (
                    variant.last_invalid_iteration
                    and variant.last_invalid_iteration >= iteration
                ):
                    events_to_remove.append(event)
                    continue
                event_url = event.get("url")
                if not variant.recycle_url_marker:
                    variant.recycle_url_marker = event_url
                elif variant.recycle_url_marker == event_url:
                    variant.last_invalid_iteration = iteration
                    events_to_remove.append(event)
            if events_to_remove:
                it["events"] = [ev for ev in it["events"] if ev not in events_to_remove]
        return [it for it in iterations if it["events"]]
