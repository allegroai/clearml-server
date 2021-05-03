from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from operator import itemgetter
from typing import Sequence, Tuple, Optional, Mapping, Set

import attr
import dpath
from boltons.iterutils import first
from elasticsearch import Elasticsearch
from jsonmodels.fields import StringField, ListField, IntField
from jsonmodels.models import Base
from redis import StrictRedis

from apiserver.apimodels import JsonSerializableMixin
from apiserver.bll.event.event_common import (
    EventSettings,
    check_empty_data,
    search_company_events,
    EventType,
)
from apiserver.bll.redis_cache_manager import RedisCacheManager
from apiserver.database.errors import translate_errors_context
from apiserver.database.model.task.metrics import MetricEventStats
from apiserver.database.model.task.task import Task
from apiserver.timing_context import TimingContext


class VariantState(Base):
    variant: str = StringField(required=True)
    last_invalid_iteration: int = IntField()


class MetricState(Base):
    metric: str = StringField(required=True)
    variants: Sequence[VariantState] = ListField([VariantState], required=True)
    timestamp: int = IntField(default=0)


class TaskScrollState(Base):
    task: str = StringField(required=True)
    metrics: Sequence[MetricState] = ListField([MetricState], required=True)
    last_min_iter: Optional[int] = IntField()
    last_max_iter: Optional[int] = IntField()

    def reset(self):
        """Reset the scrolling state for the metric"""
        self.last_min_iter = self.last_max_iter = None


class DebugImageEventsScrollState(Base, JsonSerializableMixin):
    id: str = StringField(required=True)
    tasks: Sequence[TaskScrollState] = ListField([TaskScrollState])
    warning: str = StringField()


@attr.s(auto_attribs=True)
class DebugImagesResult(object):
    metric_events: Sequence[tuple] = []
    next_scroll_id: str = None


class DebugImagesIterator:
    EVENT_TYPE = EventType.metrics_image

    def __init__(self, redis: StrictRedis, es: Elasticsearch):
        self.es = es
        self.cache_manager = RedisCacheManager(
            state_class=DebugImageEventsScrollState,
            redis=redis,
            expiration_interval=EventSettings.state_expiration_sec,
        )

    def get_task_events(
        self,
        company_id: str,
        task_metrics: Mapping[str, Set[str]],
        iter_count: int,
        navigate_earlier: bool = True,
        refresh: bool = False,
        state_id: str = None,
    ) -> DebugImagesResult:
        if check_empty_data(self.es, company_id, self.EVENT_TYPE):
            return DebugImagesResult()

        def init_state(state_: DebugImageEventsScrollState):
            state_.tasks = self._init_task_states(company_id, task_metrics)

        def validate_state(state_: DebugImageEventsScrollState):
            """
            Validate that the metrics stored in the state are the same
            as requested in the current call.
            Refresh the state if requested
            """
            if refresh:
                self._reinit_outdated_task_states(company_id, state_, task_metrics)

        with self.cache_manager.get_or_create_state(
            state_id=state_id, init_state=init_state, validate_state=validate_state
        ) as state:
            res = DebugImagesResult(next_scroll_id=state.id)
            with ThreadPoolExecutor(EventSettings.max_workers) as pool:
                res.metric_events = list(
                    pool.map(
                        partial(
                            self._get_task_metric_events,
                            company_id=company_id,
                            iter_count=iter_count,
                            navigate_earlier=navigate_earlier,
                        ),
                        state.tasks,
                    )
                )

            return res

    def _reinit_outdated_task_states(
        self,
        company_id,
        state: DebugImageEventsScrollState,
        task_metrics: Mapping[str, Set[str]],
    ):
        """
        Determine the metrics for which new debug image events were added
        since their states were initialized and re-init these states
        """
        tasks = Task.objects(id__in=list(task_metrics), company=company_id).only(
            "id", "metric_stats"
        )

        def get_last_update_times_for_task_metrics(
            task: Task,
        ) -> Mapping[str, datetime]:
            """For metrics that reported debug image events get mapping of the metric name to the last update times"""
            metric_stats: Mapping[str, MetricEventStats] = task.metric_stats
            if not metric_stats:
                return {}

            requested_metrics = task_metrics[task.id]
            return {
                stats.metric: stats.event_stats_by_type[
                    self.EVENT_TYPE.value
                ].last_update
                for stats in metric_stats.values()
                if self.EVENT_TYPE.value in stats.event_stats_by_type
                and (not requested_metrics or stats.metric in requested_metrics)
            }

        update_times = {
            task.id: get_last_update_times_for_task_metrics(task) for task in tasks
        }
        task_metric_states = {
            task_state.task: {
                metric_state.metric: metric_state for metric_state in task_state.metrics
            }
            for task_state in state.tasks
        }
        task_metrics_to_recalc = {}
        for task, metrics_times in update_times.items():
            old_metric_states = task_metric_states[task]
            metrics_to_recalc = set(
                m
                for m, t in metrics_times.items()
                if m not in old_metric_states or old_metric_states[m].timestamp < t
            )
            if metrics_to_recalc:
                task_metrics_to_recalc[task] = metrics_to_recalc

        updated_task_states = self._init_task_states(company_id, task_metrics_to_recalc)

        def merge_with_updated_task_states(
            old_state: TaskScrollState, updates: Sequence[TaskScrollState]
        ) -> TaskScrollState:
            task = old_state.task
            updated_state = first(uts for uts in updates if uts.task == task)
            if not updated_state:
                old_state.reset()
                return old_state

            updated_metrics = [m.metric for m in updated_state.metrics]
            return TaskScrollState(
                task=task,
                metrics=[
                    *updated_state.metrics,
                    *(
                        old_metric
                        for old_metric in old_state.metrics
                        if old_metric.metric not in updated_metrics
                    ),
                ],
            )

        state.tasks = [
            merge_with_updated_task_states(task_state, updated_task_states)
            for task_state in state.tasks
        ]

    def _init_task_states(
        self, company_id: str, task_metrics: Mapping[str, Set[str]]
    ) -> Sequence[TaskScrollState]:
        """
        Returned initialized metric scroll stated for the requested task metrics
        """
        with ThreadPoolExecutor(EventSettings.max_workers) as pool:
            task_metric_states = pool.map(
                partial(self._init_metric_states_for_task, company_id=company_id),
                task_metrics.items(),
            )

        return [
            TaskScrollState(task=task, metrics=metric_states,)
            for task, metric_states in zip(task_metrics, task_metric_states)
        ]

    def _init_metric_states_for_task(
        self, task_metrics: Tuple[str, Set[str]], company_id: str
    ) -> Sequence[MetricState]:
        """
        Return metric scroll states for the task filled with the variant states
        for the variants that reported any debug images
        """
        task, metrics = task_metrics
        must = [{"term": {"task": task}}, {"exists": {"field": "url"}}]
        if metrics:
            must.append({"terms": {"metric": list(metrics)}})
        es_req: dict = {
            "size": 0,
            "query": {"bool": {"must": must}},
            "aggs": {
                "metrics": {
                    "terms": {
                        "field": "metric",
                        "size": EventSettings.max_metrics_count,
                        "order": {"_key": "asc"},
                    },
                    "aggs": {
                        "last_event_timestamp": {"max": {"field": "timestamp"}},
                        "variants": {
                            "terms": {
                                "field": "variant",
                                "size": EventSettings.max_variants_count,
                                "order": {"_key": "asc"},
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
            es_res = search_company_events(
                self.es, company_id=company_id, event_type=self.EVENT_TYPE, body=es_req,
            )
        if "aggregations" not in es_res:
            return []

        def init_variant_state(variant: dict):
            """
            Return new variant state for the passed variant bucket
            If the image urls get recycled then fill the last_invalid_iteration field
            """
            state = VariantState(variant=variant["key"])
            top_iter_url = dpath.get(variant, "urls/buckets")[0]
            iters = dpath.get(top_iter_url, "iters/hits/hits")
            if len(iters) > 1:
                state.last_invalid_iteration = dpath.get(iters[1], "_source/iter")
            return state

        return [
            MetricState(
                metric=metric["key"],
                timestamp=dpath.get(metric, "last_event_timestamp/value"),
                variants=[
                    init_variant_state(variant)
                    for variant in dpath.get(metric, "variants/buckets")
                ],
            )
            for metric in dpath.get(es_res, "aggregations/metrics/buckets")
        ]

    def _get_task_metric_events(
        self,
        task_state: TaskScrollState,
        company_id: str,
        iter_count: int,
        navigate_earlier: bool,
    ) -> Tuple:
        """
        Return task metric events grouped by iterations
        Update task scroll state
        """
        if not task_state.metrics:
            return task_state.task, []

        if task_state.last_max_iter is None:
            # the first fetch is always from the latest iteration to the earlier ones
            navigate_earlier = True

        must_conditions = [
            {"term": {"task": task_state.task}},
            {"terms": {"metric": [m.metric for m in task_state.metrics]}},
            {"exists": {"field": "url"}},
        ]

        range_condition = None
        if navigate_earlier and task_state.last_min_iter is not None:
            range_condition = {"lt": task_state.last_min_iter}
        elif not navigate_earlier and task_state.last_max_iter is not None:
            range_condition = {"gt": task_state.last_max_iter}
        if range_condition:
            must_conditions.append({"range": {"iter": range_condition}})

        es_req = {
            "size": 0,
            "query": {"bool": {"must": must_conditions}},
            "aggs": {
                "iters": {
                    "terms": {
                        "field": "iter",
                        "size": iter_count,
                        "order": {"_key": "desc" if navigate_earlier else "asc"},
                    },
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
                                        "events": {
                                            "top_hits": {
                                                "sort": {"url": {"order": "desc"}}
                                            }
                                        }
                                    },
                                }
                            },
                        }
                    },
                }
            },
        }
        with translate_errors_context(), TimingContext("es", "get_debug_image_events"):
            es_res = search_company_events(
                self.es, company_id=company_id, event_type=self.EVENT_TYPE, body=es_req,
            )
        if "aggregations" not in es_res:
            return task_state.task, []

        invalid_iterations = {
            (m.metric, v.variant): v.last_invalid_iteration
            for m in task_state.metrics
            for v in m.variants
        }

        def is_valid_event(event: dict) -> bool:
            key = event.get("metric"), event.get("variant")
            if key not in invalid_iterations:
                return False

            max_invalid = invalid_iterations[key]
            return max_invalid is None or event.get("iter") > max_invalid

        def get_iteration_events(it_: dict) -> Sequence:
            return [
                ev["_source"]
                for m in dpath.get(it_, "metrics/buckets")
                for v in dpath.get(m, "variants/buckets")
                for ev in dpath.get(v, "events/hits/hits")
                if is_valid_event(ev["_source"])
            ]

        iterations = []
        for it in dpath.get(es_res, "aggregations/iters/buckets"):
            events = get_iteration_events(it)
            if events:
                iterations.append({"iter": it["key"], "events": events})

        if not navigate_earlier:
            iterations.sort(key=itemgetter("iter"), reverse=True)
        if iterations:
            task_state.last_max_iter = iterations[0]["iter"]
            task_state.last_min_iter = iterations[-1]["iter"]

        return task_state.task, iterations
