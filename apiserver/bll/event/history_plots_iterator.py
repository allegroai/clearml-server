from typing import Sequence, Tuple, Optional, Mapping

import attr
from boltons.iterutils import first
from elasticsearch import Elasticsearch
from jsonmodels.fields import StringField, IntField, ListField, BoolField
from jsonmodels.models import Base
from redis.client import StrictRedis

from .event_common import (
    EventType,
    uncompress_plot,
    EventSettings,
    check_empty_data,
    search_company_events,
)
from apiserver.apimodels import JsonSerializableMixin
from apiserver.utilities.dicts import nested_get
from apiserver.bll.redis_cache_manager import RedisCacheManager
from apiserver.apierrors import errors


class MetricState(Base):
    name: str = StringField(default=None)
    min_iteration: int = IntField()
    max_iteration: int = IntField()


class PlotsSampleState(Base, JsonSerializableMixin):
    id: str = StringField(required=True)
    iteration: int = IntField()
    task: str = StringField()
    metric: str = StringField()
    metric_states: Sequence[MetricState] = ListField([MetricState])
    warning: str = StringField()
    navigate_current_metric = BoolField(default=True)


@attr.s(auto_attribs=True)
class MetricSamplesResult(object):
    scroll_id: str = None
    events: list = []
    min_iteration: int = None
    max_iteration: int = None


class HistoryPlotsIterator:
    event_type = EventType.metrics_plot

    def __init__(self, redis: StrictRedis, es: Elasticsearch):
        self.es = es
        self.cache_manager = RedisCacheManager(
            state_class=PlotsSampleState,
            redis=redis,
            expiration_interval=EventSettings.state_expiration_sec,
        )

    def get_next_sample(
        self,
        company_id: str,
        task: str,
        state_id: str,
        navigate_earlier: bool,
        next_iteration: bool,
    ) -> MetricSamplesResult:
        """
        Get the samples for next/prev metric on the current iteration
        If does not exist then try getting sample for the first/last metric from next/prev iteration
        """
        res = MetricSamplesResult(scroll_id=state_id)
        state = self.cache_manager.get_state(state_id)
        if not state or state.task != task:
            raise errors.bad_request.InvalidScrollId(scroll_id=state_id)

        if check_empty_data(self.es, company_id=company_id, event_type=self.event_type):
            return res

        if navigate_earlier:
            range_operator = "lt"
            order = "desc"
        else:
            range_operator = "gt"
            order = "asc"

        must_conditions = [
            {"term": {"task": state.task}},
        ]
        if state.navigate_current_metric:
            must_conditions.append({"term": {"metric": state.metric}})

        next_iteration_condition = {
            "range": {"iter": {range_operator: state.iteration}}
        }
        if next_iteration or state.navigate_current_metric:
            must_conditions.append(next_iteration_condition)
        else:
            next_metric_condition = {
                "bool": {
                    "must": [
                        {"term": {"iter": state.iteration}},
                        {"range": {"metric": {range_operator: state.metric}}},
                    ]
                }
            }
            must_conditions.append(
                {"bool": {"should": [next_metric_condition, next_iteration_condition]}}
            )

        events = self._get_metric_events_for_condition(
            company_id=company_id,
            task=state.task,
            order=order,
            must_conditions=must_conditions,
        )

        if not events:
            return res

        self._fill_res_and_update_state(events=events, res=res, state=state)
        self.cache_manager.set_state(state=state)
        return res

    def get_samples_for_metric(
        self,
        company_id: str,
        task: str,
        metric: str,
        iteration: Optional[int] = None,
        refresh: bool = False,
        state_id: str = None,
        navigate_current_metric: bool = True,
    ) -> MetricSamplesResult:
        """
        Get the sample for the requested iteration or the latest before it
        If the iteration is not passed then get the latest event
        """
        res = MetricSamplesResult()
        if check_empty_data(self.es, company_id=company_id, event_type=self.event_type):
            return res

        def init_state(state_: PlotsSampleState):
            state_.task = task
            state_.metric = metric
            state_.navigate_current_metric = navigate_current_metric
            self._reset_metric_states(company_id=company_id, state=state_)

        def validate_state(state_: PlotsSampleState):
            if (
                state_.task != task
                or state_.navigate_current_metric != navigate_current_metric
                or (state_.navigate_current_metric and state_.metric != metric)
            ):
                raise errors.bad_request.InvalidScrollId(
                    "Task and metric stored in the state do not match the passed ones",
                    scroll_id=state_.id,
                )
            if refresh:
                self._reset_metric_states(company_id=company_id, state=state_)

        state: PlotsSampleState
        with self.cache_manager.get_or_create_state(
            state_id=state_id, init_state=init_state, validate_state=validate_state,
        ) as state:
            res.scroll_id = state.id

            metric_state = first(ms for ms in state.metric_states if ms.name == metric)
            if not metric_state:
                return res

            res.min_iteration = metric_state.min_iteration
            res.max_iteration = metric_state.max_iteration

            must_conditions = [
                {"term": {"task": task}},
                {"term": {"metric": metric}},
            ]
            if iteration is not None:
                must_conditions.append({"range": {"iter": {"lte": iteration}}})

            events = self._get_metric_events_for_condition(
                company_id=company_id,
                task=state.task,
                order="desc",
                must_conditions=must_conditions,
            )
            if not events:
                return res

            self._fill_res_and_update_state(events=events, res=res, state=state)
            return res

    def _reset_metric_states(self, company_id: str, state: PlotsSampleState):
        metrics = self._get_metric_iterations(
            company_id=company_id,
            task=state.task,
            metric=state.metric if state.navigate_current_metric else None,
        )
        state.metric_states = [
            MetricState(name=metric, min_iteration=min_iter, max_iteration=max_iter)
            for metric, (min_iter, max_iter) in metrics.items()
        ]

    def _get_metric_iterations(
        self, company_id: str, task: str, metric: str,
    ) -> Mapping[str, Tuple[int, int]]:
        """
        Return valid min and max iterations that the task reported events of the required type
        """
        must = [
            {"term": {"task": task}},
        ]
        if metric is not None:
            must.append({"term": {"metric": metric}})
        query = {"bool": {"must": must}}

        es_req: dict = {
            "size": 0,
            "query": query,
            "aggs": {
                "metrics": {
                    "terms": {
                        "field": "metric",
                        "size": 5000,
                        "order": {"_key": "asc"},
                    },
                    "aggs": {
                        "last_iter": {"max": {"field": "iter"}},
                        "first_iter": {"min": {"field": "iter"}},
                    },
                }
            },
        }

        es_res = search_company_events(
            body=es_req,
            es=self.es,
            company_id=company_id,
            event_type=self.event_type,
        )

        return {
            metric_bucket["key"]: (
                int(metric_bucket["first_iter"]["value"]),
                int(metric_bucket["last_iter"]["value"]),
            )
            for metric_bucket in nested_get(
                es_res, ("aggregations", "metrics", "buckets")
            )
        }

    @staticmethod
    def _fill_res_and_update_state(
        events: Sequence[dict], res: MetricSamplesResult, state: PlotsSampleState
    ):
        for event in events:
            uncompress_plot(event)
        state.metric = events[0]["metric"]
        state.iteration = events[0]["iter"]
        res.events = events
        metric_state = first(
            ms for ms in state.metric_states if ms.name == state.metric
        )
        if metric_state:
            res.min_iteration = metric_state.min_iteration
            res.max_iteration = metric_state.max_iteration

    def _get_metric_events_for_condition(
        self, company_id: str, task: str, order: str, must_conditions: Sequence
    ) -> Sequence:
        es_req = {
            "size": 0,
            "query": {"bool": {"must": must_conditions}},
            "aggs": {
                "iters": {
                    "terms": {"field": "iter", "size": 1, "order": {"_key": order}},
                    "aggs": {
                        "metrics": {
                            "terms": {
                                "field": "metric",
                                "size": 1,
                                "order": {"_key": order},
                            },
                            "aggs": {
                                "events": {
                                    "top_hits": {
                                        "sort": {"variant": {"order": "asc"}},
                                        "size": 100,
                                    }
                                }
                            },
                        },
                    },
                }
            },
        }
        es_res = search_company_events(
            self.es,
            company_id=company_id,
            event_type=self.event_type,
            body=es_req,
        )

        aggs_result = es_res.get("aggregations")
        if not aggs_result:
            return []

        for level in ("iters", "metrics"):
            level_data = aggs_result[level]["buckets"]
            if not level_data:
                return []
            aggs_result = level_data[0]

        return [
            hit["_source"]
            for hit in nested_get(aggs_result, ("events", "hits", "hits"))
        ]
