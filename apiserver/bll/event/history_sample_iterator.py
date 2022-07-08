import abc
import operator
from operator import attrgetter
from typing import Sequence, Tuple, Optional, Callable, Mapping

import attr
from boltons.iterutils import first, bucketize
from elasticsearch import Elasticsearch
from jsonmodels.fields import StringField, ListField, IntField, BoolField
from jsonmodels.models import Base
from redis import StrictRedis

from apiserver.apierrors import errors
from apiserver.apimodels import JsonSerializableMixin
from apiserver.bll.event.event_common import (
    EventSettings,
    EventType,
    check_empty_data,
    search_company_events,
    get_max_metric_and_variant_counts,
)
from apiserver.bll.redis_cache_manager import RedisCacheManager
from apiserver.database.errors import translate_errors_context
from apiserver.timing_context import TimingContext
from apiserver.utilities.dicts import nested_get


class VariantState(Base):
    name: str = StringField(required=True)
    metric: str = StringField(default=None)
    min_iteration: int = IntField()
    max_iteration: int = IntField()


class HistorySampleState(Base, JsonSerializableMixin):
    id: str = StringField(required=True)
    iteration: int = IntField()
    variant: str = StringField()
    task: str = StringField()
    metric: str = StringField()
    reached_first: bool = BoolField()
    reached_last: bool = BoolField()
    variant_states: Sequence[VariantState] = ListField([VariantState])
    warning: str = StringField()
    navigate_current_metric = BoolField(default=True)


@attr.s(auto_attribs=True)
class HistorySampleResult(object):
    scroll_id: str = None
    event: dict = None
    min_iteration: int = None
    max_iteration: int = None


class HistorySampleIterator(abc.ABC):
    def __init__(self, redis: StrictRedis, es: Elasticsearch, event_type: EventType):
        self.es = es
        self.event_type = event_type
        self.cache_manager = RedisCacheManager(
            state_class=HistorySampleState,
            redis=redis,
            expiration_interval=EventSettings.state_expiration_sec,
        )

    def get_next_sample(
        self, company_id: str, task: str, state_id: str, navigate_earlier: bool
    ) -> HistorySampleResult:
        """
        Get the sample for next/prev variant on the current iteration
        If does not exist then try getting sample for the first/last variant from next/prev iteration
        """
        res = HistorySampleResult(scroll_id=state_id)
        state = self.cache_manager.get_state(state_id)
        if not state or state.task != task:
            raise errors.bad_request.InvalidScrollId(scroll_id=state_id)

        if check_empty_data(self.es, company_id=company_id, event_type=self.event_type):
            return res

        event = self._get_next_for_current_iteration(
            company_id=company_id, navigate_earlier=navigate_earlier, state=state
        ) or self._get_next_for_another_iteration(
            company_id=company_id, navigate_earlier=navigate_earlier, state=state
        )
        if not event:
            return res

        self._fill_res_and_update_state(event=event, res=res, state=state)
        self.cache_manager.set_state(state=state)
        return res

    def _fill_res_and_update_state(
        self, event: dict, res: HistorySampleResult, state: HistorySampleState
    ):
        self._process_event(event)
        state.variant = event["variant"]
        state.metric = event["metric"]
        state.iteration = event["iter"]
        res.event = event
        var_state = first(
            vs
            for vs in state.variant_states
            if vs.name == state.variant and vs.metric == state.metric
        )
        if var_state:
            res.min_iteration = var_state.min_iteration
            res.max_iteration = var_state.max_iteration

    @abc.abstractmethod
    def _get_extra_conditions(self) -> Sequence[dict]:
        pass

    @abc.abstractmethod
    def _process_event(self, event: dict) -> dict:
        pass

    @abc.abstractmethod
    def _get_variants_conditions(self, variants: Sequence[VariantState]) -> dict:
        pass

    def _get_metric_variants_condition(self, variants: Sequence[VariantState]) -> dict:
        metrics = bucketize(variants, key=attrgetter("metric"))
        metrics_conditions = [
            {
                "bool": {
                    "must": [
                        {"term": {"metric": metric}},
                        self._get_variants_conditions(vs),
                    ]
                }
            }
            for metric, vs in metrics.items()
        ]
        return {"bool": {"should": metrics_conditions}}

    def _get_next_for_current_iteration(
        self, company_id: str, navigate_earlier: bool, state: HistorySampleState
    ) -> Optional[dict]:
        """
        Get the sample for next (if navigate_earlier is False) or previous variant sorted by name for the same iteration
        Only variants for which the iteration falls into their valid range are considered
        Return None if no such variant or sample is found
        """
        if state.navigate_current_metric:
            variants = [
                var_state
                for var_state in state.variant_states
                if var_state.metric == state.metric
            ]
        else:
            variants = state.variant_states

        cmp = operator.lt if navigate_earlier else operator.gt
        variants = [
            var_state
            for var_state in variants
            if cmp((var_state.metric, var_state.name), (state.metric, state.variant))
            and var_state.min_iteration <= state.iteration
        ]
        if not variants:
            return

        must_conditions = [
            {"term": {"task": state.task}},
            {"term": {"iter": state.iteration}},
            self._get_metric_variants_condition(variants),
            *self._get_extra_conditions(),
        ]
        order = "desc" if navigate_earlier else "asc"
        es_req = {
            "size": 1,
            "sort": [{"metric": order}, {"variant": order}],
            "query": {"bool": {"must": must_conditions}},
        }

        with translate_errors_context(), TimingContext(
            "es", "get_next_for_current_iteration"
        ):
            es_res = search_company_events(
                self.es,
                company_id=company_id,
                event_type=self.event_type,
                body=es_req,
                routing=state.task,
            )

        hits = nested_get(es_res, ("hits", "hits"))
        if not hits:
            return

        return hits[0]["_source"]

    def _get_next_for_another_iteration(
        self, company_id: str, navigate_earlier: bool, state: HistorySampleState
    ) -> Optional[dict]:
        """
        Get the sample for the first variant for the next iteration (if navigate_earlier is set to False)
        or from the last variant for the previous iteration (otherwise)
        The variants for which the sample falls in invalid range are discarded
        If no suitable sample is found then None is returned
        """
        if state.navigate_current_metric:
            variants = [
                var_state
                for var_state in state.variant_states
                if var_state.metric == state.metric
            ]
        else:
            variants = state.variant_states

        if navigate_earlier:
            range_operator = "lt"
            order = "desc"
            variants = [
                var_state
                for var_state in variants
                if var_state.min_iteration < state.iteration
            ]
        else:
            range_operator = "gt"
            order = "asc"
            variants = variants

        if not variants:
            return

        must_conditions = [
            {"term": {"task": state.task}},
            self._get_metric_variants_condition(variants),
            {"range": {"iter": {range_operator: state.iteration}}},
            *self._get_extra_conditions(),
        ]
        es_req = {
            "size": 1,
            "sort": [{"iter": order}, {"metric": order}, {"variant": order}],
            "query": {"bool": {"must": must_conditions}},
        }
        with translate_errors_context(), TimingContext(
            "es", "get_next_for_another_iteration"
        ):
            es_res = search_company_events(
                self.es,
                company_id=company_id,
                event_type=self.event_type,
                body=es_req,
                routing=state.task,
            )

        hits = nested_get(es_res, ("hits", "hits"))
        if not hits:
            return

        return hits[0]["_source"]

    def get_sample_for_variant(
        self,
        company_id: str,
        task: str,
        metric: str,
        variant: str,
        iteration: Optional[int] = None,
        refresh: bool = False,
        state_id: str = None,
        navigate_current_metric: bool = True,
    ) -> HistorySampleResult:
        """
        Get the sample for the requested iteration or the latest before it
        If the iteration is not passed then get the latest event
        """
        res = HistorySampleResult()
        if check_empty_data(self.es, company_id=company_id, event_type=self.event_type):
            return res

        def init_state(state_: HistorySampleState):
            state_.task = task
            state_.metric = metric
            state_.navigate_current_metric = navigate_current_metric
            self._reset_variant_states(company_id=company_id, state=state_)

        def validate_state(state_: HistorySampleState):
            if (
                state_.task != task
                or state_.navigate_current_metric != navigate_current_metric
                or (state_.navigate_current_metric and state_.metric != metric)
            ):
                raise errors.bad_request.InvalidScrollId(
                    "Task and metric stored in the state do not match the passed ones",
                    scroll_id=state_.id,
                )
            # fix old variant states:
            for vs in state_.variant_states:
                if vs.metric is None:
                    vs.metric = metric
            if refresh:
                self._reset_variant_states(company_id=company_id, state=state_)

        state: HistorySampleState
        with self.cache_manager.get_or_create_state(
            state_id=state_id, init_state=init_state, validate_state=validate_state,
        ) as state:
            res.scroll_id = state.id

            var_state = first(
                vs
                for vs in state.variant_states
                if vs.name == variant and vs.metric == metric
            )
            if not var_state:
                return res

            res.min_iteration = var_state.min_iteration
            res.max_iteration = var_state.max_iteration

            must_conditions = [
                {"term": {"task": task}},
                {"term": {"metric": metric}},
                {"term": {"variant": variant}},
                *self._get_extra_conditions(),
            ]
            if iteration is not None:
                must_conditions.append(
                    {
                        "range": {
                            "iter": {"lte": iteration, "gte": var_state.min_iteration}
                        }
                    }
                )
            else:
                must_conditions.append(
                    {"range": {"iter": {"gte": var_state.min_iteration}}}
                )

            es_req = {
                "size": 1,
                "sort": {"iter": "desc"},
                "query": {"bool": {"must": must_conditions}},
            }

            with translate_errors_context(), TimingContext(
                "es", "get_history_sample_for_variant"
            ):
                es_res = search_company_events(
                    self.es,
                    company_id=company_id,
                    event_type=self.event_type,
                    body=es_req,
                    routing=task,
                )

            hits = nested_get(es_res, ("hits", "hits"))
            if not hits:
                return res

            self._fill_res_and_update_state(
                event=hits[0]["_source"], res=res, state=state
            )
            return res

    def _reset_variant_states(self, company_id: str, state: HistorySampleState):
        metrics = self._get_metric_variant_iterations(
            company_id=company_id,
            task=state.task,
            metric=state.metric if state.navigate_current_metric else None,
        )
        state.variant_states = [
            VariantState(
                metric=metric,
                name=var_name,
                min_iteration=min_iter,
                max_iteration=max_iter,
            )
            for metric, variants in metrics.items()
            for var_name, min_iter, max_iter in variants
        ]

    @abc.abstractmethod
    def _get_min_max_aggs(self) -> Tuple[dict, Callable[[dict], Tuple[int, int]]]:
        pass

    def _get_metric_variant_iterations(
        self, company_id: str, task: str, metric: str,
    ) -> Mapping[str, Tuple[str, str, int, int]]:
        """
        Return valid min and max iterations that the task reported events of the required type
        """
        must = [
            {"term": {"task": task}},
            *self._get_extra_conditions(),
        ]
        if metric is not None:
            must.append({"term": {"metric": metric}})
        query = {"bool": {"must": must}}

        search_args = dict(
            es=self.es, company_id=company_id, event_type=self.event_type, routing=task,
        )
        max_metrics, max_variants = get_max_metric_and_variant_counts(
            query=query, **search_args
        )
        max_variants = int(max_variants // 2)
        min_max_aggs, get_min_max_data = self._get_min_max_aggs()
        es_req: dict = {
            "size": 0,
            "query": query,
            "aggs": {
                "metrics": {
                    "terms": {
                        "field": "metric",
                        "size": max_metrics,
                        "order": {"_key": "asc"},
                    },
                    "aggs": {
                        "variants": {
                            "terms": {
                                "field": "variant",
                                "size": max_variants,
                                "order": {"_key": "asc"},
                            },
                            "aggs": min_max_aggs,
                        }
                    },
                }
            },
        }

        with translate_errors_context(), TimingContext(
            "es", "get_history_sample_iterations"
        ):
            es_res = search_company_events(body=es_req, **search_args,)

        def get_variant_data(variant_bucket: dict) -> Tuple[str, int, int]:
            variant = variant_bucket["key"]
            min_iter, max_iter = get_min_max_data(variant_bucket)
            return variant, min_iter, max_iter

        return {
            metric_bucket["key"]: [
                get_variant_data(variant_bucket)
                for variant_bucket in nested_get(metric_bucket, ("variants", "buckets"))
            ]
            for metric_bucket in nested_get(
                es_res, ("aggregations", "metrics", "buckets")
            )
        }
