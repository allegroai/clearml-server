import operator
from typing import Sequence, Tuple, Optional

import attr
from boltons.iterutils import first
from elasticsearch import Elasticsearch
from jsonmodels.fields import StringField, ListField, IntField, BoolField
from jsonmodels.models import Base
from redis import StrictRedis

from apiserver.apierrors import errors
from apiserver.apimodels import JsonSerializableMixin
from apiserver.bll.event.event_metrics import EventMetrics
from apiserver.bll.redis_cache_manager import RedisCacheManager
from apiserver.config_repo import config
from apiserver.database.errors import translate_errors_context
from apiserver.timing_context import TimingContext
from apiserver.utilities.dicts import nested_get


class VariantState(Base):
    name: str = StringField(required=True)
    min_iteration: int = IntField()
    max_iteration: int = IntField()


class DebugSampleHistoryState(Base, JsonSerializableMixin):
    id: str = StringField(required=True)
    iteration: int = IntField()
    variant: str = StringField()
    task: str = StringField()
    metric: str = StringField()
    reached_first: bool = BoolField()
    reached_last: bool = BoolField()
    variant_states: Sequence[VariantState] = ListField([VariantState])
    warning: str = StringField()


@attr.s(auto_attribs=True)
class DebugSampleHistoryResult(object):
    scroll_id: str = None
    event: dict = None
    min_iteration: int = None
    max_iteration: int = None


class DebugSampleHistory:
    EVENT_TYPE = "training_debug_image"

    @property
    def state_expiration_sec(self):
        return config.get(
            f"services.events.events_retrieval.state_expiration_sec", 3600
        )

    def __init__(self, redis: StrictRedis, es: Elasticsearch):
        self.es = es
        self.cache_manager = RedisCacheManager(
            state_class=DebugSampleHistoryState,
            redis=redis,
            expiration_interval=self.state_expiration_sec,
        )

    def get_next_debug_image(
        self, company_id: str, task: str, state_id: str, navigate_earlier: bool
    ) -> DebugSampleHistoryResult:
        """
        Get the debug image for next/prev variant on the current iteration
        If does not exist then try getting image for the first/last variant from next/prev iteration
        """
        res = DebugSampleHistoryResult(scroll_id=state_id)
        state = self.cache_manager.get_state(state_id)
        if not state or state.task != task:
            raise errors.bad_request.InvalidScrollId(scroll_id=state_id)

        es_index = EventMetrics.get_index_name(company_id, self.EVENT_TYPE)
        if not self.es.indices.exists(es_index):
            return res

        image = self._get_next_for_current_iteration(
            es_index=es_index, navigate_earlier=navigate_earlier, state=state
        ) or self._get_next_for_another_iteration(
            es_index=es_index, navigate_earlier=navigate_earlier, state=state
        )
        if not image:
            return res

        self._fill_res_and_update_state(image=image, res=res, state=state)
        self.cache_manager.set_state(state=state)
        return res

    def _fill_res_and_update_state(
        self, image: dict, res: DebugSampleHistoryResult, state: DebugSampleHistoryState
    ):
        state.variant = image["variant"]
        state.iteration = image["iter"]
        res.event = image
        var_state = first(s for s in state.variant_states if s.name == state.variant)
        if var_state:
            res.min_iteration = var_state.min_iteration
            res.max_iteration = var_state.max_iteration

    def _get_next_for_current_iteration(
        self, es_index: str, navigate_earlier: bool, state: DebugSampleHistoryState
    ) -> Optional[dict]:
        """
        Get the image for next (if navigated earlier is False) or previous variant sorted by name for the same iteration
        Only variants for which the iteration falls into their valid range are considered
        Return None if no such variant or image is found
        """
        cmp = operator.lt if navigate_earlier else operator.gt
        variants = [
            var_state
            for var_state in state.variant_states
            if cmp(var_state.name, state.variant)
            and var_state.min_iteration <= state.iteration
        ]
        if not variants:
            return

        must_conditions = [
            {"term": {"task": state.task}},
            {"term": {"metric": state.metric}},
            {"terms": {"variant": [v.name for v in variants]}},
            {"term": {"iter": state.iteration}},
            {"exists": {"field": "url"}},
        ]
        es_req = {
            "size": 1,
            "sort": {"variant": "desc" if navigate_earlier else "asc"},
            "query": {"bool": {"must": must_conditions}},
        }

        with translate_errors_context(), TimingContext(
            "es", "get_next_for_current_iteration"
        ):
            es_res = self.es.search(index=es_index, body=es_req, routing=state.task)

        hits = nested_get(es_res, ("hits", "hits"))
        if not hits:
            return

        return hits[0]["_source"]

    def _get_next_for_another_iteration(
        self, es_index: str, navigate_earlier: bool, state: DebugSampleHistoryState
    ) -> Optional[dict]:
        """
        Get the image for the first variant for the next iteration (if navigate_earlier is set to False)
        or from the last variant for the previous iteration (otherwise)
        The variants for which the image falls in invalid range are discarded
        If no suitable image is found then None is returned
        """

        must_conditions = [
            {"term": {"task": state.task}},
            {"term": {"metric": state.metric}},
            {"exists": {"field": "url"}},
        ]

        if navigate_earlier:
            range_operator = "lt"
            order = "desc"
            variants = [
                var_state
                for var_state in state.variant_states
                if var_state.min_iteration < state.iteration
            ]
        else:
            range_operator = "gt"
            order = "asc"
            variants = state.variant_states

        if not variants:
            return

        variants_conditions = [
            {
                "bool": {
                    "must": [
                        {"term": {"variant": v.name}},
                        {"range": {"iter": {"gte": v.min_iteration}}},
                    ]
                }
            }
            for v in variants
        ]
        must_conditions.append({"bool": {"should": variants_conditions}})
        must_conditions.append({"range": {"iter": {range_operator: state.iteration}}},)

        es_req = {
            "size": 1,
            "sort": [{"iter": order}, {"variant": order}],
            "query": {"bool": {"must": must_conditions}},
        }
        with translate_errors_context(), TimingContext(
            "es", "get_next_for_another_iteration"
        ):
            es_res = self.es.search(index=es_index, body=es_req, routing=state.task)

        hits = nested_get(es_res, ("hits", "hits"))
        if not hits:
            return

        return hits[0]["_source"]

    def get_debug_image_for_variant(
        self,
        company_id: str,
        task: str,
        metric: str,
        variant: str,
        iteration: Optional[int] = None,
        state_id: str = None,
    ) -> DebugSampleHistoryResult:
        """
        Get the debug image for the requested iteration or the latest before it
        If the iteration is not passed then get the latest event
        """
        res = DebugSampleHistoryResult()
        es_index = EventMetrics.get_index_name(company_id, self.EVENT_TYPE)
        if not self.es.indices.exists(es_index):
            return res

        def init_state(state_: DebugSampleHistoryState):
            state_.task = task
            state_.metric = metric
            variant_iterations = self._get_variant_iterations(
                es_index=es_index, task=task, metric=metric
            )
            state_.variant_states = [
                VariantState(
                    name=var_name, min_iteration=min_iter, max_iteration=max_iter
                )
                for var_name, min_iter, max_iter in variant_iterations
            ]

        def validate_state(state_: DebugSampleHistoryState):
            if state_.task != task or state_.metric != metric:
                raise errors.bad_request.InvalidScrollId(
                    "Task and metric stored in the state do not match the passed ones",
                    scroll_id=state_.id,
                )

        state: DebugSampleHistoryState
        with self.cache_manager.get_or_create_state(
            state_id=state_id, init_state=init_state, validate_state=validate_state,
        ) as state:
            res.scroll_id = state.id

            var_state = first(s for s in state.variant_states if s.name == variant)
            if not var_state:
                return res

            must_conditions = [
                {"term": {"task": task}},
                {"term": {"metric": metric}},
                {"term": {"variant": variant}},
                {"exists": {"field": "url"}},
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
                "es", "get_debug_image_for_variant"
            ):
                es_res = self.es.search(index=es_index, body=es_req, routing=task)

            hits = nested_get(es_res, ("hits", "hits"))
            if not hits:
                return res

            self._fill_res_and_update_state(
                image=hits[0]["_source"], res=res, state=state
            )
            return res

    def _get_variant_iterations(
        self,
        es_index: str,
        task: str,
        metric: str,
        variants: Optional[Sequence[str]] = None,
    ) -> Sequence[Tuple[str, int, int]]:
        """
        Return valid min and max iterations that the task reported images
        The min iteration is the lowest iteration that contains non-recycled image url
        """
        must = [
            {"term": {"task": task}},
            {"term": {"metric": metric}},
            {"exists": {"field": "url"}},
        ]
        if variants:
            must.append({"terms": {"variant": variants}})

        es_req: dict = {
            "size": 0,
            "query": {"bool": {"must": must}},
            "aggs": {
                "variants": {
                    # all variants that sent debug images
                    "terms": {
                        "field": "variant",
                        "size": EventMetrics.max_variants_count,
                        "order": {"_key": "asc"},
                    },
                    "aggs": {
                        "last_iter": {"max": {"field": "iter"}},
                        "urls": {
                            # group by urls and choose the minimal iteration
                            # from all the maximal iterations per url
                            "terms": {
                                "field": "url",
                                "order": {"max_iter": "asc"},
                                "size": 1,
                            },
                            "aggs": {
                                # find max iteration for each url
                                "max_iter": {"max": {"field": "iter"}}
                            },
                        },
                    },
                }
            },
        }

        with translate_errors_context(), TimingContext(
            "es", "get_debug_image_iterations"
        ):
            es_res = self.es.search(index=es_index, body=es_req, routing=task)

        def get_variant_data(variant_bucket: dict) -> Tuple[str, int, int]:
            variant = variant_bucket["key"]
            urls = nested_get(variant_bucket, ("urls", "buckets"))
            min_iter = int(urls[0]["max_iter"]["value"])
            max_iter = int(variant_bucket["last_iter"]["value"])
            return variant, min_iter, max_iter

        return [
            get_variant_data(variant_bucket)
            for variant_bucket in nested_get(
                es_res, ("aggregations", "variants", "buckets")
            )
        ]
