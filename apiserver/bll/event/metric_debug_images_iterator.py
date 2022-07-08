from typing import Sequence, Tuple, Callable

from elasticsearch import Elasticsearch
from redis.client import StrictRedis

from apiserver.utilities.dicts import nested_get
from .event_common import EventType
from .metric_events_iterator import MetricEventsIterator, VariantState


class MetricDebugImagesIterator(MetricEventsIterator):
    def __init__(self, redis: StrictRedis, es: Elasticsearch):
        super().__init__(redis, es, EventType.metrics_image)

    def _get_extra_conditions(self) -> Sequence[dict]:
        return [{"exists": {"field": "url"}}]

    def _get_variant_state_aggs(self) -> Tuple[dict, Callable[[dict, VariantState], None]]:
        aggs = {
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
        }

        def fill_variant_state_data(variant_bucket: dict,  state: VariantState):
            """If the image urls get recycled then fill the last_invalid_iteration field"""
            top_iter_url = nested_get(variant_bucket, ("urls", "buckets"))[0]
            iters = nested_get(top_iter_url, ("iters", "hits", "hits"))
            if len(iters) > 1:
                state.last_invalid_iteration = nested_get(iters[1], ("_source", "iter"))

        return aggs, fill_variant_state_data

    def _process_event(self, event: dict) -> dict:
        return event

    def _get_same_variant_events_order(self) -> dict:
        return {"url": {"order": "desc"}}
