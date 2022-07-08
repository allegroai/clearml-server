from typing import Sequence, Tuple, Callable

from elasticsearch import Elasticsearch
from redis.client import StrictRedis

from apiserver.utilities.dicts import nested_get
from .event_common import EventType
from .history_sample_iterator import HistorySampleIterator, VariantState


class HistoryDebugImageIterator(HistorySampleIterator):
    def __init__(self, redis: StrictRedis, es: Elasticsearch):
        super().__init__(redis, es, EventType.metrics_image)

    def _get_extra_conditions(self) -> Sequence[dict]:
        return [{"exists": {"field": "url"}}]

    def _get_variants_conditions(self, variants: Sequence[VariantState]) -> dict:
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
        return {"bool": {"should": variants_conditions}}

    def _process_event(self, event: dict) -> dict:
        return event

    def _get_min_max_aggs(self) -> Tuple[dict, Callable[[dict], Tuple[int, int]]]:
        # The min iteration is the lowest iteration that contains non-recycled image url
        aggs = {
            "last_iter": {"max": {"field": "iter"}},
            "urls": {
                # group by urls and choose the minimal iteration
                # from all the maximal iterations per url
                "terms": {"field": "url", "order": {"max_iter": "asc"}, "size": 1},
                "aggs": {
                    # find max iteration for each url
                    "max_iter": {"max": {"field": "iter"}}
                },
            },
        }

        def get_min_max_data(variant_bucket: dict) -> Tuple[int, int]:
            urls = nested_get(variant_bucket, ("urls", "buckets"))
            min_iter = int(urls[0]["max_iter"]["value"])
            max_iter = int(variant_bucket["last_iter"]["value"])
            return min_iter, max_iter

        return aggs, get_min_max_data
