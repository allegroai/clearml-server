from typing import Sequence, Tuple, Callable

from elasticsearch import Elasticsearch
from redis.client import StrictRedis

from .event_common import EventType, uncompress_plot
from .history_sample_iterator import HistorySampleIterator, VariantState


class HistoryPlotIterator(HistorySampleIterator):
    def __init__(self, redis: StrictRedis, es: Elasticsearch):
        super().__init__(redis, es, EventType.metrics_plot)

    def _get_extra_conditions(self) -> Sequence[dict]:
        return []

    def _get_variants_conditions(self, variants: Sequence[VariantState]) -> dict:
        return {"terms": {"variant": [v.name for v in variants]}}

    def _process_event(self, event: dict) -> dict:
        uncompress_plot(event)
        return event

    def _get_min_max_aggs(self) -> Tuple[dict, Callable[[dict], Tuple[int, int]]]:
        # The min iteration is the lowest iteration that contains non-recycled image url
        aggs = {
            "last_iter": {"max": {"field": "iter"}},
            "first_iter": {"min": {"field": "iter"}},
        }

        def get_min_max_data(variant_bucket: dict) -> Tuple[int, int]:
            min_iter = int(variant_bucket["first_iter"]["value"])
            max_iter = int(variant_bucket["last_iter"]["value"])
            return min_iter, max_iter

        return aggs, get_min_max_data
