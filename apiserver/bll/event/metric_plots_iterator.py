from typing import Sequence

from elasticsearch import Elasticsearch
from redis.client import StrictRedis

from .event_common import EventType, uncompress_plot
from .metric_events_iterator import MetricEventsIterator


class MetricPlotsIterator(MetricEventsIterator):
    def __init__(self, redis: StrictRedis, es: Elasticsearch):
        super().__init__(redis, es, EventType.metrics_plot)

    def _get_extra_conditions(self) -> Sequence[dict]:
        return []

    def _get_variant_state_aggs(self):
        return None, None

    def _process_event(self, event: dict) -> dict:
        uncompress_plot(event)
        return event

    def _get_same_variant_events_order(self) -> dict:
        return {"timestamp": {"order": "desc"}}
