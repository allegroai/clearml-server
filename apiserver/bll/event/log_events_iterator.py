from typing import Optional, Tuple, Sequence

import attr
from elasticsearch import Elasticsearch

from bll.event.event_metrics import EventMetrics
from database.errors import translate_errors_context
from timing_context import TimingContext


@attr.s(auto_attribs=True)
class TaskEventsResult:
    total_events: int = 0
    next_scroll_id: str = None
    events: list = attr.Factory(list)


class LogEventsIterator:
    EVENT_TYPE = "log"

    def __init__(self, es: Elasticsearch):
        self.es = es

    def get_task_events(
        self,
        company_id: str,
        task_id: str,
        batch_size: int,
        navigate_earlier: bool = True,
        from_timestamp: Optional[int] = None,
    ) -> TaskEventsResult:
        es_index = EventMetrics.get_index_name(company_id, self.EVENT_TYPE)
        if not self.es.indices.exists(es_index):
            return TaskEventsResult()

        res = TaskEventsResult()
        res.events, res.total_events = self._get_events(
            es_index=es_index,
            task_id=task_id,
            batch_size=batch_size,
            navigate_earlier=navigate_earlier,
            from_timestamp=from_timestamp,
        )
        return res

    def _get_events(
        self,
        es_index,
        task_id: str,
        batch_size: int,
        navigate_earlier: bool,
        from_timestamp: Optional[int],
    ) -> Tuple[Sequence[dict], int]:
        """
        Return up to 'batch size' events starting from the previous timestamp either in the
        direction of earlier events (navigate_earlier=True) or in the direction of later events.
        If last_min_timestamp and last_max_timestamp are not set then start either from latest or earliest.
        For the last timestamp all the events are brought (even if the resulting size
        exceeds batch_size) so that this timestamp events will not be lost between the calls.
        In case any events were received update 'last_min_timestamp' and 'last_max_timestamp'
        """

        # retrieve the next batch of events
        es_req = {
            "size": batch_size,
            "query": {"term": {"task": task_id}},
            "sort": {"timestamp": "desc" if navigate_earlier else "asc"},
        }

        if from_timestamp:
            es_req["search_after"] = [from_timestamp]

        with translate_errors_context(), TimingContext("es", "get_task_events"):
            es_result = self.es.search(index=es_index, body=es_req)
            hits = es_result["hits"]["hits"]
            hits_total = es_result["hits"]["total"]["value"]
            if not hits:
                return [], hits_total

            events = [hit["_source"] for hit in hits]

            # retrieve the events that match the last event timestamp
            # but did not make it into the previous call due to batch_size limitation
            es_req = {
                "size": 10000,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"task": task_id}},
                            {"term": {"timestamp": events[-1]["timestamp"]}},
                        ]
                    }
                },
            }
            es_result = self.es.search(index=es_index, body=es_req)
            last_second_hits = es_result["hits"]["hits"]
            if not last_second_hits or len(last_second_hits) < 2:
                # if only one element is returned for the last timestamp
                # then it is already present in the events
                return events, hits_total

            already_present_ids = set(hit["_id"] for hit in hits)
            last_second_events = [
                hit["_source"]
                for hit in last_second_hits
                if hit["_id"] not in already_present_ids
            ]

            # return the list merged from original query results +
            # leftovers from the last timestamp
            return (
                [*events, *last_second_events],
                hits_total,
            )
