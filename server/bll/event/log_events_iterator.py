from typing import Optional, Tuple, Sequence

import attr
from elasticsearch import Elasticsearch
from jsonmodels.fields import StringField, IntField
from jsonmodels.models import Base
from redis import StrictRedis

from apierrors import errors
from apimodels import JsonSerializableMixin
from bll.event.event_metrics import EventMetrics
from bll.redis_cache_manager import RedisCacheManager
from config import config
from database.errors import translate_errors_context
from timing_context import TimingContext


class LogEventsScrollState(Base, JsonSerializableMixin):
    id: str = StringField(required=True)
    task: str = StringField(required=True)
    last_min_timestamp: Optional[int] = IntField()
    last_max_timestamp: Optional[int] = IntField()

    def reset(self):
        """Reset the scrolling state """
        self.last_min_timestamp = self.last_max_timestamp = None


@attr.s(auto_attribs=True)
class TaskEventsResult:
    total_events: int = 0
    next_scroll_id: str = None
    events: list = attr.Factory(list)


class LogEventsIterator:
    EVENT_TYPE = "log"

    @property
    def state_expiration_sec(self):
        return config.get(
            f"services.events.events_retrieval.state_expiration_sec", 3600
        )

    def __init__(self, redis: StrictRedis, es: Elasticsearch):
        self.es = es
        self.cache_manager = RedisCacheManager(
            state_class=LogEventsScrollState,
            redis=redis,
            expiration_interval=self.state_expiration_sec,
        )

    def get_task_events(
        self,
        company_id: str,
        task_id: str,
        batch_size: int,
        navigate_earlier: bool = True,
        refresh: bool = False,
        state_id: str = None,
    ) -> TaskEventsResult:
        es_index = EventMetrics.get_index_name(company_id, self.EVENT_TYPE)
        if not self.es.indices.exists(es_index):
            return TaskEventsResult()

        def init_state(state_: LogEventsScrollState):
            state_.task = task_id

        def validate_state(state_: LogEventsScrollState):
            """
            Checks that the task id stored in the state
            is equal to the one passed with the current call
            Refresh the state if requested
            """
            if state_.task != task_id:
                raise errors.bad_request.InvalidScrollId(
                    "Task stored in the state does not match the passed one",
                    scroll_id=state_.id,
                )
            if refresh:
                state_.reset()

        with self.cache_manager.get_or_create_state(
            state_id=state_id, init_state=init_state, validate_state=validate_state,
        ) as state:
            res = TaskEventsResult(next_scroll_id=state.id)
            res.events, res.total_events = self._get_events(
                es_index=es_index,
                batch_size=batch_size,
                navigate_earlier=navigate_earlier,
                state=state,
            )
            return res

    def _get_events(
        self,
        es_index,
        batch_size: int,
        navigate_earlier: bool,
        state: LogEventsScrollState,
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
            "query": {"term": {"task": state.task}},
            "sort": {"timestamp": "desc" if navigate_earlier else "asc"},
        }

        if navigate_earlier and state.last_min_timestamp is not None:
            es_req["search_after"] = [state.last_min_timestamp]
        elif not navigate_earlier and state.last_max_timestamp is not None:
            es_req["search_after"] = [state.last_max_timestamp]

        with translate_errors_context(), TimingContext("es", "get_task_events"):
            es_result = self.es.search(index=es_index, body=es_req, routing=state.task)
            hits = es_result["hits"]["hits"]
            hits_total = es_result["hits"]["total"]
            if not hits:
                return [], hits_total

            events = [hit["_source"] for hit in hits]
            if navigate_earlier:
                state.last_max_timestamp = events[0]["timestamp"]
                state.last_min_timestamp = events[-1]["timestamp"]
            else:
                state.last_min_timestamp = events[0]["timestamp"]
                state.last_max_timestamp = events[-1]["timestamp"]

            # retrieve the events that match the last event timestamp
            # but did not make it into the previous call due to batch_size limitation
            es_req = {
                "size": 10000,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"task": state.task}},
                            {"term": {"timestamp": events[-1]["timestamp"]}},
                        ]
                    }
                },
            }
            es_result = self.es.search(index=es_index, body=es_req, routing=state.task)
            hits = es_result["hits"]["hits"]
            if not hits or len(hits) < 2:
                # if only one element is returned for the last timestamp
                # then it is already present in the events
                return events, hits_total

            last_events = [hit["_source"] for hit in es_result["hits"]["hits"]]
            already_present_ids = set(ev["_id"] for ev in events)

            # return the list merged from original query results +
            # leftovers from the last timestamp
            return (
                [
                    *events,
                    *(ev for ev in last_events if ev["_id"] not in already_present_ids),
                ],
                hits_total,
            )
