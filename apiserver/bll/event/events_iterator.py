from typing import Optional, Tuple, Sequence, Any

import attr
import jsonmodels.models
import jwt
from elasticsearch import Elasticsearch
from jwt.algorithms import get_default_algorithms

from apiserver.bll.event.event_common import (
    check_empty_data,
    search_company_events,
    EventType,
    MetricVariants,
    get_metric_variants_condition,
    count_company_events,
)
from apiserver.bll.event.scalar_key import ScalarKeyEnum, ScalarKey
from apiserver.config_repo import config
from apiserver.database.errors import translate_errors_context


@attr.s(auto_attribs=True)
class TaskEventsResult:
    total_events: int = 0
    next_scroll_id: str = None
    events: list = attr.Factory(list)


class EventsIterator:
    def __init__(self, es: Elasticsearch):
        self.es = es

    def get_task_events(
        self,
        event_type: EventType,
        company_id: str,
        task_id: str,
        batch_size: int,
        navigate_earlier: bool = True,
        from_key_value: Optional[Any] = None,
        metric_variants: MetricVariants = None,
        key: ScalarKeyEnum = ScalarKeyEnum.timestamp,
        **kwargs,
    ) -> TaskEventsResult:
        if check_empty_data(self.es, company_id, event_type):
            return TaskEventsResult()

        from_key_value = kwargs.pop("from_timestamp", from_key_value)

        res = TaskEventsResult()
        res.events, res.total_events = self._get_events(
            event_type=event_type,
            company_id=company_id,
            task_id=task_id,
            batch_size=batch_size,
            navigate_earlier=navigate_earlier,
            from_key_value=from_key_value,
            metric_variants=metric_variants,
            key=ScalarKey.resolve(key),
        )
        return res

    def count_task_events(
        self,
        event_type: EventType,
        company_id: str,
        task_id: str,
        metric_variants: MetricVariants = None,
    ) -> int:
        if check_empty_data(self.es, company_id, event_type):
            return 0

        query, _ = self._get_initial_query_and_must(task_id, metric_variants)
        es_req = {
            "query": query,
        }

        with translate_errors_context():
            es_result = count_company_events(
                self.es, company_id=company_id, event_type=event_type, body=es_req,
            )

            return es_result["count"]

    def _get_events(
        self,
        event_type: EventType,
        company_id: str,
        task_id: str,
        batch_size: int,
        navigate_earlier: bool,
        key: ScalarKey,
        from_key_value: Optional[Any],
        metric_variants: MetricVariants = None,
    ) -> Tuple[Sequence[dict], int]:
        """
        Return up to 'batch size' events starting from the previous key-field value (timestamp or iter) either in the
        direction of earlier events (navigate_earlier=True) or in the direction of later events.
        If from_key_field is not set then start either from latest or earliest.
        For the last key-field value all the events are brought (even if the resulting size exceeds batch_size)
        so that events with this value will not be lost between the calls.
        """
        query, must = self._get_initial_query_and_must(task_id, metric_variants)

        # retrieve the next batch of events
        es_req = {
            "size": batch_size,
            "query": query,
            "sort": {key.field: "desc" if navigate_earlier else "asc"},
        }

        if from_key_value:
            es_req["search_after"] = [from_key_value]

        with translate_errors_context():
            es_result = search_company_events(
                self.es, company_id=company_id, event_type=event_type, body=es_req,
            )
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
                        "must": must + [{"term": {key.field: events[-1][key.field]}}]
                    }
                },
            }
            es_result = search_company_events(
                self.es, company_id=company_id, event_type=event_type, body=es_req,
            )
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

    @staticmethod
    def _get_initial_query_and_must(
        task_id: str, metric_variants: MetricVariants = None
    ) -> Tuple[dict, list]:
        if not metric_variants:
            must = [{"term": {"task": task_id}}]
            query = {"term": {"task": task_id}}
        else:
            must = [
                {"term": {"task": task_id}},
                get_metric_variants_condition(metric_variants),
            ]
            query = {"bool": {"must": must}}
        return query, must


class Scroll(jsonmodels.models.Base):
    def get_scroll_id(self) -> str:
        return jwt.encode(
            self.to_struct(),
            key=config.get(
                "services.events.events_retrieval.scroll_id_key", "1234567890"
            ),
        )

    @classmethod
    def from_scroll_id(cls, scroll_id: str):
        try:
            return cls(
                **jwt.decode(
                    scroll_id,
                    key=config.get(
                        "services.events.events_retrieval.scroll_id_key", "1234567890"
                    ),
                    algorithms=get_default_algorithms(),
                )
            )
        except jwt.PyJWTError:
            raise ValueError("Invalid Scroll ID")
