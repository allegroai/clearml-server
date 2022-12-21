import base64
import hashlib
import re
import zlib
from collections import defaultdict
from contextlib import closing
from datetime import datetime
from operator import attrgetter
from typing import Sequence, Set, Tuple, Optional, List, Mapping, Union

import elasticsearch
from elasticsearch.helpers import BulkIndexError
from mongoengine import Q
from nested_dict import nested_dict

from apiserver.bll.event.event_common import (
    EventType,
    get_index_name,
    check_empty_data,
    search_company_events,
    delete_company_events,
    MetricVariants,
    get_metric_variants_condition,
    uncompress_plot,
    get_max_metric_and_variant_counts,
)
from apiserver.bll.event.events_iterator import EventsIterator, TaskEventsResult
from apiserver.bll.event.history_debug_image_iterator import HistoryDebugImageIterator
from apiserver.bll.event.history_plots_iterator import HistoryPlotsIterator
from apiserver.bll.event.metric_debug_images_iterator import MetricDebugImagesIterator
from apiserver.bll.event.metric_plots_iterator import MetricPlotsIterator
from apiserver.bll.util import parallel_chunked_decorator
from apiserver.database import utils as dbutils
from apiserver.database.model.model import Model
from apiserver.es_factory import es_factory
from apiserver.apierrors import errors
from apiserver.bll.event.event_metrics import EventMetrics
from apiserver.bll.task import TaskBLL
from apiserver.config_repo import config
from apiserver.database.errors import translate_errors_context
from apiserver.database.model.task.task import Task, TaskStatus
from apiserver.redis_manager import redman
from apiserver.tools import safe_get
from apiserver.utilities.dicts import nested_get
from apiserver.utilities.json import loads

# noinspection PyTypeChecker
EVENT_TYPES: Set[str] = set(map(attrgetter("value"), EventType))
LOCKED_TASK_STATUSES = (TaskStatus.publishing, TaskStatus.published)
MAX_LONG = 2 ** 63 - 1
MIN_LONG = -(2 ** 63)


log = config.logger(__file__)


class PlotFields:
    valid_plot = "valid_plot"
    plot_len = "plot_len"
    plot_str = "plot_str"
    plot_data = "plot_data"
    source_urls = "source_urls"


class EventBLL(object):
    id_fields = ("task", "iter", "metric", "variant", "key")
    empty_scroll = "FFFF"
    img_source_regex = re.compile(
        r"['\"]source['\"]:\s?['\"]([a-z][a-z0-9+\-.]*://.*?)['\"]",
        flags=re.IGNORECASE,
    )
    _task_event_query = {
        "bool": {
            "should": [
                {"term": {"model_event": False}},
                {"bool": {"must_not": [{"exists": {"field": "model_event"}}]}},
            ]
        }
    }
    _model_event_query = {"term": {"model_event": True}}

    def __init__(self, events_es=None, redis=None):
        self.es = events_es or es_factory.connect("events")
        self.redis = redis or redman.connection("apiserver")
        self._metrics = EventMetrics(self.es)
        self._skip_iteration_for_metric = set(
            config.get("services.events.ignore_iteration.metrics", [])
        )
        self.debug_images_iterator = MetricDebugImagesIterator(
            es=self.es, redis=self.redis
        )
        self.debug_image_sample_history = HistoryDebugImageIterator(
            es=self.es, redis=self.redis
        )
        self.plots_iterator = MetricPlotsIterator(es=self.es, redis=self.redis)
        self.plot_sample_history = HistoryPlotsIterator(es=self.es, redis=self.redis)
        self.events_iterator = EventsIterator(es=self.es)

    @property
    def metrics(self) -> EventMetrics:
        return self._metrics

    @staticmethod
    def _get_valid_tasks(company_id, task_ids: Set, allow_locked_tasks=False) -> Set:
        """Verify that task exists and can be updated"""
        if not task_ids:
            return set()

        with translate_errors_context():
            query = Q(id__in=task_ids, company=company_id)
            if not allow_locked_tasks:
                query &= Q(status__nin=LOCKED_TASK_STATUSES)
            res = Task.objects(query).only("id")
            return {r.id for r in res}

    @staticmethod
    def _get_valid_models(company_id, model_ids: Set, allow_locked_models=False) -> Set:
        """Verify that task exists and can be updated"""
        if not model_ids:
            return set()

        with translate_errors_context():
            query = Q(id__in=model_ids, company=company_id)
            if not allow_locked_models:
                query &= Q(ready__ne=True)
            res = Model.objects(query).only("id")
            return {r.id for r in res}

    def add_events(
        self, company_id, events, worker, allow_locked=False
    ) -> Tuple[int, int, dict]:
        model_events = events[0].get("model_event", False)
        for event in events:
            if event.get("model_event", model_events) != model_events:
                raise errors.bad_request.ValidationError(
                    "Inconsistent model_event setting in the passed events"
                )
            if event.pop("allow_locked", allow_locked) != allow_locked:
                raise errors.bad_request.ValidationError(
                    "Inconsistent allow_locked setting in the passed events"
                )

        actions: List[dict] = []
        task_or_model_ids = set()
        task_iteration = defaultdict(lambda: 0)
        task_last_scalar_events = nested_dict(
            3, dict
        )  # task_id -> metric_hash -> variant_hash -> MetricEvent
        task_last_events = nested_dict(
            3, dict
        )  # task_id -> metric_hash -> event_type -> MetricEvent
        errors_per_type = defaultdict(int)
        invalid_iteration_error = f"Iteration number should not exceed {MAX_LONG}"
        if model_events:
            for event in events:
                model = event.pop("model", None)
                if model is not None:
                    event["task"] = model
            valid_entities = self._get_valid_models(
                company_id,
                model_ids={
                    event["task"] for event in events if event.get("task") is not None
                },
                allow_locked_models=allow_locked,
            )
            entity_name = "model"
        else:
            valid_entities = self._get_valid_tasks(
                company_id,
                task_ids={
                    event["task"] for event in events if event.get("task") is not None
                },
                allow_locked_tasks=allow_locked,
            )
            entity_name = "task"

        for event in events:
            # remove spaces from event type
            event_type = event.get("type")
            if event_type is None:
                errors_per_type["Event must have a 'type' field"] += 1
                continue

            event_type = event_type.replace(" ", "_")
            if event_type not in EVENT_TYPES:
                errors_per_type[f"Invalid event type {event_type}"] += 1
                continue

            if model_events and event_type == EventType.task_log.value:
                errors_per_type[f"Task log events are not supported for models"] += 1
                continue

            task_or_model_id = event.get("task")
            if task_or_model_id is None:
                errors_per_type["Event must have a 'task' field"] += 1
                continue

            if task_or_model_id not in valid_entities:
                errors_per_type[f"Invalid {entity_name} id {task_or_model_id}"] += 1
                continue

            event["type"] = event_type

            # @timestamp indicates the time the event is written, not when it happened
            event["@timestamp"] = es_factory.get_es_timestamp_str()

            # for backward bomba-tavili-tea
            if "ts" in event:
                event["timestamp"] = event.pop("ts")

            # set timestamp and worker if not sent
            if "timestamp" not in event:
                event["timestamp"] = es_factory.get_timestamp_millis()

            if "worker" not in event:
                event["worker"] = worker

            # force iter to be a long int
            iter = event.get("iter")
            if iter is not None:
                if model_events:
                    iter = 0
                else:
                    iter = int(iter)
                    if iter > MAX_LONG or iter < MIN_LONG:
                        errors_per_type[invalid_iteration_error] += 1
                        continue
                event["iter"] = iter

            # used to have "values" to indicate array. no need anymore
            if "values" in event:
                event["value"] = event["values"]
                del event["values"]

            event["metric"] = event.get("metric") or ""
            event["variant"] = event.get("variant") or ""
            event["model_event"] = model_events

            index_name = get_index_name(company_id, event_type)
            es_action = {
                "_op_type": "index",  # overwrite if exists with same ID
                "_index": index_name,
                "_source": event,
            }

            # for "log" events, don't assing custom _id - whatever is sent, is written (not overwritten)
            if event_type != EventType.task_log.value:
                es_action["_id"] = self._get_event_id(event)
            else:
                es_action["_id"] = dbutils.id()

            task_or_model_ids.add(task_or_model_id)
            if (
                iter is not None
                and not model_events
                and event.get("metric") not in self._skip_iteration_for_metric
            ):
                task_iteration[task_or_model_id] = max(
                    iter, task_iteration[task_or_model_id]
                )

            if not model_events:
                self._update_last_metric_events_for_task(
                    last_events=task_last_events[task_or_model_id], event=event,
                )
                if event_type == EventType.metrics_scalar.value:
                    self._update_last_scalar_events_for_task(
                        last_events=task_last_scalar_events[task_or_model_id],
                        event=event,
                    )

            actions.append(es_action)

        plot_actions = [
            action["_source"]
            for action in actions
            if action["_source"]["type"] == EventType.metrics_plot.value
        ]
        if plot_actions:
            self.validate_and_compress_plots(
                plot_actions,
                validate_json=config.get("services.events.validate_plot_str", False),
                compression_threshold=config.get(
                    "services.events.plot_compression_threshold", 100_000
                ),
            )

        added = 0
        with translate_errors_context():
            if actions:
                chunk_size = 500
                # TODO: replace it with helpers.parallel_bulk in the future once the parallel pool leak is fixed
                with closing(
                    elasticsearch.helpers.streaming_bulk(
                        self.es,
                        actions,
                        chunk_size=chunk_size,
                        # thread_count=8,
                        refresh=True,
                    )
                ) as it:
                    for success, info in it:
                        if success:
                            added += 1
                        else:
                            errors_per_type["Error when indexing events batch"] += 1

                if not model_events:
                    remaining_tasks = set()
                    now = datetime.utcnow()
                    for task_or_model_id in task_or_model_ids:
                        # Update related tasks. For reasons of performance, we prefer to update
                        # all of them and not only those who's events were successful
                        updated = self._update_task(
                            company_id=company_id,
                            task_id=task_or_model_id,
                            now=now,
                            iter_max=task_iteration.get(task_or_model_id),
                            last_scalar_events=task_last_scalar_events.get(
                                task_or_model_id
                            ),
                            last_events=task_last_events.get(task_or_model_id),
                        )

                        if not updated:
                            remaining_tasks.add(task_or_model_id)
                            continue

                    if remaining_tasks:
                        TaskBLL.set_last_update(
                            remaining_tasks, company_id, last_update=now
                        )

            # this is for backwards compatibility with streaming bulk throwing exception on those
            invalid_iterations_count = errors_per_type.get(invalid_iteration_error)
            if invalid_iterations_count:
                raise BulkIndexError(
                    f"{invalid_iterations_count} document(s) failed to index.",
                    [invalid_iteration_error],
                )

        if not added:
            raise errors.bad_request.EventsNotAdded(**errors_per_type)

        errors_count = sum(errors_per_type.values())
        return added, errors_count, errors_per_type

    @parallel_chunked_decorator(chunk_size=10)
    def validate_and_compress_plots(
        self,
        plot_events: Sequence[dict],
        validate_json: bool,
        compression_threshold: int,
    ):
        for event in plot_events:
            validate = validate_json and not event.pop("skip_validation", False)
            plot_str = event.get(PlotFields.plot_str)
            if not plot_str:
                event[PlotFields.plot_len] = 0
                if validate:
                    event[PlotFields.valid_plot] = False
                continue

            plot_len = len(plot_str)
            event[PlotFields.plot_len] = plot_len
            if validate:
                event[PlotFields.valid_plot] = self._is_valid_json(plot_str)

            urls = {match for match in self.img_source_regex.findall(plot_str)}
            if urls:
                event[PlotFields.source_urls] = list(urls)

            if compression_threshold and plot_len >= compression_threshold:
                event[PlotFields.plot_data] = base64.encodebytes(
                    zlib.compress(plot_str.encode(), level=1)
                ).decode("ascii")
                event.pop(PlotFields.plot_str, None)

    @parallel_chunked_decorator(chunk_size=10)
    def uncompress_plots(self, plot_events: Sequence[dict]):
        for event in plot_events:
            uncompress_plot(event)

    @staticmethod
    def _is_valid_json(text: str) -> bool:
        """Check str for valid json"""
        if not text:
            return False
        try:
            loads(text)
        except Exception:
            return False
        return True

    def _update_last_scalar_events_for_task(self, last_events, event):
        """
        Update last_events structure with the provided event details if this event is more
        recent than the currently stored event for its metric/variant combination.

        last_events contains [hashed_metric_name -> hashed_variant_name -> event]. Keys are hashed to avoid mongodb
        key conflicts due to invalid characters and/or long field names.
        """
        metric = event.get("metric")
        variant = event.get("variant")
        if not (metric and variant):
            return

        metric_hash = dbutils.hash_field_name(metric)
        variant_hash = dbutils.hash_field_name(variant)

        last_event = last_events[metric_hash][variant_hash]
        event_iter = event.get("iter", 0)
        event_timestamp = event.get("timestamp", 0)
        value = event.get("value")
        if value is not None and (
            (event_iter, event_timestamp)
            >= (
                last_event.get("iter", event_iter),
                last_event.get("timestamp", event_timestamp),
            )
        ):
            event_data = {
                k: event[k]
                for k in ("value", "metric", "variant", "iter", "timestamp")
                if k in event
            }
            last_event_min_value = last_event.get("min_value", value)
            last_event_min_value_iter = last_event.get("min_value_iter", event_iter)
            if value < last_event_min_value:
                event_data["min_value"] = value
                event_data["min_value_iter"] = event_iter
            else:
                event_data["min_value"] = last_event_min_value
                event_data["min_value_iter"] = last_event_min_value_iter
            last_event_max_value = last_event.get("max_value", value)
            last_event_max_value_iter = last_event.get("max_value_iter", event_iter)
            if value > last_event_max_value:
                event_data["max_value"] = value
                event_data["max_value_iter"] = event_iter
            else:
                event_data["max_value"] = last_event_max_value
                event_data["max_value_iter"] = last_event_max_value_iter
            last_events[metric_hash][variant_hash] = event_data

    def _update_last_metric_events_for_task(self, last_events, event):
        """
        Update last_events structure with the provided event details if this event is more
        recent than the currently stored event for its metric/event_type combination.
        last_events contains [metric_name -> event_type -> event]
        """
        metric = event.get("metric")
        event_type = event.get("type")
        if not (metric and event_type):
            return

        timestamp = last_events[metric][event_type].get("timestamp", None)
        if timestamp is None or timestamp < event["timestamp"]:
            last_events[metric][event_type] = event

    def _update_task(
        self,
        company_id,
        task_id,
        now,
        iter_max=None,
        last_scalar_events=None,
        last_events=None,
    ):
        """
        Update task information in DB with aggregated results after handling event(s) related to this task.

        This updates the task with the highest iteration value encountered during the last events update, as well
        as the latest metric/variant scalar values reported (according to the report timestamp) and the task's last
        update time.
        """
        if iter_max is None and not last_events and not last_scalar_events:
            return False

        return TaskBLL.update_statistics(
            task_id,
            company_id,
            last_update=now,
            last_iteration_max=iter_max,
            last_scalar_events=last_scalar_events,
            last_events=last_events,
        )

    def _get_event_id(self, event):
        id_values = (str(event[field]) for field in self.id_fields if field in event)
        return hashlib.md5("-".join(id_values).encode()).hexdigest()

    def scroll_task_events(
        self,
        company_id: str,
        task_id: str,
        order: str,
        event_type: EventType,
        batch_size=10000,
        scroll_id=None,
    ):
        if scroll_id == self.empty_scroll:
            return [], scroll_id, 0

        if scroll_id:
            with translate_errors_context():
                es_res = self.es.scroll(scroll_id=scroll_id, scroll="1h")
        else:
            size = min(batch_size, 10000)
            if check_empty_data(self.es, company_id=company_id, event_type=event_type):
                return [], None, 0

            es_req = {
                "size": size,
                "sort": {"timestamp": {"order": order}},
                "query": {"bool": {"must": [{"term": {"task": task_id}}]}},
            }

            with translate_errors_context():
                es_res = search_company_events(
                    self.es,
                    company_id=company_id,
                    event_type=event_type,
                    body=es_req,
                    scroll="1h",
                )

        events, total_events, next_scroll_id = self._get_events_from_es_res(es_res)
        if event_type in (EventType.metrics_plot, EventType.all):
            self.uncompress_plots(events)

        return events, next_scroll_id, total_events

    def get_last_iterations_per_event_metric_variant(
        self,
        company_id: str,
        task_id: str,
        num_last_iterations: int,
        event_type: EventType,
        metric_variants: MetricVariants = None,
    ):
        if check_empty_data(self.es, company_id=company_id, event_type=event_type):
            return []

        must = [{"term": {"task": task_id}}]
        if metric_variants:
            must.append(get_metric_variants_condition(metric_variants))
        query = {"bool": {"must": must}}
        search_args = dict(es=self.es, company_id=company_id, event_type=event_type)
        max_metrics, max_variants = get_max_metric_and_variant_counts(
            query=query, **search_args,
        )
        max_variants = int(max_variants // num_last_iterations)

        es_req: dict = {
            "size": 0,
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
                            "aggs": {
                                "iters": {
                                    "terms": {
                                        "field": "iter",
                                        "size": num_last_iterations,
                                        "order": {"_key": "desc"},
                                    }
                                }
                            },
                        }
                    },
                }
            },
            "query": query,
        }

        with translate_errors_context():
            es_res = search_company_events(body=es_req, **search_args)

        if "aggregations" not in es_res:
            return []

        return [
            (metric["key"], variant["key"], iter["key"])
            for metric in es_res["aggregations"]["metrics"]["buckets"]
            for variant in metric["variants"]["buckets"]
            for iter in variant["iters"]["buckets"]
        ]

    def get_task_plots(
        self,
        company_id: str,
        tasks: Sequence[str],
        last_iterations_per_plot: int = None,
        sort=None,
        size: int = 500,
        scroll_id: str = None,
        no_scroll: bool = False,
        metric_variants: MetricVariants = None,
        model_events: bool = False,
    ):
        if scroll_id == self.empty_scroll:
            return TaskEventsResult()

        if scroll_id:
            with translate_errors_context():
                es_res = self.es.scroll(scroll_id=scroll_id, scroll="1h")
        else:
            event_type = EventType.metrics_plot
            if check_empty_data(self.es, company_id=company_id, event_type=event_type):
                return TaskEventsResult()

            plot_valid_condition = {
                "bool": {
                    "should": [
                        {"term": {PlotFields.valid_plot: True}},
                        {
                            "bool": {
                                "must_not": {"exists": {"field": PlotFields.valid_plot}}
                            }
                        },
                    ]
                }
            }
            must = [plot_valid_condition]

            if last_iterations_per_plot is None or model_events:
                must.append({"terms": {"task": tasks}})
                if metric_variants:
                    must.append(get_metric_variants_condition(metric_variants))
            else:
                should = []
                for i, task_id in enumerate(tasks):
                    last_iters = self.get_last_iterations_per_event_metric_variant(
                        company_id=company_id,
                        task_id=task_id,
                        num_last_iterations=last_iterations_per_plot,
                        event_type=event_type,
                        metric_variants=metric_variants,
                    )
                    if not last_iters:
                        continue

                    for metric, variant, iter in last_iters:
                        should.append(
                            {
                                "bool": {
                                    "must": [
                                        {"term": {"task": task_id}},
                                        {"term": {"metric": metric}},
                                        {"term": {"variant": variant}},
                                        {"term": {"iter": iter}},
                                    ]
                                }
                            }
                        )
                if not should:
                    return TaskEventsResult()
                must.append({"bool": {"should": should}})

            if sort is None:
                sort = [{"timestamp": {"order": "asc"}}]

            es_req = {
                "sort": sort,
                "size": min(size, 10000),
                "query": {"bool": {"must": must}},
            }

            with translate_errors_context():
                es_res = search_company_events(
                    self.es,
                    company_id=company_id,
                    event_type=event_type,
                    body=es_req,
                    ignore=404,
                    **({} if no_scroll else {"scroll": "1h"}),
                )

        events, total_events, next_scroll_id = self._get_events_from_es_res(es_res)
        self.uncompress_plots(events)
        return TaskEventsResult(
            events=events, next_scroll_id=next_scroll_id, total_events=total_events
        )

    def _get_events_from_es_res(self, es_res: dict) -> Tuple[list, int, Optional[str]]:
        """
        Return events and next scroll id from the scrolled query
        Release the scroll once it is exhausted
        """
        total_events = safe_get(es_res, "hits/total/value", default=0)
        events = [doc["_source"] for doc in safe_get(es_res, "hits/hits", default=[])]
        next_scroll_id = es_res.get("_scroll_id")
        if next_scroll_id and not events:
            self.clear_scroll(next_scroll_id)
            next_scroll_id = self.empty_scroll

        return events, total_events, next_scroll_id

    def get_debug_image_urls(
        self, company_id: str, task_id: str, after_key: dict = None
    ) -> Tuple[Sequence[str], Optional[dict]]:
        if check_empty_data(self.es, company_id, EventType.metrics_image):
            return [], None

        es_req = {
            "size": 0,
            "aggs": {
                "debug_images": {
                    "composite": {
                        "size": 1000,
                        **({"after": after_key} if after_key else {}),
                        "sources": [{"url": {"terms": {"field": "url"}}}],
                    }
                }
            },
            "query": {
                "bool": {
                    "must": [{"term": {"task": task_id}}, {"exists": {"field": "url"}}]
                }
            },
        }

        es_response = search_company_events(
            self.es,
            company_id=company_id,
            event_type=EventType.metrics_image,
            body=es_req,
        )
        res = nested_get(es_response, ("aggregations", "debug_images"))
        if not res:
            return [], None

        return [bucket["key"]["url"] for bucket in res["buckets"]], res.get("after_key")

    def get_plot_image_urls(
        self, company_id: str, task_id: str, scroll_id: Optional[str]
    ) -> Tuple[Sequence[dict], Optional[str]]:
        if scroll_id == self.empty_scroll:
            return [], None

        if scroll_id:
            es_res = self.es.scroll(scroll_id=scroll_id, scroll="10m")
        else:
            if check_empty_data(self.es, company_id, EventType.metrics_plot):
                return [], None

            es_req = {
                "size": 1000,
                "_source": [PlotFields.source_urls],
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"task": task_id}},
                            {"exists": {"field": PlotFields.source_urls}},
                        ]
                    }
                },
            }
            es_res = search_company_events(
                self.es,
                company_id=company_id,
                event_type=EventType.metrics_plot,
                body=es_req,
                scroll="10m",
            )

        events, _, next_scroll_id = self._get_events_from_es_res(es_res)
        return events, next_scroll_id

    def get_task_events(
        self,
        company_id: str,
        task_id: Union[str, Sequence[str]],
        event_type: EventType,
        metrics: MetricVariants = None,
        last_iter_count=None,
        sort=None,
        size=500,
        scroll_id=None,
        no_scroll=False,
        model_events=False,
    ) -> TaskEventsResult:
        if scroll_id == self.empty_scroll:
            return TaskEventsResult()

        if scroll_id:
            with translate_errors_context():
                es_res = self.es.scroll(scroll_id=scroll_id, scroll="1h")
        else:
            if check_empty_data(self.es, company_id=company_id, event_type=event_type):
                return TaskEventsResult()

            task_ids = [task_id] if isinstance(task_id, str) else task_id

            must = []
            if metrics:
                must.append(get_metric_variants_condition(metrics))

            if last_iter_count is None or model_events:
                must.append({"terms": {"task": task_ids}})
            else:
                tasks_iters = self.get_last_iters(
                    company_id=company_id,
                    event_type=event_type,
                    task_id=task_ids,
                    iters=last_iter_count,
                    metrics=metrics,
                )
                should = [
                    {
                        "bool": {
                            "must": [
                                {"term": {"task": task}},
                                {"terms": {"iter": last_iters}},
                            ]
                        }
                    }
                    for task, last_iters in tasks_iters.items()
                    if last_iters
                ]
                if not should:
                    return TaskEventsResult()
                must.append({"bool": {"should": should}})

            if sort is None:
                sort = [{"timestamp": {"order": "asc"}}]

            es_req = {
                "sort": sort,
                "size": min(size, 10000),
                "query": {"bool": {"must": must}},
            }

            with translate_errors_context():
                es_res = search_company_events(
                    self.es,
                    company_id=company_id,
                    event_type=event_type,
                    body=es_req,
                    ignore=404,
                    **({} if no_scroll else {"scroll": "1h"}),
                )

        events, total_events, next_scroll_id = self._get_events_from_es_res(es_res)
        if event_type in (EventType.metrics_plot, EventType.all):
            self.uncompress_plots(events)

        return TaskEventsResult(
            events=events, next_scroll_id=next_scroll_id, total_events=total_events
        )

    def get_metrics_and_variants(
        self, company_id: str, task_id: str, event_type: EventType
    ):
        if check_empty_data(self.es, company_id=company_id, event_type=event_type):
            return {}

        query = {"bool": {"must": [{"term": {"task": task_id}}]}}
        search_args = dict(es=self.es, company_id=company_id, event_type=event_type)
        max_metrics, max_variants = get_max_metric_and_variant_counts(
            query=query, **search_args,
        )
        es_req = {
            "size": 0,
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
                            }
                        }
                    },
                }
            },
            "query": query,
        }

        with translate_errors_context():
            es_res = search_company_events(body=es_req, **search_args)

        metrics = {}
        for metric_bucket in es_res["aggregations"]["metrics"].get("buckets"):
            metric = metric_bucket["key"]
            metrics[metric] = [
                b["key"] for b in metric_bucket["variants"].get("buckets")
            ]

        return metrics

    def get_task_latest_scalar_values(
        self, company_id, task_id
    ) -> Tuple[Sequence[dict], int]:
        event_type = EventType.metrics_scalar
        if check_empty_data(self.es, company_id=company_id, event_type=event_type):
            return [], 0

        query = {
            "bool": {
                "must": [
                    {"query_string": {"query": "value:>0"}},
                    {"term": {"task": task_id}},
                ]
            }
        }
        search_args = dict(es=self.es, company_id=company_id, event_type=event_type)
        max_metrics, max_variants = get_max_metric_and_variant_counts(
            query=query, **search_args,
        )
        es_req = {
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
                            "aggs": {
                                "last_value": {
                                    "top_hits": {
                                        "docvalue_fields": ["value"],
                                        "_source": "value",
                                        "size": 1,
                                        "sort": [{"iter": {"order": "desc"}}],
                                    }
                                },
                                "last_timestamp": {"max": {"field": "@timestamp"}},
                                "last_10_value": {
                                    "top_hits": {
                                        "docvalue_fields": ["value"],
                                        "_source": "value",
                                        "size": 10,
                                        "sort": [{"iter": {"order": "desc"}}],
                                    }
                                },
                            },
                        }
                    },
                }
            },
            "_source": {"excludes": []},
        }
        with translate_errors_context():
            es_res = search_company_events(body=es_req, **search_args)

        metrics = []
        max_timestamp = 0
        for metric_bucket in es_res["aggregations"]["metrics"].get("buckets"):
            metric_summary = dict(name=metric_bucket["key"], variants=[])
            for variant_bucket in metric_bucket["variants"].get("buckets"):
                variant_name = variant_bucket["key"]
                last_value = variant_bucket["last_value"]["hits"]["hits"][0]["fields"][
                    "value"
                ][0]
                last_10_value = variant_bucket["last_10_value"]["hits"]["hits"][0][
                    "fields"
                ]["value"][0]
                timestamp = variant_bucket["last_timestamp"]["value"]
                max_timestamp = max(timestamp, max_timestamp)
                metric_summary["variants"].append(
                    dict(
                        name=variant_name,
                        last_value=last_value,
                        last_10_value=last_10_value,
                    )
                )
            metrics.append(metric_summary)
        return metrics, max_timestamp

    def get_vector_metrics_per_iter(self, company_id, task_id, metric, variant):
        event_type = EventType.metrics_vector
        if check_empty_data(self.es, company_id=company_id, event_type=event_type):
            return [], []

        es_req = {
            "size": 10000,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"task": task_id}},
                        {"term": {"metric": metric}},
                        {"term": {"variant": variant}},
                    ]
                }
            },
            "_source": ["iter", "value"],
            "sort": ["iter"],
        }
        with translate_errors_context():
            es_res = search_company_events(
                self.es, company_id=company_id, event_type=event_type, body=es_req
            )

        vectors = []
        iterations = []
        for hit in es_res["hits"]["hits"]:
            vectors.append(hit["_source"]["value"])
            iterations.append(hit["_source"]["iter"])

        return iterations, vectors

    def get_last_iters(
        self,
        company_id: str,
        event_type: EventType,
        task_id: Union[str, Sequence[str]],
        iters: int,
        metrics: MetricVariants = None
    ) -> Mapping[str, Sequence]:
        if check_empty_data(self.es, company_id=company_id, event_type=event_type):
            return {}

        task_ids = [task_id] if isinstance(task_id, str) else task_id
        must = [{"terms": {"task": task_ids}}]
        if metrics:
            must.append(get_metric_variants_condition(metrics))

        es_req: dict = {
            "size": 0,
            "aggs": {
                "tasks": {
                    "terms": {"field": "task"},
                    "aggs": {
                        "iters": {
                            "terms": {
                                "field": "iter",
                                "size": iters,
                                "order": {"_key": "desc"},
                            }
                        }
                    },
                }
            },
            "query": {"bool": {"must": must}},
        }

        with translate_errors_context():
            es_res = search_company_events(
                self.es, company_id=company_id, event_type=event_type, body=es_req,
            )

        if "aggregations" not in es_res:
            return {}

        return {
            tb["key"]: [ib["key"] for ib in tb["iters"]["buckets"]]
            for tb in es_res["aggregations"]["tasks"]["buckets"]
        }

    @staticmethod
    def _validate_model_state(
        company_id: str, model_id: str, allow_locked: bool = False
    ):
        extra_msg = None
        query = Q(id=model_id, company=company_id)
        if not allow_locked:
            query &= Q(ready__ne=True)
            extra_msg = "or model published"
        res = Model.objects(query).only("id").first()
        if not res:
            raise errors.bad_request.InvalidModelId(
                extra_msg, company=company_id, id=model_id
            )

    @staticmethod
    def _validate_task_state(company_id: str, task_id: str, allow_locked: bool = False):
        extra_msg = None
        query = Q(id=task_id, company=company_id)
        if not allow_locked:
            query &= Q(status__nin=LOCKED_TASK_STATUSES)
            extra_msg = "or task published"
        res = Task.objects(query).only("id").first()
        if not res:
            raise errors.bad_request.InvalidTaskId(
                extra_msg, company=company_id, id=task_id
            )

    @staticmethod
    def _get_events_deletion_params(async_delete: bool) -> dict:
        if async_delete:
            return {
                "wait_for_completion": False,
                "requests_per_second": config.get(
                    "services.events.max_async_deleted_events_per_sec", 1000
                ),
            }

        return {"refresh": True}

    def delete_task_events(
        self, company_id, task_id, allow_locked=False, model=False, async_delete=False,
    ):
        if model:
            self._validate_model_state(
                company_id=company_id, model_id=task_id, allow_locked=allow_locked,
            )
        else:
            self._validate_task_state(
                company_id=company_id, task_id=task_id, allow_locked=allow_locked
            )

        es_req = {"query": {"term": {"task": task_id}}}
        with translate_errors_context():
            es_res = delete_company_events(
                es=self.es,
                company_id=company_id,
                event_type=EventType.all,
                body=es_req,
                **self._get_events_deletion_params(async_delete),
            )

        if not async_delete:
            return es_res.get("deleted", 0)

    def clear_task_log(
        self,
        company_id: str,
        task_id: str,
        allow_locked: bool = False,
        threshold_sec: int = None,
    ):
        self._validate_task_state(
            company_id=company_id, task_id=task_id, allow_locked=allow_locked
        )
        if check_empty_data(
            self.es, company_id=company_id, event_type=EventType.task_log
        ):
            return 0

        with translate_errors_context():
            must = [{"term": {"task": task_id}}]
            sort = None
            if threshold_sec:
                timestamp_ms = int(threshold_sec * 1000)
                must.append(
                    {
                        "range": {
                            "timestamp": {
                                "lt": (es_factory.get_timestamp_millis() - timestamp_ms)
                            }
                        }
                    }
                )
                sort = {"timestamp": {"order": "desc"}}
            es_req = {
                "query": {"bool": {"must": must}},
                **({"sort": sort} if sort else {}),
            }
            es_res = delete_company_events(
                es=self.es,
                company_id=company_id,
                event_type=EventType.task_log,
                body=es_req,
                refresh=True,
            )
            return es_res.get("deleted", 0)

    def delete_multi_task_events(
        self, company_id: str, task_ids: Sequence[str], async_delete=False
    ):
        """
        Delete mutliple task events. No check is done for tasks write access
        so it should be checked by the calling code
        """
        es_req = {"query": {"terms": {"task": task_ids}}}
        with translate_errors_context():
            es_res = delete_company_events(
                es=self.es,
                company_id=company_id,
                event_type=EventType.all,
                body=es_req,
                **self._get_events_deletion_params(async_delete),
            )

        if not async_delete:
            return es_res.get("deleted", 0)

    def clear_scroll(self, scroll_id: str):
        if scroll_id == self.empty_scroll:
            return
        # noinspection PyBroadException
        try:
            self.es.clear_scroll(scroll_id=scroll_id)
        except elasticsearch.exceptions.NotFoundError:
            pass
        except elasticsearch.exceptions.RequestError:
            pass
        except Exception as ex:
            log.exception("Failed clearing scroll %s", scroll_id)
