import hashlib
from collections import defaultdict
from contextlib import closing
from datetime import datetime
from operator import attrgetter
from typing import Sequence, Set, Tuple

import six
from elasticsearch import helpers
from mongoengine import Q
from nested_dict import nested_dict

import database.utils as dbutils
import es_factory
from apierrors import errors
from bll.event.debug_images_iterator import DebugImagesIterator
from bll.event.event_metrics import EventMetrics, EventType
from bll.event.log_events_iterator import LogEventsIterator, TaskEventsResult
from bll.task import TaskBLL
from config import config
from database.errors import translate_errors_context
from database.model.task.task import Task, TaskStatus
from redis_manager import redman
from timing_context import TimingContext
from utilities.dicts import flatten_nested_items

# noinspection PyTypeChecker
EVENT_TYPES = set(map(attrgetter("value"), EventType))
LOCKED_TASK_STATUSES = (TaskStatus.publishing, TaskStatus.published)


class EventBLL(object):
    id_fields = ("task", "iter", "metric", "variant", "key")

    def __init__(self, events_es=None, redis=None):
        self.es = events_es or es_factory.connect("events")
        self._metrics = EventMetrics(self.es)
        self._skip_iteration_for_metric = set(
            config.get("services.events.ignore_iteration.metrics", [])
        )
        self.redis = redis or redman.connection("apiserver")
        self.debug_images_iterator = DebugImagesIterator(es=self.es, redis=self.redis)
        self.log_events_iterator = LogEventsIterator(es=self.es, redis=self.redis)

    @property
    def metrics(self) -> EventMetrics:
        return self._metrics

    @staticmethod
    def _get_valid_tasks(company_id, task_ids: Set, allow_locked_tasks=False) -> Set:
        """Verify that task exists and can be updated"""
        if not task_ids:
            return set()

        with translate_errors_context(), TimingContext("mongo", "task_by_ids"):
            query = Q(id__in=task_ids, company=company_id)
            if not allow_locked_tasks:
                query &= Q(status__nin=LOCKED_TASK_STATUSES)
            res = Task.objects(query).only("id")
            return {r.id for r in res}

    def add_events(
        self, company_id, events, worker, allow_locked_tasks=False
    ) -> Tuple[int, int, dict]:
        actions = []
        task_ids = set()
        task_iteration = defaultdict(lambda: 0)
        task_last_scalar_events = nested_dict(
            3, dict
        )  # task_id -> metric_hash -> variant_hash -> MetricEvent
        task_last_events = nested_dict(
            3, dict
        )  # task_id -> metric_hash -> event_type -> MetricEvent
        errors_per_type = defaultdict(int)
        valid_tasks = self._get_valid_tasks(
            company_id,
            task_ids={
                event["task"] for event in events if event.get("task") is not None
            },
            allow_locked_tasks=allow_locked_tasks,
        )
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

            task_id = event.get("task")
            if task_id is None:
                errors_per_type["Event must have a 'task' field"] += 1
                continue

            if task_id not in valid_tasks:
                errors_per_type["Invalid task id"] += 1
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
                iter = int(iter)
                event["iter"] = iter

            # used to have "values" to indicate array. no need anymore
            if "values" in event:
                event["value"] = event["values"]
                del event["values"]

            event["metric"] = event.get("metric") or ""
            event["variant"] = event.get("variant") or ""

            index_name = EventMetrics.get_index_name(company_id, event_type)
            es_action = {
                "_op_type": "index",  # overwrite if exists with same ID
                "_index": index_name,
                "_type": "event",
                "_source": event,
            }

            # for "log" events, don't assing custom _id - whatever is sent, is written (not overwritten)
            if event_type != "log":
                es_action["_id"] = self._get_event_id(event)
            else:
                es_action["_id"] = dbutils.id()

            es_action["_routing"] = task_id
            task_ids.add(task_id)
            if (
                iter is not None
                and event.get("metric") not in self._skip_iteration_for_metric
            ):
                task_iteration[task_id] = max(iter, task_iteration[task_id])

            self._update_last_metric_events_for_task(
                last_events=task_last_events[task_id], event=event,
            )
            if event_type == EventType.metrics_scalar.value:
                self._update_last_scalar_events_for_task(
                    last_events=task_last_scalar_events[task_id], event=event
                )

            actions.append(es_action)

        added = 0
        if actions:
            chunk_size = 500
            with translate_errors_context(), TimingContext("es", "events_add_batch"):
                # TODO: replace it with helpers.parallel_bulk in the future once the parallel pool leak is fixed
                with closing(
                    helpers.streaming_bulk(
                        self.es,
                        actions,
                        chunk_size=chunk_size,
                        # thread_count=8,
                        refresh=True,
                    )
                ) as it:
                    for success, info in it:
                        if success:
                            added += chunk_size
                        else:
                            errors_per_type["Error when indexing events batch"] += 1

                remaining_tasks = set()
                now = datetime.utcnow()
                for task_id in task_ids:
                    # Update related tasks. For reasons of performance, we prefer to update
                    # all of them and not only those who's events were successful
                    updated = self._update_task(
                        company_id=company_id,
                        task_id=task_id,
                        now=now,
                        iter_max=task_iteration.get(task_id),
                        last_scalar_events=task_last_scalar_events.get(task_id),
                        last_events=task_last_events.get(task_id),
                    )

                    if not updated:
                        remaining_tasks.add(task_id)
                        continue

                if remaining_tasks:
                    TaskBLL.set_last_update(
                        remaining_tasks, company_id, last_update=now
                    )

        # Compensate for always adding chunk_size on success (last chunk is probably smaller)
        added = min(added, len(actions))

        if not added:
            raise errors.bad_request.EventsNotAdded(**errors_per_type)

        errors_count = sum(errors_per_type.values())
        return added, errors_count, errors_per_type

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

        timestamp = last_events[metric_hash][variant_hash].get("timestamp", None)
        if timestamp is None or timestamp < event["timestamp"]:
            last_events[metric_hash][variant_hash] = event

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
        fields = {}

        if iter_max is not None:
            fields["last_iteration_max"] = iter_max

        if last_scalar_events:
            fields["last_scalar_values"] = list(
                flatten_nested_items(
                    last_scalar_events,
                    nesting=2,
                    include_leaves=["value", "metric", "variant"],
                )
            )

        if last_events:
            fields["last_events"] = last_events

        if not fields:
            return False

        return TaskBLL.update_statistics(task_id, company_id, last_update=now, **fields)

    def _get_event_id(self, event):
        id_values = (str(event[field]) for field in self.id_fields if field in event)
        return hashlib.md5("-".join(id_values).encode()).hexdigest()

    def scroll_task_events(
        self,
        company_id,
        task_id,
        order,
        event_type=None,
        batch_size=10000,
        scroll_id=None,
    ):
        if scroll_id:
            with translate_errors_context(), TimingContext("es", "task_log_events"):
                es_res = self.es.scroll(scroll_id=scroll_id, scroll="1h")
        else:
            size = min(batch_size, 10000)
            if event_type is None:
                event_type = "*"

            es_index = EventMetrics.get_index_name(company_id, event_type)

            if not self.es.indices.exists(es_index):
                return [], None, 0

            es_req = {
                "size": size,
                "sort": {"timestamp": {"order": order}},
                "query": {"bool": {"must": [{"term": {"task": task_id}}]}},
            }

            with translate_errors_context(), TimingContext("es", "scroll_task_events"):
                es_res = self.es.search(
                    index=es_index, body=es_req, scroll="1h", routing=task_id
                )

        events = [hit["_source"] for hit in es_res["hits"]["hits"]]
        next_scroll_id = es_res["_scroll_id"]
        total_events = es_res["hits"]["total"]

        return events, next_scroll_id, total_events

    def get_last_iterations_per_event_metric_variant(
        self, es_index: str, task_id: str, num_last_iterations: int, event_type: str
    ):
        if not self.es.indices.exists(es_index):
            return []

        es_req: dict = {
            "size": 0,
            "aggs": {
                "metrics": {
                    "terms": {
                        "field": "metric",
                        "size": EventMetrics.MAX_METRICS_COUNT,
                    },
                    "aggs": {
                        "variants": {
                            "terms": {
                                "field": "variant",
                                "size": EventMetrics.MAX_VARIANTS_COUNT,
                            },
                            "aggs": {
                                "iters": {
                                    "terms": {
                                        "field": "iter",
                                        "size": num_last_iterations,
                                        "order": {"_term": "desc"},
                                    }
                                }
                            },
                        }
                    },
                }
            },
            "query": {"bool": {"must": [{"term": {"task": task_id}}]}},
        }
        if event_type:
            es_req["query"]["bool"]["must"].append({"term": {"type": event_type}})

        with translate_errors_context(), TimingContext(
            "es", "task_last_iter_metric_variant"
        ):
            es_res = self.es.search(index=es_index, body=es_req, routing=task_id)
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
    ):
        if scroll_id:
            with translate_errors_context(), TimingContext("es", "get_task_events"):
                es_res = self.es.scroll(scroll_id=scroll_id, scroll="1h")
        else:
            event_type = "plot"
            es_index = EventMetrics.get_index_name(company_id, event_type)
            if not self.es.indices.exists(es_index):
                return TaskEventsResult()

            query = {"bool": defaultdict(list)}

            if last_iterations_per_plot is None:
                must = query["bool"]["must"]
                must.append({"terms": {"task": tasks}})
            else:
                should = query["bool"]["should"]
                for i, task_id in enumerate(tasks):
                    last_iters = self.get_last_iterations_per_event_metric_variant(
                        es_index, task_id, last_iterations_per_plot, event_type
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

            if sort is None:
                sort = [{"timestamp": {"order": "asc"}}]

            es_req = {"sort": sort, "size": min(size, 10000), "query": query}

            routing = ",".join(tasks)

            with translate_errors_context(), TimingContext("es", "get_task_plots"):
                es_res = self.es.search(
                    index=es_index,
                    body=es_req,
                    ignore=404,
                    routing=routing,
                    scroll="1h",
                )

        events = [doc["_source"] for doc in es_res.get("hits", {}).get("hits", [])]
        # scroll id may be missing when queering a totally empty DB
        next_scroll_id = es_res.get("_scroll_id")
        total_events = es_res["hits"]["total"]

        return TaskEventsResult(
            events=events, next_scroll_id=next_scroll_id, total_events=total_events
        )

    def get_task_events(
        self,
        company_id,
        task_id,
        event_type=None,
        metric=None,
        variant=None,
        last_iter_count=None,
        sort=None,
        size=500,
        scroll_id=None,
    ):

        if scroll_id:
            with translate_errors_context(), TimingContext("es", "get_task_events"):
                es_res = self.es.scroll(scroll_id=scroll_id, scroll="1h")
        else:
            task_ids = [task_id] if isinstance(task_id, six.string_types) else task_id
            if event_type is None:
                event_type = "*"

            es_index = EventMetrics.get_index_name(company_id, event_type)
            if not self.es.indices.exists(es_index):
                return TaskEventsResult()

            query = {"bool": defaultdict(list)}

            if metric or variant:
                must = query["bool"]["must"]
                if metric:
                    must.append({"term": {"metric": metric}})
                if variant:
                    must.append({"term": {"variant": variant}})

            if last_iter_count is None:
                must = query["bool"]["must"]
                must.append({"terms": {"task": task_ids}})
            else:
                should = query["bool"]["should"]
                for i, task_id in enumerate(task_ids):
                    last_iters = self.get_last_iters(
                        es_index, task_id, event_type, last_iter_count
                    )
                    if not last_iters:
                        continue
                    should.append(
                        {
                            "bool": {
                                "must": [
                                    {"term": {"task": task_id}},
                                    {"terms": {"iter": last_iters}},
                                ]
                            }
                        }
                    )
                if not should:
                    return TaskEventsResult()

            if sort is None:
                sort = [{"timestamp": {"order": "asc"}}]

            es_req = {"sort": sort, "size": min(size, 10000), "query": query}

            routing = ",".join(task_ids)

            with translate_errors_context(), TimingContext("es", "get_task_events"):
                es_res = self.es.search(
                    index=es_index,
                    body=es_req,
                    ignore=404,
                    routing=routing,
                    scroll="1h",
                )

        events = [doc["_source"] for doc in es_res.get("hits", {}).get("hits", [])]
        next_scroll_id = es_res["_scroll_id"]
        total_events = es_res["hits"]["total"]

        return TaskEventsResult(
            events=events, next_scroll_id=next_scroll_id, total_events=total_events
        )

    def get_metrics_and_variants(self, company_id, task_id, event_type):

        es_index = EventMetrics.get_index_name(company_id, event_type)

        if not self.es.indices.exists(es_index):
            return {}

        es_req = {
            "size": 0,
            "aggs": {
                "metrics": {
                    "terms": {
                        "field": "metric",
                        "size": EventMetrics.MAX_METRICS_COUNT,
                    },
                    "aggs": {
                        "variants": {
                            "terms": {
                                "field": "variant",
                                "size": EventMetrics.MAX_VARIANTS_COUNT,
                            }
                        }
                    },
                }
            },
            "query": {"bool": {"must": [{"term": {"task": task_id}}]}},
        }

        with translate_errors_context(), TimingContext(
            "es", "events_get_metrics_and_variants"
        ):
            es_res = self.es.search(index=es_index, body=es_req, routing=task_id)

        metrics = {}
        for metric_bucket in es_res["aggregations"]["metrics"].get("buckets"):
            metric = metric_bucket["key"]
            metrics[metric] = [
                b["key"] for b in metric_bucket["variants"].get("buckets")
            ]

        return metrics

    def get_task_latest_scalar_values(self, company_id, task_id):
        es_index = EventMetrics.get_index_name(company_id, "training_stats_scalar")

        if not self.es.indices.exists(es_index):
            return {}

        es_req = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {"query_string": {"query": "value:>0"}},
                        {"term": {"task": task_id}},
                    ]
                }
            },
            "aggs": {
                "metrics": {
                    "terms": {
                        "field": "metric",
                        "size": EventMetrics.MAX_METRICS_COUNT,
                        "order": {"_term": "asc"},
                    },
                    "aggs": {
                        "variants": {
                            "terms": {
                                "field": "variant",
                                "size": EventMetrics.MAX_VARIANTS_COUNT,
                                "order": {"_term": "asc"},
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
        with translate_errors_context(), TimingContext(
            "es", "events_get_metrics_and_variants"
        ):
            es_res = self.es.search(index=es_index, body=es_req, routing=task_id)

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

        es_index = EventMetrics.get_index_name(company_id, "training_stats_vector")
        if not self.es.indices.exists(es_index):
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
        with translate_errors_context(), TimingContext("es", "task_stats_vector"):
            es_res = self.es.search(index=es_index, body=es_req, routing=task_id)

        vectors = []
        iterations = []
        for hit in es_res["hits"]["hits"]:
            vectors.append(hit["_source"]["value"])
            iterations.append(hit["_source"]["iter"])

        return iterations, vectors

    def get_last_iters(self, es_index, task_id, event_type, iters):
        if not self.es.indices.exists(es_index):
            return []

        es_req: dict = {
            "size": 0,
            "aggs": {
                "iters": {
                    "terms": {
                        "field": "iter",
                        "size": iters,
                        "order": {"_term": "desc"},
                    }
                }
            },
            "query": {"bool": {"must": [{"term": {"task": task_id}}]}},
        }
        if event_type:
            es_req["query"]["bool"]["must"].append({"term": {"type": event_type}})

        with translate_errors_context(), TimingContext("es", "task_last_iter"):
            es_res = self.es.search(index=es_index, body=es_req, routing=task_id)
        if "aggregations" not in es_res:
            return []

        return [b["key"] for b in es_res["aggregations"]["iters"]["buckets"]]

    def delete_task_events(self, company_id, task_id, allow_locked=False):
        with translate_errors_context():
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

        es_index = EventMetrics.get_index_name(company_id, "*")
        es_req = {"query": {"term": {"task": task_id}}}
        with translate_errors_context(), TimingContext("es", "delete_task_events"):
            es_res = self.es.delete_by_query(
                index=es_index, body=es_req, routing=task_id, refresh=True
            )

        return es_res.get("deleted", 0)
