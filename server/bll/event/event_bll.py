from collections import defaultdict
from contextlib import closing
from datetime import datetime
from operator import attrgetter

import attr
import six
from elasticsearch import helpers
from enum import Enum

from mongoengine import Q
from nested_dict import nested_dict

import database.utils as dbutils
import es_factory
from apierrors import errors
from bll.task import TaskBLL
from database.errors import translate_errors_context
from database.model.task.task import Task
from database.model.task.metrics import MetricEvent
from timing_context import TimingContext


class EventType(Enum):
    metrics_scalar = "training_stats_scalar"
    metrics_vector = "training_stats_vector"
    metrics_image = "training_debug_image"
    metrics_plot = "plot"
    task_log = "log"


# noinspection PyTypeChecker
EVENT_TYPES = set(map(attrgetter("value"), EventType))


@attr.s
class TaskEventsResult(object):
    events = attr.ib(type=list, default=attr.Factory(list))
    total_events = attr.ib(type=int, default=0)
    next_scroll_id = attr.ib(type=str, default=None)


class EventBLL(object):
    id_fields = ["task", "iter", "metric", "variant", "key"]

    def __init__(self, events_es=None):
        self.es = events_es if events_es is not None else es_factory.connect("events")

    def add_events(self, company_id, events, worker):
        actions = []
        task_ids = set()
        task_iteration = defaultdict(lambda: 0)
        task_last_events = nested_dict(
            3, dict
        )  # task_id -> metric_hash -> variant_hash -> MetricEvent

        for event in events:
            # remove spaces from event type
            if "type" not in event:
                raise errors.BadRequest("Event must have a 'type' field", event=event)

            event_type = event["type"].replace(" ", "_")
            if event_type not in EVENT_TYPES:
                raise errors.BadRequest(
                    "Invalid event type {}".format(event_type),
                    event=event,
                    types=EVENT_TYPES,
                )

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

            index_name = EventBLL.get_index_name(company_id, event_type)
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

            task_id = event.get("task")
            if task_id is not None:
                es_action["_routing"] = task_id
                task_ids.add(task_id)
                if iter is not None:
                    task_iteration[task_id] = max(iter, task_iteration[task_id])

                if event_type == EventType.metrics_scalar.value:
                    self._update_last_metric_event_for_task(
                        task_last_events=task_last_events, task_id=task_id, event=event
                    )
            else:
                es_action["_routing"] = task_id

            actions.append(es_action)

        if task_ids:
            # verify task_ids
            with translate_errors_context(), TimingContext("mongo", "task_by_ids"):
                res = Task.objects(id__in=task_ids, company=company_id).only("id")
                if len(res) < len(task_ids):
                    invalid_task_ids = tuple(set(task_ids) - set(r.id for r in res))
                    raise errors.bad_request.InvalidTaskId(
                        company=company_id, ids=invalid_task_ids
                    )

        errors_in_bulk = []
        added = 0
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
                        errors_in_bulk.append(info)

            last_metrics = {
                t.id: t.to_proper_dict().get("last_metrics", {})
                for t in Task.objects(id__in=task_ids, company=company_id).only(
                    "last_metrics"
                )
            }

            remaining_tasks = set()
            now = datetime.utcnow()
            for task_id in task_ids:
                # Update related tasks. For reasons of performance, we prefer to update all of them and not only those
                #  who's events were successful

                updated = self._update_task(
                    company_id=company_id,
                    task_id=task_id,
                    now=now,
                    iter=task_iteration.get(task_id),
                    last_events=task_last_events.get(task_id),
                    last_metrics=last_metrics.get(task_id),
                )

                if not updated:
                    remaining_tasks.add(task_id)
                    continue

            if remaining_tasks:
                TaskBLL.set_last_update(remaining_tasks, company_id, last_update=now)

        # Compensate for always adding chunk_size on success (last chunk is probably smaller)
        added = min(added, len(actions))

        return added, errors_in_bulk

    def _update_last_metric_event_for_task(self, task_last_events, task_id, event):
        """
        Update task_last_events structure for the provided task_id with the provided event details if this event is more
        recent than the currently stored event for its metric/variant combination.

        task_last_events contains [hashed_metric_name -> hashed_variant_name -> event]. Keys are hashed to avoid mongodb
        key conflicts due to invalid characters and/or long field names.
        """
        metric = event.get("metric")
        variant = event.get("variant")
        if not (metric and variant):
            return

        metric_hash = dbutils.hash_field_name(metric)
        variant_hash = dbutils.hash_field_name(variant)

        last_events = task_last_events[task_id]

        timestamp = last_events[metric_hash][variant_hash].get("timestamp", None)
        if timestamp is None or timestamp < event["timestamp"]:
            last_events[metric_hash][variant_hash] = event

    def _update_task(
        self, company_id, task_id, now, iter=None, last_events=None, last_metrics=None
    ):
        """
        Update task information in DB with aggregated results after handling event(s) related to this task.

        This updates the task with the highest iteration value encountered during the last events update, as well
        as the latest metric/variant scalar values reported (according to the report timestamp) and the task's last
        update time.
        """
        fields = {}

        if iter is not None:
            fields["last_iteration"] = iter

        if last_events:

            def get_metric_event(ev):
                me = MetricEvent.from_dict(**ev)
                if "timestamp" in ev:
                    me.timestamp = datetime.utcfromtimestamp(ev["timestamp"] / 1000)
                return me

            new_last_metrics = nested_dict(2, MetricEvent)
            new_last_metrics.update(last_metrics)

            for metric_hash, variants in last_events.items():
                for variant_hash, event in variants.items():
                    new_last_metrics[metric_hash][variant_hash] = get_metric_event(
                        event
                    )

            fields["last_metrics"] = new_last_metrics.to_dict()

        if not fields:
            return False

        return TaskBLL.update_statistics(task_id, company_id, last_update=now, **fields)

    def _get_event_id(self, event):
        id_values = (str(event[field]) for field in self.id_fields if field in event)
        return "-".join(id_values)

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

            es_index = EventBLL.get_index_name(company_id, event_type)

            if not self.es.indices.exists(es_index):
                return [], None, 0

            es_req = {
                "size": size,
                "sort": {"timestamp": {"order": order}},
                "query": {"bool": {"must": [{"term": {"task": task_id}}]}},
            }

            with translate_errors_context(), TimingContext("es", "scroll_task_events"):
                es_res = self.es.search(index=es_index, body=es_req, scroll="1h")

        events = [hit["_source"] for hit in es_res["hits"]["hits"]]
        next_scroll_id = es_res["_scroll_id"]
        total_events = es_res["hits"]["total"]

        return events, next_scroll_id, total_events

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

            es_index = EventBLL.get_index_name(company_id, event_type)
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

        es_index = EventBLL.get_index_name(company_id, event_type)

        if not self.es.indices.exists(es_index):
            return {}

        es_req = {
            "size": 0,
            "aggs": {
                "metrics": {
                    "terms": {"field": "metric", "size": 200},
                    "aggs": {"variants": {"terms": {"field": "variant", "size": 200}}},
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
        es_index = EventBLL.get_index_name(company_id, "training_stats_scalar")

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
                        "size": 1000,
                        "order": {"_term": "asc"},
                    },
                    "aggs": {
                        "variants": {
                            "terms": {
                                "field": "variant",
                                "size": 1000,
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

    def compare_scalar_metrics_average_per_iter(
        self, company_id, task_ids, allow_public=True
    ):
        assert isinstance(task_ids, list)

        task_name_by_id = {}
        with translate_errors_context():
            task_objs = Task.get_many(
                company=company_id,
                query=Q(id__in=task_ids),
                allow_public=allow_public,
                override_projection=("id", "name"),
                return_dicts=False,
            )
            if len(task_objs) < len(task_ids):
                invalid = tuple(set(task_ids) - set(r.id for r in task_objs))
                raise errors.bad_request.InvalidTaskId(company=company_id, ids=invalid)

            task_name_by_id = {t.id: t.name for t in task_objs}

        es_index = EventBLL.get_index_name(company_id, "training_stats_scalar")
        if not self.es.indices.exists(es_index):
            return {}

        es_req = {
            "size": 0,
            "_source": {"excludes": []},
            "query": {"terms": {"task": task_ids}},
            "aggs": {
                "iters": {
                    "histogram": {"field": "iter", "interval": 1, "min_doc_count": 1},
                    "aggs": {
                        "metric_and_variant": {
                            "terms": {
                                "script": "doc['metric'].value +'/'+ doc['variant'].value",
                                "size": 10000,
                            },
                            "aggs": {
                                "tasks": {
                                    "terms": {"field": "task"},
                                    "aggs": {"avg_val": {"avg": {"field": "value"}}},
                                }
                            },
                        }
                    },
                }
            },
        }
        with translate_errors_context(), TimingContext("es", "task_stats_comparison"):
            es_res = self.es.search(index=es_index, body=es_req)

        if "aggregations" not in es_res:
            return

        metrics = {}
        for iter_bucket in es_res["aggregations"]["iters"]["buckets"]:
            iteration = int(iter_bucket["key"])
            for metric_bucket in iter_bucket["metric_and_variant"]["buckets"]:
                metric_name = metric_bucket["key"]
                if metrics.get(metric_name) is None:
                    metrics[metric_name] = {}

                metric_data = metrics[metric_name]
                for task_bucket in metric_bucket["tasks"]["buckets"]:
                    task_id = task_bucket["key"]
                    value = task_bucket["avg_val"]["value"]
                    if metric_data.get(task_id) is None:
                        metric_data[task_id] = {
                            "x": [],
                            "y": [],
                            "name": task_name_by_id[
                                task_id
                            ],  # todo: lookup task name from id
                        }
                    metric_data[task_id]["x"].append(iteration)
                    metric_data[task_id]["y"].append(value)

        return metrics

    def get_scalar_metrics_average_per_iter(self, company_id, task_id):

        es_index = EventBLL.get_index_name(company_id, "training_stats_scalar")
        if not self.es.indices.exists(es_index):
            return {}

        es_req = {
            "size": 0,
            "_source": {"excludes": []},
            "query": {"term": {"task": task_id}},
            "aggs": {
                "iters": {
                    "histogram": {"field": "iter", "interval": 1, "min_doc_count": 1},
                    "aggs": {
                        "metrics": {
                            "terms": {
                                "field": "metric",
                                "size": 200,
                                "order": {"_term": "desc"},
                            },
                            "aggs": {
                                "variants": {
                                    "terms": {
                                        "field": "variant",
                                        "size": 500,
                                        "order": {"_term": "desc"},
                                    },
                                    "aggs": {"avg_val": {"avg": {"field": "value"}}},
                                }
                            },
                        }
                    },
                }
            },
            "version": True,
        }

        with translate_errors_context(), TimingContext("es", "task_stats_scalar"):
            es_res = self.es.search(index=es_index, body=es_req, routing=task_id)

        metrics = {}
        if "aggregations" in es_res:
            for iter_bucket in es_res["aggregations"]["iters"]["buckets"]:
                iteration = int(iter_bucket["key"])
                for metric_bucket in iter_bucket["metrics"]["buckets"]:
                    metric_name = metric_bucket["key"]
                    if metrics.get(metric_name) is None:
                        metrics[metric_name] = {}

                    metric_data = metrics[metric_name]
                    for variant_bucket in metric_bucket["variants"]["buckets"]:
                        variant = variant_bucket["key"]
                        value = variant_bucket["avg_val"]["value"]
                        if metric_data.get(variant) is None:
                            metric_data[variant] = {"x": [], "y": [], "name": variant}
                        metric_data[variant]["x"].append(iteration)
                        metric_data[variant]["y"].append(value)
        return metrics

    def get_vector_metrics_per_iter(self, company_id, task_id, metric, variant):

        es_index = EventBLL.get_index_name(company_id, "training_stats_vector")
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

    def delete_task_events(self, company_id, task_id):
        es_index = EventBLL.get_index_name(company_id, "*")
        es_req = {"query": {"term": {"task": task_id}}}
        with translate_errors_context(), TimingContext("es", "delete_task_events"):
            es_res = self.es.delete_by_query(
                index=es_index, body=es_req, routing=task_id, refresh=True
            )

        return es_res.get("deleted", 0)

    @staticmethod
    def get_index_name(company_id, event_type):
        event_type = event_type.lower().replace(" ", "_")
        return "events-%s-%s" % (event_type, company_id)
