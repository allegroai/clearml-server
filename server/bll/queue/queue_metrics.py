from collections import defaultdict
from datetime import datetime
from typing import Sequence

import elasticsearch.helpers
from elasticsearch import Elasticsearch

import es_factory
from apierrors.errors import bad_request
from bll.query import Builder as QueryBuilder
from config import config
from database.errors import translate_errors_context
from database.model.queue import Queue, Entry
from timing_context import TimingContext

log = config.logger(__file__)


class QueueMetrics:
    class EsKeys:
        DOC_TYPE = "metrics"
        WAITING_TIME_FIELD = "average_waiting_time"
        QUEUE_LENGTH_FIELD = "queue_length"
        TIMESTAMP_FIELD = "timestamp"
        QUEUE_FIELD = "queue"

    def __init__(self, es: Elasticsearch):
        self.es = es

    @staticmethod
    def _queue_metrics_prefix_for_company(company_id: str) -> str:
        """Returns the es index prefix for the company"""
        return f"queue_metrics_{company_id}_"

    @staticmethod
    def _get_es_index_suffix():
        """Get the index name suffix for storing current month data"""
        return datetime.utcnow().strftime("%Y-%m")

    @staticmethod
    def _calc_avg_waiting_time(entries: Sequence[Entry]) -> float:
        """
        Calculate avg waiting time for the given tasks.
        Return 0 if the list is empty
        """
        if not entries:
            return 0

        now = datetime.utcnow()
        total_waiting_in_secs = sum((now - e.added).total_seconds() for e in entries)
        return total_waiting_in_secs / len(entries)

    def log_queue_metrics_to_es(self, company_id: str, queues: Sequence[Queue]) -> bool:
        """
        Calculate and write queue statistics (avg waiting time and queue length) to Elastic
        :return: True if the write to es was successful, false otherwise
        """
        es_index = (
            self._queue_metrics_prefix_for_company(company_id)
            + self._get_es_index_suffix()
        )

        timestamp = es_factory.get_timestamp_millis()

        def make_doc(queue: Queue) -> dict:
            entries = [e for e in queue.entries if e.added]
            return dict(
                _index=es_index,
                _type=self.EsKeys.DOC_TYPE,
                _source={
                    self.EsKeys.TIMESTAMP_FIELD: timestamp,
                    self.EsKeys.QUEUE_FIELD: queue.id,
                    self.EsKeys.WAITING_TIME_FIELD: self._calc_avg_waiting_time(
                        entries
                    ),
                    self.EsKeys.QUEUE_LENGTH_FIELD: len(entries),
                },
            )

        actions = list(map(make_doc, queues))

        es_res = elasticsearch.helpers.bulk(self.es, actions)
        added, errors = es_res[:2]
        return (added == len(actions)) and not errors

    def _log_current_metrics(self, company_id: str, queue_ids=Sequence[str]):
        query = dict(company=company_id)
        if queue_ids:
            query["id__in"] = list(queue_ids)
        queues = Queue.objects(**query)
        self.log_queue_metrics_to_es(company_id, queues=list(queues))

    def _search_company_metrics(self, company_id: str, es_req: dict) -> dict:
        return self.es.search(
            index=f"{self._queue_metrics_prefix_for_company(company_id)}*",
            doc_type=self.EsKeys.DOC_TYPE,
            body=es_req,
        )

    @classmethod
    def _get_dates_agg(cls, interval) -> dict:
        """
        Aggregation for building date histogram with internal grouping per queue.
        We are grouping by queue inside date histogram and not vice versa so that
        it will be easy to average between queue metrics inside each date bucket.
        Ignore empty buckets.
        """
        return {
            "dates": {
                "date_histogram": {
                    "field": cls.EsKeys.TIMESTAMP_FIELD,
                    "interval": f"{interval}s",
                    "min_doc_count": 1,
                },
                "aggs": {
                    "queues": {
                        "terms": {"field": cls.EsKeys.QUEUE_FIELD},
                        "aggs": cls._get_top_waiting_agg(),
                    }
                },
            }
        }

    @classmethod
    def _get_top_waiting_agg(cls) -> dict:
        """
        Aggregation for getting max waiting time and the corresponding queue length
        inside each date->queue bucket
        """
        return {
            "top_avg_waiting": {
                "top_hits": {
                    "sort": [
                        {cls.EsKeys.WAITING_TIME_FIELD: {"order": "desc"}},
                        {cls.EsKeys.QUEUE_LENGTH_FIELD: {"order": "desc"}},
                    ],
                    "_source": {
                        "includes": [
                            cls.EsKeys.WAITING_TIME_FIELD,
                            cls.EsKeys.QUEUE_LENGTH_FIELD,
                        ]
                    },
                    "size": 1,
                }
            }
        }

    def get_queue_metrics(
        self,
        company_id: str,
        from_date: float,
        to_date: float,
        interval: int,
        queue_ids: Sequence[str],
    ) -> dict:
        """
        Get the company queue metrics in the specified time range.
        Returned as date histograms of average values per queue and metric type.
        The from_date is extended by 'metrics_before_from_date' seconds from
        queues.conf due to possibly small amount of points. The default extension is 3600s
        In case no queue ids are specified the avg across all the
        company queues is calculated for each metric
        """
        # self._log_current_metrics(company, queue_ids=queue_ids)

        if from_date >= to_date:
            raise bad_request.FieldsValueError("from_date must be less than to_date")

        seconds_before = config.get("services.queues.metrics_before_from_date", 3600)
        must_terms = [QueryBuilder.dates_range(from_date - seconds_before, to_date)]
        if queue_ids:
            must_terms.append(QueryBuilder.terms("queue", queue_ids))

        es_req = {
            "size": 0,
            "query": {"bool": {"must": must_terms}},
            "aggs": self._get_dates_agg(interval),
        }

        with translate_errors_context(), TimingContext("es", "get_queue_metrics"):
            res = self._search_company_metrics(company_id, es_req)

        if "aggregations" not in res:
            return {}

        date_metrics = [
            dict(
                timestamp=d["key"],
                queue_metrics=self._extract_queue_metrics(d["queues"]["buckets"]),
            )
            for d in res["aggregations"]["dates"]["buckets"]
            if d["doc_count"] > 0
        ]
        if queue_ids:
            return self._datetime_histogram_per_queue(date_metrics)

        return self._average_datetime_histogram(date_metrics)

    @classmethod
    def _datetime_histogram_per_queue(cls, date_metrics: Sequence[dict]) -> dict:
        """
        Build datetime histogram per queue from datetime histogram where every
        bucket contains all the queues metrics
        """
        queues_data = defaultdict(list)
        for date_data in date_metrics:
            timestamp = date_data["timestamp"]
            for queue, metrics in date_data["queue_metrics"].items():
                queues_data[queue].append({"date": timestamp, **metrics})

        return queues_data

    @classmethod
    def _average_datetime_histogram(cls, date_metrics: Sequence[dict]) -> dict:
        """
        Calculate weighted averages and total count for each bucket of date_metrics histogram.
        If for any queue the data is missing then take it from the previous bucket
        The result is returned as a dictionary with one key 'total'
        """
        queues_total = []
        last_values = {}
        for date_data in date_metrics:
            date_metrics = date_data["queue_metrics"]
            queue_metrics = {
                **date_metrics,
                **{k: v for k, v in last_values.items() if k not in date_metrics},
            }

            total_length = sum(m["queue_length"] for m in queue_metrics.values())
            if total_length:
                total_average = sum(
                    m["avg_waiting_time"] * m["queue_length"] / total_length
                    for m in queue_metrics.values()
                )
            else:
                total_average = 0

            queues_total.append(
                dict(
                    date=date_data["timestamp"],
                    avg_waiting_time=total_average,
                    queue_length=total_length,
                )
            )

            for k, v in date_metrics.items():
                last_values[k] = v

        return dict(total=queues_total)

    @classmethod
    def _extract_queue_metrics(cls, queue_buckets: Sequence[dict]) -> dict:
        """
        Extract ES data for single date and queue bucket
        """
        queue_metrics = dict()
        for queue_data in queue_buckets:
            if not queue_data["doc_count"]:
                continue
            res = queue_data["top_avg_waiting"]["hits"]["hits"][0]["_source"]
            queue_metrics[queue_data["key"]] = {
                "queue_length": res[cls.EsKeys.QUEUE_LENGTH_FIELD],
                "avg_waiting_time": res[cls.EsKeys.WAITING_TIME_FIELD],
            }
        return queue_metrics
