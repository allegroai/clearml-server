from enum import Enum
from typing import Union, Sequence

from boltons.typeutils import classproperty
from elasticsearch import Elasticsearch

from apiserver.config_repo import config


class EventType(Enum):
    metrics_scalar = "training_stats_scalar"
    metrics_vector = "training_stats_vector"
    metrics_image = "training_debug_image"
    metrics_plot = "plot"
    task_log = "log"
    all = "*"


class EventSettings:
    @classproperty
    def max_workers(self):
        return config.get("services.events.events_retrieval.max_metrics_concurrency", 4)

    @classproperty
    def state_expiration_sec(self):
        return config.get(
            f"services.events.events_retrieval.state_expiration_sec", 3600
        )

    @classproperty
    def max_metrics_count(self):
        return config.get("services.events.events_retrieval.max_metrics_count", 100)

    @classproperty
    def max_variants_count(self):
        return config.get("services.events.events_retrieval.max_variants_count", 100)


def get_index_name(company_id: str, event_type: str):
    event_type = event_type.lower().replace(" ", "_")
    return f"events-{event_type}-{company_id}"


def check_empty_data(es: Elasticsearch, company_id: str, event_type: EventType) -> bool:
    es_index = get_index_name(company_id, event_type.value)
    if not es.indices.exists(es_index):
        return True
    return False


def search_company_events(
    es: Elasticsearch,
    company_id: Union[str, Sequence[str]],
    event_type: EventType,
    body: dict,
    **kwargs,
) -> dict:
    es_index = get_index_name(company_id, event_type.value)
    return es.search(index=es_index, body=body, **kwargs)


def delete_company_events(
    es: Elasticsearch, company_id: str, event_type: EventType, body: dict, **kwargs
) -> dict:
    es_index = get_index_name(company_id, event_type.value)
    return es.delete_by_query(index=es_index, body=body, **kwargs)
