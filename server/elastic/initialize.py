from furl import furl

from config import config
from elastic.apply_mappings import apply_mappings_to_host, get_template
from es_factory import get_cluster_config

log = config.logger(__file__)


class MissingElasticConfiguration(Exception):
    """
    Exception when cluster configuration is not found in config files
    """

    pass


def _url_from_host_conf(conf: dict) -> str:
    return furl(scheme="http", host=conf["host"], port=conf["port"]).url


def init_es_data() -> bool:
    """Return True if the db was empty"""
    hosts_config = get_cluster_config("events").get("hosts")
    if not hosts_config:
        raise MissingElasticConfiguration("for cluster 'events'")

    empty_db = not get_template(_url_from_host_conf(hosts_config[0]), "events*")

    for conf in hosts_config:
        host = _url_from_host_conf(conf)
        log.info(f"Applying mappings to host: {host}")
        res = apply_mappings_to_host(host)
        log.info(res)

    return empty_db
