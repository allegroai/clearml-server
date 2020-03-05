from furl import furl

from config import config
from elastic.apply_mappings import apply_mappings_to_host
from es_factory import get_cluster_config

log = config.logger(__file__)


class MissingElasticConfiguration(Exception):
    """
    Exception when cluster configuration is not found in config files
    """

    pass


def init_es_data():
    hosts_config = get_cluster_config("events").get("hosts")
    if not hosts_config:
        raise MissingElasticConfiguration("for cluster 'events'")

    for conf in hosts_config:
        host = furl(scheme="http", host=conf["host"], port=conf["port"]).url
        log.info(f"Applying mappings to host: {host}")
        res = apply_mappings_to_host(host)
        log.info(res)
