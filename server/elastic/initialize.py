from time import sleep

from elasticsearch import Elasticsearch, exceptions

import es_factory
from config import config
from elastic.apply_mappings import apply_mappings_to_cluster

log = config.logger(__file__)


class MissingElasticConfiguration(Exception):
    """
    Exception when cluster configuration is not found in config files
    """

    pass


class ElasticConnectionError(Exception):
    """
    Exception when could not connect to elastic during init
    """

    pass


def check_elastic_empty() -> bool:
    """
    Check for elasticsearch connection
    Use probing settings and not the default es cluster ones
    so that we can handle correctly the connection rejects due to ES not fully started yet
    :return:
    """
    cluster_conf = es_factory.get_cluster_config("events")
    max_retries = config.get("apiserver.elastic.probing.max_retries", 4)
    timeout = config.get("apiserver.elastic.probing.timeout", 30)
    for retry in range(max_retries):
        try:
            es = Elasticsearch(hosts=cluster_conf.get("hosts"))
            return not es.indices.get_template(name="events*")
        except exceptions.NotFoundError as ex:
            log.error(ex)
            return True
        except exceptions.ConnectionError:
            if retry >= max_retries - 1:
                raise ElasticConnectionError()
            log.warn(
                f"Could not connect to es server. Retry {retry+1} of {max_retries}. Waiting for {timeout}sec"
            )
            sleep(timeout)


def init_es_data():
    for name in es_factory.get_all_cluster_names():
        cluster_conf = es_factory.get_cluster_config(name)
        hosts_config = cluster_conf.get("hosts")
        if not hosts_config:
            raise MissingElasticConfiguration(f"for cluster '{name}'")

        log.info(f"Applying mappings to ES host: {hosts_config}")
        args = cluster_conf.get("args", {})
        res = apply_mappings_to_cluster(hosts_config, name, es_args=args)
        log.info(res)
