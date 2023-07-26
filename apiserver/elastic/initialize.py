import logging
from time import sleep
from typing import Type, Optional, Sequence, Any, Union

import urllib3.exceptions
from elasticsearch import Elasticsearch, exceptions

from apiserver.es_factory import es_factory
from apiserver.config_repo import config
from apiserver.elastic.apply_mappings import apply_mappings_to_cluster

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


class ConnectionErrorFilter(logging.Filter):
    def __init__(
        self,
        level: Optional[Union[int, str]] = None,
        err_type: Optional[Type] = None,
        args_prefix: Optional[Sequence[Any]] = None,
    ):
        super(ConnectionErrorFilter, self).__init__()
        if level is None:
            self.level = None
        else:
            try:
                self.level = int(level)
            except ValueError:
                self.level = logging.getLevelName(level)

        self.err_type = err_type
        self.args = args_prefix and tuple(args_prefix)
        self.last_blocked = None

    def filter(self, record):
        try:
            filter_out = (
                (self.err_type is None or record.exc_info[0] == self.err_type)
                and (self.level is None or record.levelno == self.level)
                and (self.args is None or record.args[: len(self.args)] == self.args)
            )
            if filter_out:
                self.last_blocked = record
            return not filter_out
        except Exception:
            return True


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

    es_logger = logging.getLogger("elasticsearch")
    log_filter = ConnectionErrorFilter(
        err_type=urllib3.exceptions.NewConnectionError, args_prefix=("GET",)
    )

    try:
        es_logger.addFilter(log_filter)
        for retry in range(max_retries):
            try:
                es = Elasticsearch(
                    hosts=cluster_conf.get("hosts", None),
                    http_auth=es_factory.get_credentials("events", cluster_conf),
                    **cluster_conf.get("args", {}),
                )
                return not es.indices.get_template(name="events*")
            except exceptions.NotFoundError as ex:
                log.error(ex)
                return True
            except exceptions.ConnectionError as ex:
                if retry >= max_retries - 1:
                    raise ElasticConnectionError(
                        f"Error connecting to Elasticsearch: {str(ex)}"
                    )
                log.warn(
                    f"Could not connect to ElasticSearch Service. Retry {retry+1} of {max_retries}. Waiting for {timeout}sec"
                )
                sleep(timeout)
    finally:
        es_logger.removeFilter(log_filter)


def init_es_data():
    for name in es_factory.get_all_cluster_names():
        cluster_conf = es_factory.get_cluster_config(name)
        hosts_config = cluster_conf.get("hosts")
        if not hosts_config:
            raise MissingElasticConfiguration(f"for cluster '{name}'")

        log.info(f"Applying mappings to ES host: {hosts_config}")
        args = cluster_conf.get("args", {})
        http_auth = es_factory.get_credentials(name)

        res = apply_mappings_to_cluster(
            hosts_config, name, es_args=args, http_auth=http_auth
        )
        log.info(res)
