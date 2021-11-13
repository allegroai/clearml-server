from datetime import datetime
from os import getenv
from typing import Tuple

from boltons.iterutils import first
from elasticsearch import Elasticsearch, Transport

from apiserver.config_repo import config

log = config.logger(__file__)

OVERRIDE_HOST_ENV_KEY = (
    "CLEARML_ELASTIC_SERVICE_HOST",
    "TRAINS_ELASTIC_SERVICE_HOST",
    "ELASTIC_SERVICE_HOST",
    "ELASTIC_SERVICE_SERVICE_HOST",
)
OVERRIDE_PORT_ENV_KEY = (
    "CLEARML_ELASTIC_SERVICE_PORT",
    "TRAINS_ELASTIC_SERVICE_PORT",
    "ELASTIC_SERVICE_PORT",
)

OVERRIDE_USERNAME_ENV_KEY = (
    "CLEARML_ELASTIC_SERVICE_USERNAME",
    "TRAINS_ELASTIC_SERVICE_USERNAME",
    "ELASTIC_SERVICE_USERNAME",
)

OVERRIDE_PASSWORD_ENV_KEY = (
    "CLEARML_ELASTIC_SERVICE_PASSWORD",
    "TRAINS_ELASTIC_SERVICE_PASSWORD",
    "ELASTIC_SERVICE_PASSWORD",
)

OVERRIDE_HOST = first(filter(None, map(getenv, OVERRIDE_HOST_ENV_KEY)))
if OVERRIDE_HOST:
    log.info(f"Using override elastic host {OVERRIDE_HOST}")

OVERRIDE_USERNAME = first(filter(None, map(getenv, OVERRIDE_USERNAME_ENV_KEY)))
if OVERRIDE_USERNAME:
    log.info(f"Using override elastic username {OVERRIDE_USERNAME}")

OVERRIDE_PASSWORD = first(filter(None, map(getenv, OVERRIDE_PASSWORD_ENV_KEY)))
if OVERRIDE_PASSWORD:
    log.info(f"Using override elastic password {OVERRIDE_PASSWORD}")

OVERRIDE_PORT = first(filter(None, map(getenv, OVERRIDE_PORT_ENV_KEY)))
if OVERRIDE_PORT:
    log.info(f"Using override elastic port {OVERRIDE_PORT}")

_instances = {}


class MissingClusterConfiguration(Exception):
    """
    Exception when cluster configuration is not found in config files
    """

    pass


class InvalidClusterConfiguration(Exception):
    """
    Exception when cluster configuration does not contain required properties
    """

    pass


class ESFactory:
    @classmethod
    def connect(cls, cluster_name):
        """
        Returns the es client for the cluster.
        Connects to the cluster if did not connect previously
        :param cluster_name: Dot separated cluster path in the configuration file
        :return: es client
        :raises MissingClusterConfiguration: in case no config section is found for the cluster
        :raises InvalidClusterConfiguration: in case cluster config section misses needed properties
        """
        if cluster_name not in _instances:
            cluster_config = cls.get_cluster_config(cluster_name)
            hosts = cluster_config.get("hosts", None)
            if not hosts:
                raise InvalidClusterConfiguration(cluster_name)

            http_auth = cluster_config.get("http_auth", None)
            args = cluster_config.get("args", {})
            _instances[cluster_name] = Elasticsearch(
                hosts=hosts, transport_class=Transport, http_auth=http_auth, **args
            )

        return _instances[cluster_name]

    @classmethod
    def get_all_cluster_names(cls):
        return list(config.get("hosts.elastic"))

    @classmethod
    def get_override(cls, cluster_name: str) -> Tuple[str, str, str, str]:
        return OVERRIDE_HOST, OVERRIDE_PORT, OVERRIDE_USERNAME, OVERRIDE_PASSWORD

    @classmethod
    def get_cluster_config(cls, cluster_name):
        """
        Returns cluster config for the specified cluster path
        :param cluster_name: Dot separated cluster path in the configuration file
        :return: config section for the cluster
        :raises MissingClusterConfiguration: in case no config section is found for the cluster
        """
        cluster_key = ".".join(("hosts.elastic", cluster_name))
        cluster_config = config.get(cluster_key, None)
        if not cluster_config:
            raise MissingClusterConfiguration(cluster_name)

        def set_host_prop(key, value):
            for entry in cluster_config.get("hosts", []):
                entry[key] = value

        host, port, username, password = cls.get_override(cluster_name)

        if host:
            set_host_prop("host", host)

        if port:
            set_host_prop("port", port)

        if username and password:
            cluster_config.set("http_auth", (username, password))

        return cluster_config

    @classmethod
    def connect_all(cls):
        clusters = config.get("hosts.elastic").as_plain_ordered_dict()
        for name in clusters:
            cls.connect(name)

    @classmethod
    def instances(cls):
        return _instances

    @classmethod
    def timestamp_str_to_millis(cls, ts_str):
        epoch = datetime.utcfromtimestamp(0)
        current_date = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        return int((current_date - epoch).total_seconds() * 1000.0)

    @classmethod
    def get_timestamp_millis(cls):
        now = datetime.utcnow()
        epoch = datetime.utcfromtimestamp(0)
        return int((now - epoch).total_seconds() * 1000.0)

    @classmethod
    def get_es_timestamp_str(cls):
        now = datetime.utcnow()
        return (
            now.strftime("%Y-%m-%dT%H:%M:%S") + ".%03d" % (now.microsecond / 1000) + "Z"
        )


es_factory = ESFactory()
