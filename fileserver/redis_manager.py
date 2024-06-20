from os import getenv

from boltons.iterutils import first
from redis import StrictRedis
from redis.cluster import RedisCluster

from apiserver.apierrors.errors.server_error import ConfigError, GeneralError
from apiserver.config_repo import config

log = config.logger(__file__)

OVERRIDE_HOST_ENV_KEY = (
    "CLEARML_REDIS_SERVICE_HOST",
    "TRAINS_REDIS_SERVICE_HOST",
    "REDIS_SERVICE_HOST",
)
OVERRIDE_PORT_ENV_KEY = (
    "CLEARML_REDIS_SERVICE_PORT",
    "TRAINS_REDIS_SERVICE_PORT",
    "REDIS_SERVICE_PORT",
)
OVERRIDE_PASSWORD_ENV_KEY = (
    "CLEARML_REDIS_SERVICE_PASSWORD",
    "TRAINS_REDIS_SERVICE_PASSWORD",
    "REDIS_SERVICE_PASSWORD",
)

OVERRIDE_HOST = first(filter(None, map(getenv, OVERRIDE_HOST_ENV_KEY)))
if OVERRIDE_HOST:
    log.info(f"Using override redis host {OVERRIDE_HOST}")

OVERRIDE_PORT = first(filter(None, map(getenv, OVERRIDE_PORT_ENV_KEY)))
if OVERRIDE_PORT:
    log.info(f"Using override redis port {OVERRIDE_PORT}")

OVERRIDE_PASSWORD = first(filter(None, map(getenv, OVERRIDE_PASSWORD_ENV_KEY)))


class RedisManager(object):
    def __init__(self, redis_config_dict):
        self.aliases = {}
        for alias, alias_config in redis_config_dict.items():

            alias_config = alias_config.as_plain_ordered_dict()
            alias_config["password"] = config.get(
                f"secure.redis.{alias}.password", None
            )

            is_cluster = alias_config.get("cluster", False)

            host = OVERRIDE_HOST or alias_config.get("host", None)
            if host:
                alias_config["host"] = host

            port = OVERRIDE_PORT or alias_config.get("port", None)
            if port:
                alias_config["port"] = port

            password = OVERRIDE_PASSWORD or alias_config.get("password", None)
            if password:
                alias_config["password"] = password

            if not port or not host:
                raise ConfigError(
                    "Redis configuration is invalid. missing port or host", alias=alias
                )

            if is_cluster:
                del alias_config["cluster"]
                del alias_config["db"]
                self.aliases[alias] = RedisCluster(**alias_config)
            else:
                self.aliases[alias] = StrictRedis(**alias_config)

    def connection(self, alias) -> StrictRedis:
        obj = self.aliases.get(alias)
        if not obj:
            raise GeneralError(f"Invalid Redis alias {alias}")

        obj.get("health")
        return obj

    def host(self, alias):
        r = self.connection(alias)
        if isinstance(r, RedisCluster):
            connections = r.get_default_node().redis_connection.connection_pool._available_connections
        else:
            connections = r.connection_pool._available_connections

        if not connections:
            return None

        return connections[0].host


redman = RedisManager(config.get("hosts.redis"))
