import threading
from os import getenv
from time import sleep

from boltons.iterutils import first
from redis import StrictRedis
from redis.sentinel import Sentinel, SentinelConnectionPool

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


class MyPubSubWorkerThread(threading.Thread):
    def __init__(self, sentinel, on_new_master, msg_sleep_time, daemon=True):
        super(MyPubSubWorkerThread, self).__init__()
        self.daemon = daemon
        self.sentinel = sentinel
        self.on_new_master = on_new_master
        self.sentinel_host = sentinel.connection_pool.connection_kwargs["host"]
        self.msg_sleep_time = msg_sleep_time
        self._running = False
        self.pubsub = None

    def subscribe(self):
        if self.pubsub:
            try:
                self.pubsub.unsubscribe()
                self.pubsub.punsubscribe()
            except Exception:
                pass
            finally:
                self.pubsub = None

        subscriptions = {"+switch-master": self.on_new_master}

        while not self.pubsub or not self.pubsub.subscribed:
            try:
                self.pubsub = self.sentinel.pubsub()
                self.pubsub.subscribe(**subscriptions)
            except Exception as ex:
                log.warn(
                    f"Error while subscribing to sentinel at {self.sentinel_host} ({ex.args[0]}) Sleeping and retrying"
                )
                sleep(3)
        log.info(f"Subscribed to sentinel {self.sentinel_host}")

    def run(self):
        if self._running:
            return
        self._running = True

        self.subscribe()

        while self.pubsub.subscribed:
            try:
                self.pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=self.msg_sleep_time
                )
            except Exception as ex:
                log.warn(
                    f"Error while getting message from sentinel {self.sentinel_host} ({ex.args[0]}) Resubscribing"
                )
                self.subscribe()

        self.pubsub.close()
        self._running = False

    def stop(self):
        # stopping simply unsubscribes from all channels and patterns.
        # the unsubscribe responses that are generated will short circuit
        # the loop in run(), calling pubsub.close() to clean up the connection
        self.pubsub.unsubscribe()
        self.pubsub.punsubscribe()


# todo,future - multi master clusters?
class RedisCluster(object):
    def __init__(self, sentinel_hosts, service_name, **connection_kwargs):
        self.service_name = service_name
        self.sentinel = Sentinel(sentinel_hosts, **connection_kwargs)
        self.master = None
        self.master_host_port = None
        self.reconfigure()
        self.sentinel_threads = {}
        self.listen()

    def reconfigure(self):
        try:
            self.master_host_port = self.sentinel.discover_master(self.service_name)
            self.master = self.sentinel.master_for(self.service_name)
            log.info(f"Reconfigured master to {self.master_host_port}")
        except Exception as ex:
            log.error(f"Error while reconfiguring. {ex.args[0]}")

    def listen(self):
        def on_new_master(workerThread):
            self.reconfigure()

        for sentinel in self.sentinel.sentinels:
            sentinel_host = sentinel.connection_pool.connection_kwargs["host"]
            self.sentinel_threads[sentinel_host] = MyPubSubWorkerThread(
                sentinel, on_new_master, msg_sleep_time=0.001, daemon=True
            )
            self.sentinel_threads[sentinel_host].start()


class RedisManager(object):
    def __init__(self, redis_config_dict):
        self.aliases = {}
        for alias, alias_config in redis_config_dict.items():

            alias_config = alias_config.as_plain_ordered_dict()
            alias_config["password"] = config.get(f"secure.redis.{alias}.password", None)

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

            db = alias_config.get("db", 0)

            sentinels = alias_config.get("sentinels", None)
            service_name = alias_config.get("service_name", None)

            if not is_cluster and sentinels:
                raise ConfigError(
                    "Redis configuration is invalid. mixed regular and cluster mode",
                    alias=alias,
                )
            if is_cluster and (not sentinels or not service_name):
                raise ConfigError(
                    "Redis configuration is invalid. missing sentinels or service_name",
                    alias=alias,
                )
            if not is_cluster and (not port or not host):
                raise ConfigError(
                    "Redis configuration is invalid. missing port or host", alias=alias
                )

            if is_cluster:
                # todo support all redis connection args via sentinel's connection_kwargs
                del alias_config["sentinels"]
                del alias_config["cluster"]
                del alias_config["service_name"]
                self.aliases[alias] = RedisCluster(
                    sentinels, service_name, **alias_config
                )
            else:
                self.aliases[alias] = StrictRedis(**alias_config)

    def connection(self, alias) -> StrictRedis:
        obj = self.aliases.get(alias)
        if not obj:
            raise GeneralError(f"Invalid Redis alias {alias}")
        if isinstance(obj, RedisCluster):
            obj.master.get("health")
            return obj.master
        else:
            obj.get("health")
            return obj

    def host(self, alias):
        r = self.connection(alias)
        pool = r.connection_pool
        if isinstance(pool, SentinelConnectionPool):
            connections = pool.connection_kwargs[
                "connection_pool"
            ]._available_connections
        else:
            connections = pool._available_connections

        if len(connections) > 0:
            return connections[0].host
        else:
            return None


redman = RedisManager(config.get("hosts.redis"))
