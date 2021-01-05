import atexit
from hashlib import md5

from flask import Flask
from semantic_version import Version

from apiserver.database import database
from apiserver.bll.statistics.stats_reporter import StatisticsReporter
from apiserver.config import config, info
from apiserver.elastic.initialize import init_es_data, check_elastic_empty, ElasticConnectionError
from apiserver.mongo.initialize import (
    init_mongo_data,
    pre_populate_data,
    check_mongo_empty,
    get_last_server_version,
)
from apiserver.service_repo import ServiceRepo
from apiserver.sync import distributed_lock
from apiserver.updates import check_updates_thread
from apiserver.utilities.threads_manager import ThreadsManager

log = config.logger(__file__)


class AppSequence:
    def __init__(self, app: Flask):
        self.app = app

    def start(self):
        log.info("################ API Server initializing #####################")
        self._configure()
        self._init_dbs()
        self._load_services()
        self._start_worker()
        atexit.register(self._on_worker_stop)

    def _configure(self):
        self.app.config["SECRET_KEY"] = config.get(
            "secure.http.session_secret.apiserver"
        )
        self.app.config["JSONIFY_PRETTYPRINT_REGULAR"] = config.get(
            "apiserver.pretty_json"
        )

    def _init_dbs(self):
        database.initialize()

        # build a key that uniquely identifies specific mongo instance
        hosts_string = ";".join(sorted(database.get_hosts()))
        key = "db_init_" + md5(hosts_string.encode()).hexdigest()
        with distributed_lock(key, timeout=config.get("apiserver.db_init_timout", 120)):
            upgrade_monitoring = config.get(
                "apiserver.elastic.upgrade_monitoring.v16_migration_verification", True
            )
            try:
                empty_es = check_elastic_empty()
            except ElasticConnectionError as err:
                if not upgrade_monitoring:
                    raise
                log.error(err)
                info.es_connection_error = True

            empty_db = check_mongo_empty()
            if (
                upgrade_monitoring
                and not empty_db
                and (info.es_connection_error or empty_es)
                and get_last_server_version() < Version("0.16.0")
            ):
                log.info(f"ES database seems not migrated")
                info.missed_es_upgrade = True

            if info.es_connection_error and not info.missed_es_upgrade:
                raise Exception(
                    "Error starting server: failed connecting to ElasticSearch service"
                )

            if not info.missed_es_upgrade:
                init_es_data()
                init_mongo_data()

        if (
            not info.missed_es_upgrade
            and empty_db
            and config.get("apiserver.pre_populate.enabled", False)
        ):
            pre_populate_data()

    def _load_services(self):
        ServiceRepo.load("services")
        log.info(f"Exposed Services: {' '.join(ServiceRepo.endpoint_names())}")

    def _start_worker(self):
        check_updates_thread.start()
        StatisticsReporter.start()

    def _on_worker_stop(self):
        ThreadsManager.terminating = True
