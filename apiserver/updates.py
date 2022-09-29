import os
from datetime import timedelta, datetime
from threading import Thread
from time import sleep
from typing import Optional

import attr
import requests
from semantic_version import Version

from apiserver.config_repo import config
from apiserver.config.info import get_version
from apiserver.database.model.settings import Settings
from apiserver.redis_manager import redman

log = config.logger(__name__)


class CheckUpdatesThread(Thread):
    _enabled = bool(config.get("apiserver.check_for_updates.enabled", True))
    _lock_name = "check_updates"
    _redis = redman.connection("apiserver")

    @attr.s(auto_attribs=True)
    class _VersionResponse:
        version: str
        patch_upgrade: bool
        description: str = None

    def __init__(self):
        super(CheckUpdatesThread, self).__init__(
            target=self._check_updates, daemon=True
        )

    @property
    def update_interval(self):
        return timedelta(
            seconds=max(
                float(
                    config.get(
                        "apiserver.check_for_updates.check_interval_sec", 60 * 60 * 24,
                    )
                ),
                60 * 5,
            )
        )

    def start(self) -> None:
        if not self._enabled:
            log.info("Checking for updates is disabled")
            return
        super(CheckUpdatesThread, self).start()

    @property
    def component_name(self) -> str:
        return config.get(
            "apiserver.check_for_updates.component_name", "clearml-server"
        )

    def _check_new_version_available(self) -> Optional[_VersionResponse]:
        url = config.get(
            "apiserver.check_for_updates.url", "https://updates.clear.ml/updates",
        )

        uid = Settings.get_by_key("server.uuid")

        response = requests.get(
            url,
            json={"versions": {self.component_name: str(get_version())}, "uid": uid},
            timeout=float(
                config.get("apiserver.check_for_updates.request_timeout_sec", 3.0)
            ),
        )

        if not response.ok:
            return

        response = response.json().get(self.component_name)
        if not response:
            return

        latest_version = response.get("version")
        if not latest_version:
            return

        cur_version = Version(get_version())
        latest_version = Version(latest_version)
        if cur_version >= latest_version:
            return

        return self._VersionResponse(
            version=str(latest_version),
            patch_upgrade=(
                latest_version.major == cur_version.major
                and latest_version.minor == cur_version.minor
            ),
            description=response.get("description").split("\r\n"),
        )

    def _check_updates(self):
        while True:
            # noinspection PyBroadException
            try:
                if self._redis.set(
                    self._lock_name,
                    value=datetime.utcnow().isoformat(),
                    ex=self.update_interval - timedelta(seconds=60),
                    nx=True,
                ):
                    response = self._check_new_version_available()
                    if response:
                        if response.patch_upgrade:
                            log.info(
                                f"{self.component_name.upper()} new package available: upgrade to v{response.version} "
                                f"is recommended!\nRelease Notes:\n{os.linesep.join(response.description)}"
                            )
                        else:
                            log.info(
                                f"{self.component_name.upper()} new version available: upgrade to v{response.version}"
                                f" is recommended!"
                            )
            except Exception as ex:
                log.exception("Failed obtaining updates: " + str(ex))

            sleep(self.update_interval.total_seconds())


check_updates_thread = CheckUpdatesThread()
