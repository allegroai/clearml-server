import os
from threading import Thread
from time import sleep
from typing import Optional

import attr
import requests
from semantic_version import Version

from config import config
from database.model.settings import Settings
from version import __version__ as current_version

log = config.logger(__name__)


class CheckUpdatesThread(Thread):
    _enabled = bool(config.get("apiserver.check_for_updates.enabled", True))

    @attr.s(auto_attribs=True)
    class _VersionResponse:
        version: str
        patch_upgrade: bool
        description: str = None

    def __init__(self):
        super(CheckUpdatesThread, self).__init__(
            target=self._check_updates, daemon=True
        )

    def start(self) -> None:
        if not self._enabled:
            log.info("Checking for updates is disabled")
            return
        super(CheckUpdatesThread, self).start()

    @property
    def component_name(self) -> str:
        return config.get("apiserver.check_for_updates.component_name", "trains-server")

    def _check_new_version_available(self) -> Optional[_VersionResponse]:
        url = config.get(
            "apiserver.check_for_updates.url",
            "https://updates.trains.allegro.ai/updates",
        )

        uid = Settings.get_by_key("server.uuid")

        response = requests.get(
            url,
            json={"versions": {self.component_name: str(current_version)}, "uid": uid},
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

        cur_version = Version(current_version)
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
            except Exception:
                log.exception("Failed obtaining updates")

            sleep(
                max(
                    float(
                        config.get(
                            "apiserver.check_for_updates.check_interval_sec",
                            60 * 60 * 24,
                        )
                    ),
                    60 * 5,
                )
            )


check_updates_thread = CheckUpdatesThread()
