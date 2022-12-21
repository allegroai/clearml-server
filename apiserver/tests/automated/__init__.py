import abc
import sys
from datetime import datetime, timezone
from functools import partial
from typing import Iterable
from unittest import TestCase

from packaging.version import parse

from apiserver.tests.api_client import APIClient
from apiserver.config_repo import config

log = config.logger(__file__)


class TestServiceInterface(metaclass=abc.ABCMeta):
    api = abc.abstractproperty()

    @abc.abstractmethod
    def defer(self, func, *args, can_fail=False, **kwargs):
        pass


class TestService(TestCase, TestServiceInterface):
    @property
    def api(self):
        return self._api

    @api.setter
    def api(self, value):
        self._api = value

    def defer(self, func, *args, can_fail=False, **kwargs):
        self._deferred.append((can_fail, partial(func, *args, **kwargs)))

    def _create_temp_helper(
        self,
        service,
        object_name,
        create_endpoint,
        delete_endpoint,
        create_params,
        *,
        client=None,
        delete_params=None,
    ):
        client = client or self.api
        res, data = client.send(f"{service}.{create_endpoint}", create_params)

        object_id = data["id"]
        self.defer(
            client.send,
            f"{service}.{delete_endpoint}",
            can_fail=True,
            data={object_name: object_id, "force": True, **(delete_params or {})},
        )
        return object_id

    @staticmethod
    def update_missing(target: dict, **update):
        target.update({k: v for k, v in update.items() if k not in target})

    def create_temp(self, service, *, client=None, delete_params=None, object_name="", **kwargs) -> str:
        return self._create_temp_helper(
            service=service,
            create_endpoint="create",
            delete_endpoint="delete",
            object_name=object_name or service.rstrip("s"),
            create_params=kwargs,
            client=client,
            delete_params=delete_params,
        )

    def setUp(self, version="999.0"):
        self._api = APIClient(base_url=f"http://localhost:8008/v{version}")
        self._deferred = []
        self._version = parse(version)
        header(self.id())

    def tearDown(self):
        log.info("Cleanup...")
        for can_fail, func in reversed(self._deferred):
            try:
                func()
            except Exception as ex:
                if not can_fail:
                    log.exception(ex)
        self._deferred = []

    def assertEqualNoOrder(self, first: Iterable, second: Iterable):
        """Compares 2 sequences regardless of their items order"""
        self.assertEqual(set(first), set(second))


def header(info, title="=" * 20):
    print(title, info, title, file=sys.stderr)


def utc_now_tz_aware() -> datetime:
    """
    Returns utc now with the utc time zone.
    Suitable for subsequent usage with functions that
    make use of tz info like 'timestamp'
    """
    return datetime.now(timezone.utc)
