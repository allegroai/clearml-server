import json
import os
import time
from contextlib import contextmanager
from typing import Sequence
from typing import Union, Tuple, Type

import requests
import six
from boltons.iterutils import remap
from boltons.typeutils import issubclass
from requests.adapters import HTTPAdapter, Retry
from requests.auth import HTTPBasicAuth

from apiserver.apierrors.base import BaseError
from apiserver.config_repo import config

log = config.logger(__file__)


class APICallResult:
    def __init__(self, res):
        if isinstance(res, str):
            self._dict = json.loads(res)
        else:
            self._dict = res

        self.data = self._dict["data"]
        self.meta = APICallResultMeta(self._dict["meta"])

    def as_dict(self):
        return self._dict


class APICallResultMeta:
    def __init__(self, meta_dict):
        m = meta_dict
        self._dict = m
        self.call_id = m["id"]
        self.trx_id = m["trx"]
        self.result_code = m["result_code"]
        self.result_msg = m["result_msg"]
        self.result_subcode = m["result_subcode"]
        self.error_stack = m.get("error_stack") or ""
        self.endpoint_name = m["endpoint"]["name"]
        self.endpoint_version = m["endpoint"]["actual_version"]
        self.requested_endpoint_version = m["endpoint"]["requested_version"]
        self.codes = self.result_code, self.result_subcode


def format_duration(duration):
    return "" if duration is None else f" ({int(duration)} sec)"


class APIError(Exception):
    def __init__(self, result: APICallResult, duration=None):
        super().__init__(result)
        self.result = result
        self.duration = duration

    def __str__(self):
        meta = self.result.meta
        message = (
            f"APIClient got {meta.result_code}/{meta.result_subcode} from {meta.endpoint_name}: "
            f"{meta.result_msg}{format_duration(self.duration)}"
        )
        if meta.error_stack:
            header = "\n--- SERVER ERROR {} ---\n"
            formatted_traceback = "\n{}{}{}\n".format(header.format("START"), meta.error_stack, header.format("END"))
            message += formatted_traceback
        return message


class AttrDict(dict):
    """
    ``dict`` which supports attribute lookup syntax.
    Use to implement polymorphism over ``dict``s and database objects, which don't support subscription syntax.
    """

    def __init__(self, dct=None, **kwargs):
        super().__init__(
            remap(
                {**(dct or {}), **kwargs},
                lambda _, key, value: (
                    key,
                    type(self)(value)
                    if isinstance(value, dict) and not isinstance(value, type(self))
                    else value,
                ),
            )
        )

    def __getattr__(self, item):
        return self[item]


class APIClient:
    def __init__(
        self,
        api_key=None,
        secret_key=None,
        base_url=None,
        impersonated_user_id=None,
        session_token=None,
    ):
        if not session_token:
            self.api_key = (
                api_key
                or os.environ.get("SM_API_KEY")
                or config.get("apiclient.api_key")
            )
            if not self.api_key:
                raise ValueError("APIClient requires api_key in constructor or config")

            self.secret_key = (
                secret_key
                or os.environ.get("SM_API_SECRET")
                or config.get("apiclient.secret_key")
            )
            if not self.secret_key:
                raise ValueError(
                    "APIClient requires secret_key in constructor or config"
                )

        self.base_url = (
            base_url or os.environ.get("SM_API_URL") or config.get("apiclient.base_url")
        )
        if not self.base_url:
            raise ValueError("APIClient requires base_url in constructor or config")

        if self.base_url.endswith("/"):
            self.base_url = self.base_url[:-1]

        self.session_token = session_token

        # create http session
        self.http_session = requests.session()
        retries = config.get("apiclient.retries", 7)
        backoff_factor = config.get("apiclient.backoff_factor", 0.3)
        status_forcelist = config.get("apiclient.status_forcelist", (500, 502, 504))
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.http_session.mount("http://", adapter)
        self.http_session.mount("https://", adapter)

        if impersonated_user_id:
            self.http_session.headers["X-ClearML-Impersonate-As"] = impersonated_user_id

        if not self.session_token:
            self.login()

    def login(self):
        res, self.session_token = self.send("auth.login", extract="token")

    def impersonate(self, user_id):
        return type(self)(
            impersonated_user_id=user_id,
            **{
                key: getattr(self, key)
                for key in ("api_key", "secret_key", "base_url", "session_token")
            },
        )

    def send_batch(
        self,
        endpoint,
        items: Sequence[dict],
        headers_overrides=None,
        raise_errors=True,
        log_response=False,
        extract=None,
    ):
        headers_overrides = headers_overrides or {}
        headers_overrides.update({"Content-Type": "application/json-lines"})
        assert isinstance(items, (list, tuple)), type(items)
        json_lines = (json.dumps(item) for item in items)
        data = "\n".join(json_lines)
        return self.send(
            endpoint,
            data=data,
            headers_overrides=headers_overrides,
            raise_errors=raise_errors,
            log_response=log_response,
            extract=extract,
        )

    def send(
        self,
        endpoint,
        data=None,
        headers_overrides=None,
        raise_errors=True,
        log_response=False,
        is_async=False,
        extract=None,
    ):
        if headers_overrides is None:
            headers_overrides = {}
        if data is None:
            data = {}
        headers = {"Content-Type": "application/json"}
        headers.update(headers_overrides)
        if is_async:
            headers["X-ClearML-Async"] = "1"

        if not isinstance(data, six.string_types):
            data = json.dumps(data)

        if not self.session_token:
            auth = HTTPBasicAuth(self.api_key, self.secret_key)
        else:
            auth = None
            headers["Authorization"] = "Bearer %s" % self.session_token

        url = "%s/%s" % (self.base_url, endpoint)
        start = time.time()

        http_res = self.http_session.post(url, headers=headers, data=data, auth=auth)
        if not http_res.text:
            msg = "APIClient got non standard response from %s. http_status=%s " % (
                url,
                http_res.status_code,
            )
            log.error(msg)
            raise Exception(msg)

        res = APICallResult(http_res.text)
        if res.meta.result_code == 202:
            # poll server for async result
            got_result = False
            call_id = res.meta.call_id
            async_res_url = "%s/async.result?id=%s" % (self.base_url, call_id)
            async_res_headers = headers.copy()
            async_res_headers.pop("X-ClearML-Async")
            while not got_result:
                log.info("Got 202. Checking async result for %s (%s)" % (url, call_id))
                http_res = self.http_session.get(
                    async_res_url, headers=async_res_headers
                )
                if http_res.status_code == 202:
                    time.sleep(5)
                else:
                    res = APICallResult(http_res.text)
                    got_result = True

        duration = time.time() - start

        if res.meta.result_code != 200:
            error = APIError(res, duration)
            log.error(error)
            if raise_errors:
                raise error
        else:
            msg = "APIClient got {} from {}{}".format(
                res.meta.result_code, url, format_duration(duration)
            )
            log.info(msg)
            if log_response:
                log.debug(res.as_dict())

        if extract is None:
            return res, res.data
        else:
            return res, res.data.get(extract)

    class Service(object):
        def __init__(self, api, name):
            self.api = api
            self.name = name

        def __getattr__(self, item):
            return lambda **kwargs: AttrDict(
                self.api.send("{}.{}".format(self.name, item), kwargs)[1]
            )

    def __getattr__(self, item):
        return self.Service(self, item)

    @staticmethod
    @contextmanager
    def raises(codes: Union[Type[BaseError], Tuple[int, int]]):
        if issubclass(codes, BaseError):
            codes = codes.codes
        log.info("Expecting failure: %s, %s", *codes)
        try:
            yield
        except APIError as e:
            if e.result.meta.codes != codes:
                raise
        else:
            assert False, "call should fail"
