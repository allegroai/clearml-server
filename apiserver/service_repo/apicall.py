import time
import types
from traceback import format_exc
from typing import Type, Optional

from jsonmodels import models
from six import string_types

import database
from timing_context import TimingContext, TimingStats
from utilities import json
from .auth import Identity
from .auth import Payload as AuthPayload
from .base import PartialVersion
from .errors import CallParsingError
from .schema_validator import SchemaValidator

JSON_CONTENT_TYPE = "application/json"


class DataContainer(object):
    """ Data container that supports raw data (dict or a list of batched dicts) and a data model """

    null_schema_validator: SchemaValidator = SchemaValidator(None)

    def __init__(self, data=None, batched_data=None):
        if data and batched_data:
            raise ValueError("data and batched data are not supported simultaneously")
        self._batched_data = None
        self._data = None
        self._data_model = None
        self._data_model_cls = None
        self._schema_validator: SchemaValidator = self.null_schema_validator
        # use setter to properly initialize data
        self.data = data
        self.batched_data = batched_data
        self._raw_data = None
        self._content_type = JSON_CONTENT_TYPE

    @property
    def schema_validator(self):
        return self._schema_validator

    @schema_validator.setter
    def schema_validator(self, value):
        self._schema_validator = value
        self._update_data_model()

    @property
    def data(self):
        return self._data or {}

    @data.setter
    def data(self, value):
        """ Set the data using a raw dict. If a model cls is defined, validate the raw data """
        if value is not None:
            assert isinstance(value, dict), "Data should be a dict"
        self._data = value
        self._update_data_model()

    @property
    def batched_data(self):
        if self._batched_data is not None:
            return self._batched_data
        elif self.data != {}:
            return [self.data]
        else:
            return []

    @batched_data.setter
    def batched_data(self, value):
        if not value:
            return
        assert isinstance(value, (tuple, list)), "Batched data should be a list"
        self._batched_data = value
        self._update_data_model()

    @property
    def raw_data(self):
        return self._raw_data

    @raw_data.setter
    def raw_data(self, value):
        assert isinstance(
            value, string_types + (types.GeneratorType,)
        ), "Raw data must be a string type or generator"
        self._raw_data = value

    @property
    def content_type(self):
        return self._content_type

    @content_type.setter
    def content_type(self, value):
        self._content_type = value

    def _update_data_model(self):
        self.schema_validator.detailed_validate(self._data)

        cls = self.data_model_cls
        if not cls or (self._data is None and self._batched_data is None):
            return

        # handle batched items
        if self._batched_data:
            try:
                data_model = [cls(**item) for item in self._batched_data]
            except (ValueError, TypeError) as ex:
                raise CallParsingError(str(ex))

            for m in data_model:
                m.validate()
        else:
            try:
                data_model = cls(**self.data)
            except (ValueError, TypeError) as ex:
                raise CallParsingError(str(ex))

            if not self.schema_validator.enabled:
                data_model.validate()
        self._data_model = data_model

    @property
    def data_model(self):
        return self._data_model

    # @property
    # def get_partial_update(self, data_model_class):
    #     return {k: v for k, v in self.data_model.to_struct().iteritems() if k in self.data}
    @property
    def data_model_for_partial_update(self):
        """
        Return only data model fields that we actually passed by the user
        :return:
        """
        return {k: v for k, v in self.data_model.to_struct().items() if k in self.data}

    @data_model.setter
    def data_model(self, value):
        """ Set the data using a model instance. NOTE: batched_data is never updated. """
        cls = self.data_model_cls
        if not cls:
            raise ValueError("Data model is not defined")
        if isinstance(value, cls):
            # instance of the data model class - just take it
            self._data_model = value
        elif issubclass(cls, type(value)):
            # instance of a subclass of the data model class - create the expected class instance and use the instance
            # we received to initialize it
            self._data_model = cls(**value.to_struct())
        else:
            raise ValueError(f"Invalid data model (expecting {cls} or super classes)")
        self._data = value.to_struct()

    @property
    def data_model_cls(self) -> Optional[Type[models.Base]]:
        return self._data_model_cls

    @data_model_cls.setter
    def data_model_cls(self, value: Type[models.Base]):
        assert issubclass(value, models.Base)
        self._data_model_cls = value
        self._update_data_model()


class APICallResult(DataContainer):
    def __init__(self, data=None, code=200, subcode=0, msg="OK", traceback=""):
        super(APICallResult, self).__init__(data)
        self._code = code
        self._subcode = subcode
        self._msg = msg
        self._traceback = traceback
        self._extra = None
        self._filename = None
        self._headers = {}
        self._cookies = {}

    def get_log_entry(self):
        res = dict(
            msg=self.msg,
            code=self.code,
            subcode=self.subcode,
            traceback=self._traceback,
            extra=self._extra,
        )
        return res

    def copy_from(self, result):
        self._code = result.code
        self._subcode = result.subcode
        self._msg = result.code
        self._traceback = result.traceback
        self._extra = result.extra_log

    @property
    def msg(self):
        return self._msg

    @msg.setter
    def msg(self, value):
        self._msg = value

    @property
    def code(self):
        return self._code

    @code.setter
    def code(self, value):
        self._code = value

    @property
    def subcode(self):
        return self._subcode

    @subcode.setter
    def subcode(self, value):
        self._subcode = value

    @property
    def traceback(self):
        return self._traceback

    @traceback.setter
    def traceback(self, value):
        self._traceback = value

    @property
    def extra_log(self):
        """ Extra data to be logged into ES """
        return self._extra

    @extra_log.setter
    def extra_log(self, value):
        self._extra = value

    @property
    def filename(self):
        return self._filename

    @filename.setter
    def filename(self, value):
        self._filename = value

    @property
    def headers(self):
        return self._headers

    @property
    def cookies(self):
        return self._cookies


class MissingIdentity(Exception):
    pass


class APICall(DataContainer):
    HEADER_AUTHORIZATION = "Authorization"
    HEADER_REAL_IP = "X-Real-IP"
    HEADER_FORWARDED_FOR = "X-Forwarded-For"
    """ Standard headers """

    _transaction_headers = ("X-Trains-Trx",)
    """ Transaction ID """

    @property
    def HEADER_TRANSACTION(self):
        return self._transaction_headers[0]

    _worker_headers = ("X-Trains-Worker",)
    """ Worker (machine) ID """

    @property
    def HEADER_WORKER(self):
        return self._worker_headers[0]

    _impersonate_as_headers = ("X-Trains-Impersonate-As",)
    """ Impersonate as someone else (using his identity and permissions) """

    @property
    def HEADER_IMPERSONATE_AS(self):
        return self._impersonate_as_headers[0]

    _act_as_headers = ("X-Trains-Act-As",)
    """ Act as someone else (using his identity, but with your own role and permissions) """

    @property
    def HEADER_ACT_AS(self):
        return self._act_as_headers[0]

    _async_headers = ("X-Trains-Async",)
    """ Specifies that this call should be done asynchronously """

    @property
    def HEADER_ASYNC(self):
        return self._async_headers[0]

    def __init__(
        self,
        endpoint_name,
        remote_addr=None,
        endpoint_version: PartialVersion = PartialVersion("1.0"),
        data=None,
        batched_data=None,
        headers=None,
        files=None,
        trx=None,
    ):
        super(APICall, self).__init__(data=data, batched_data=batched_data)

        self._id = database.utils.id()
        self._files = files  # currently dic of key to flask's FileStorage)
        self._start_ts = time.time()
        self._end_ts = 0
        self._duration = 0
        self._endpoint_name = endpoint_name
        self._remote_addr = remote_addr
        assert isinstance(endpoint_version, PartialVersion), endpoint_version
        self._requested_endpoint_version = endpoint_version
        self._actual_endpoint_version = None
        self._headers = {}
        self._kpis = {}
        self._log_api = True
        if headers:
            self._headers.update(headers)
        self._result = APICallResult()
        self._auth = None
        self._impersonation = None
        if trx:
            self.set_header(self._transaction_headers, trx)
        self._requires_authorization = True

    @property
    def id(self):
        return self._id

    @property
    def requires_authorization(self):
        return self._requires_authorization

    @requires_authorization.setter
    def requires_authorization(self, value):
        self._requires_authorization = value

    @property
    def log_api(self):
        return self._log_api

    @log_api.setter
    def log_api(self, value):
        self._log_api = value

    def assign_new_id(self):
        self._id = database.utils.id()

    def get_header(self, header, default=None):
        """
        Get header value
        :param header: Header name options (more than on supported, listed by priority)
        :param default: Default value if no such headers were found
        """
        for option in header if isinstance(header, (tuple, list)) else (header,):
            if option in self._headers:
                return self._headers[option]
        return default

    def clear_header(self, header):
        """
        Clear header value
        :param header: Header name options (more than on supported, all will be cleared)
        """
        for value in header if isinstance(header, (tuple, list)) else (header,):
            self.headers.pop(value, None)

    def set_header(self, header, value):
        """
        Set header value
        :param header: header name (if a list is provided, first item is used)
        :param value: Value to set
        :return:
        """
        self._headers[
            header[0] if isinstance(header, (tuple, list)) else header
        ] = value

    @property
    def real_ip(self):
        """ Obtain visitor's IP address """
        return (
                self.get_header(self.HEADER_FORWARDED_FOR)
                or self.get_header(self.HEADER_REAL_IP)
                or self._remote_addr
                or "untrackable"
        )

    @property
    def failed(self):
        return self.result and self.result.code != 200

    @property
    def duration(self):
        return self._duration

    @property
    def endpoint_name(self):
        return self._endpoint_name

    @property
    def requested_endpoint_version(self) -> PartialVersion:
        return self._requested_endpoint_version

    @property
    def auth(self):
        """ Authenticated payload (Token or Basic) """
        return self._auth

    @auth.setter
    def auth(self, value):
        if value:
            assert isinstance(value, AuthPayload)
        self._auth = value

    @property
    def impersonation_headers(self):
        return {
            k: v
            for k, v in self._headers.items()
            if k in (self._impersonate_as_headers + self._act_as_headers)
        }

    @property
    def impersonate_as(self):
        return self.get_header(self._impersonate_as_headers)

    @property
    def act_as(self):
        return self.get_header(self._act_as_headers)

    @property
    def impersonation(self):
        return self._impersonation

    @impersonation.setter
    def impersonation(self, value):
        if value:
            assert isinstance(value, AuthPayload)
        self._impersonation = value

    @property
    def identity(self) -> Identity:
        if self.impersonation:
            if not self.impersonation.identity:
                raise Exception("Missing impersonate identity")
            return self.impersonation.identity
        if self.auth:
            if not self.auth.identity:
                raise Exception("Missing authorized identity (not authorized?)")
            return self.auth.identity
        raise MissingIdentity("Missing identity")

    @property
    def actual_endpoint_version(self):
        return self._actual_endpoint_version

    @actual_endpoint_version.setter
    def actual_endpoint_version(self, value):
        self._actual_endpoint_version = value

    @property
    def headers(self):
        return self._headers

    @property
    def kpis(self):
        """
        Key Performance Indicators, holding things like number of returned frames/rois, etc.
        :return:
        """
        return self._kpis

    @property
    def trx(self):
        return self.get_header(self._transaction_headers, self.id)

    @trx.setter
    def trx(self, value):
        self.set_header(self._transaction_headers, value)

    @property
    def worker(self):
        return self.get_header(self._worker_headers, "<unknown>")

    @property
    def authorization(self):
        """ Call authorization data used to authenticate the call """
        return self.get_header(self.HEADER_AUTHORIZATION)

    @property
    def result(self):
        return self._result

    @property
    def exec_async(self):
        return self.get_header(self._async_headers) is not None

    @exec_async.setter
    def exec_async(self, value):
        if value:
            self.set_header(self._async_headers, "1")
        else:
            self.clear_header(self._async_headers)

    def mark_end(self):
        self._end_ts = time.time()
        self._duration = int((self._end_ts - self._start_ts) * 1000)
        self.stats = TimingStats.aggregate()

    def get_response(self):
        def make_version_number(version):
            """
            Client versions <=2.0 expect expect endpoint versions in float format, otherwise throwing an exception
            """
            if version is None:
                return None
            if self.requested_endpoint_version < PartialVersion("2.1"):
                return float(str(version))
            return str(version)

        if self.result.raw_data and not self.failed:
            # endpoint returned raw data and no error was detected, return raw data, no fancy dicts
            return self.result.raw_data, self.result.content_type

        else:
            res = {
                "meta": {
                    "id": self.id,
                    "trx": self.trx,
                    "endpoint": {
                        "name": self.endpoint_name,
                        "requested_version": make_version_number(
                            self.requested_endpoint_version
                        ),
                        "actual_version": make_version_number(
                            self.actual_endpoint_version
                        ),
                    },
                    "result_code": self.result.code,
                    "result_subcode": self.result.subcode,
                    "result_msg": self.result.msg,
                    "error_stack": self.result.traceback,
                },
                "data": self.result.data,
            }
            if self.content_type.lower() == JSON_CONTENT_TYPE:
                with TimingContext("json", "serialization"):
                    try:
                        res = json.dumps(res)
                    except Exception as ex:
                        # JSON serialization may fail, probably problem with data so pop it and try again
                        if not self.result.data:
                            raise
                        self.result.data = None
                        msg = "Error serializing response data: " + str(ex)
                        self.set_error_result(
                            code=500, subcode=0, msg=msg, include_stack=False
                        )
                        return self.get_response()

            return res, self.content_type

    def set_error_result(self, msg, code=500, subcode=0, include_stack=False):
        tb = format_exc() if include_stack else None
        self._result = APICallResult(
            data=self._result.data, code=code, subcode=subcode, msg=msg, traceback=tb
        )
