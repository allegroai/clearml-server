from typing import Tuple

import six
from boltons.iterutils import is_collection, remap
from boltons.typeutils import classproperty

from .apierror import APIError

jsonable_types = (dict, list, tuple, str, int, float, bool, type(None))


class BaseError(APIError):
    _default_code = 500
    _default_subcode = 0
    _default_msg = ""

    def __init__(self, extra_msg=None, replacement_msg=None, **kwargs):
        message = replacement_msg or self._default_msg
        if extra_msg:
            message += f" ({extra_msg})"
        if kwargs:
            kwargs_msg = ", ".join(
                f"{k}={self._format_kwarg(v)}" for k, v in kwargs.items()
            )
            message += f": {kwargs_msg}"

        super(BaseError, self).__init__(
            code=self._default_code,
            subcode=self._default_subcode,
            msg=message,
            error_data=self._to_safe_json_types(kwargs),
        )

    @staticmethod
    def _to_safe_json_types(data):
        def visit(_, k, v):
            if not isinstance(v, jsonable_types):
                v = str(v)
            return k, v

        return remap(data, visit=visit)

    @staticmethod
    def _format_kwarg(value):
        if is_collection(value):
            return f'({", ".join(str(v) for v in value)})'
        elif isinstance(value, six.string_types):
            return value
        return str(value)

    @classproperty
    def codes(self) -> Tuple[int, int]:
        return self._default_code, self._default_subcode
