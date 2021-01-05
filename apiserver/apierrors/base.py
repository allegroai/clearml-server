import six
from boltons.typeutils import classproperty
from typing import Tuple

from .apierror import APIError


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
        params = kwargs.copy()
        params.update(
            code=self._default_code, subcode=self._default_subcode, msg=message
        )
        super(BaseError, self).__init__(**params)

    @staticmethod
    def _format_kwarg(value):
        if isinstance(value, (tuple, list)):
            return f'({", ".join(str(v) for v in value)})'
        elif isinstance(value, six.string_types):
            return value
        return str(value)

    @classproperty
    def codes(self) -> Tuple[int, int]:
        return self._default_code, self._default_subcode
