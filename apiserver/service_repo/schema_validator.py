import sys
from typing import Optional, Callable

import attr
import fastjsonschema
import jsonschema
from boltons.iterutils import remap

from apiserver.apierrors import errors
from apiserver.config_repo import config

log = config.logger(__file__)


@attr.s(auto_attribs=True, cmp=False)
class FastValidationError(Exception):
    error: fastjsonschema.JsonSchemaException
    data: dict


class SchemaValidator:
    def __init__(self, schema: Optional[dict]):
        """
        Utility for different schema validation strategies
        :param schema: jsonschema to validate against
        """
        self.schema = schema
        self.validator: Callable = schema and fastjsonschema.compile(schema)

    @property
    def enabled(self) -> bool:
        return self.schema is not None

    def fast_validate(self, data: dict) -> None:
        """
        Perform a quick validate with laconic error messages
        :param data: data to validate
        :raises: fastjsonschema.JsonSchemaException
        """
        if self.enabled and data is not None:
            data = remap(data, lambda path, key, value: value is not None)
            try:
                self.validator(data)
            except fastjsonschema.JsonSchemaException as e:
                raise FastValidationError(e, data) from e

    def detailed_validate(self, data: dict) -> None:
        """
        Perform a slow validate with detailed error messages
        :param data: data to validate
        :raises: errors.bad_request.ValidationError
        """
        try:
            self.fast_validate(data)
        except FastValidationError as error:
            _, _, traceback = sys.exc_info()
            try:
                jsonschema.validate(error.data, self.schema)
            except jsonschema.exceptions.ValidationError as detailed_error:
                raise errors.bad_request.ValidationError(
                    message=detailed_error.message,
                    path=list(detailed_error.path),
                    context=detailed_error.context,
                    cause=detailed_error.cause,
                    validator=detailed_error.validator,
                    validator_value=detailed_error.validator_value,
                    instance=detailed_error.instance,
                    parent=detailed_error.parent,
                )
            else:
                log.error("fast validation failed while detailed validation succeeded")
                raise error.error.with_traceback(traceback)
