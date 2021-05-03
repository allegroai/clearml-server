from typing import Text, Sequence, Callable, Union, Type

from inspect import signature
from jsonmodels import models

from .apicall import APICall, APICallResult
from .endpoint import EndpointFunc, Endpoint
from .service_repo import ServiceRepo


__all__ = ["ServiceRepo", "APICall", "endpoint"]


LegacyEndpointFunc = Callable[[APICall], None]


def endpoint(
    name: Text,
    min_version: Text = "1.0",
    required_fields: Sequence[Text] = None,
    request_data_model: Type[models.Base] = None,
    response_data_model: Type[models.Base] = None,
    validate_schema=False,
):
    """ Endpoint decorator, used to declare a method as an endpoint handler """

    def decorator(f: Union[EndpointFunc, LegacyEndpointFunc]) -> EndpointFunc:
        # Backwards compatibility: support endpoints with both old-style signature (call) and new-style signature
        #  (call, company, request_model)
        func = f
        sig = signature(f)
        nonlocal request_data_model
        if len(sig.parameters) == 3 and request_data_model is None:
            last_arg = list(sig.parameters.items())[-1][1]
            if last_arg.annotation != last_arg.empty:
                request_data_model = last_arg.annotation
        elif len(sig.parameters) == 1:
            # old-style
            def adapter(call, *_, **__):
                return f(call)

            func = adapter

        ServiceRepo.register(
            Endpoint(
                name=name,
                func=func,
                min_version=min_version,
                required_fields=required_fields,
                request_data_model=request_data_model,
                response_data_model=response_data_model,
                validate_schema=validate_schema,
            )
        )

        return func

    return decorator
