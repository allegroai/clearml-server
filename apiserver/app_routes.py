from flask import Flask, request, Response

from werkzeug.exceptions import BadRequest

from apiserver.apierrors.base import BaseError
from apiserver.config import config
from apiserver.service_repo import ServiceRepo, APICall
from apiserver.service_repo.auth import AuthType
from apiserver.service_repo.errors import PathParsingError
from apiserver.timing_context import TimingContext
from apiserver.utilities import json

log = config.logger(__file__)


def before_app_first_request():
    pass


def before_request():
    if request.method == "OPTIONS":
        return "", 200
    if "/static/" in request.path:
        return
    if request.path.startswith("/rq"):
        return

    try:
        call = create_api_call(request)
        content, content_type = ServiceRepo.handle_call(call)
        headers = {}
        if call.result.filename:
            headers[
                "Content-Disposition"
            ] = f"attachment; filename={call.result.filename}"

        if call.result.headers:
            headers.update(call.result.headers)

        response = Response(
            content, mimetype=content_type, status=call.result.code, headers=headers
        )

        if call.result.cookies:
            for key, value in call.result.cookies.items():
                if value is None:
                    response.set_cookie(
                        key, "", expires=0, **config.get("apiserver.auth.cookies")
                    )
                else:
                    response.set_cookie(
                        key, value, **config.get("apiserver.auth.cookies")
                    )

        return response
    except Exception as ex:
        log.exception(f"Failed processing request {request.url}: {ex}")
        return f"Failed processing request {request.url}", 500


def update_call_data(call, req):
    """ Use request payload/form to fill call data or batched data """
    if req.content_type == "application/json-lines":
        items = []
        for i, line in enumerate(req.data.splitlines()):
            try:
                event = json.loads(line)
                if not isinstance(event, dict):
                    raise BadRequest(
                        f"json lines must contain objects, found: {type(event).__name__}"
                    )
                items.append(event)
            except ValueError as e:
                msg = f"{e} in batch item #{i}"
                req.on_json_loading_failed(msg)
        call.batched_data = items
    else:
        json_body = req.get_json(force=True, silent=False) if req.data else None
        # merge form and args
        form = req.form.copy()
        form.update(req.args)
        form = form.to_dict()
        # convert string numbers to floats
        for key in form:
            if form[key].replace(".", "", 1).isdigit():
                if "." in form[key]:
                    form[key] = float(form[key])
                else:
                    form[key] = int(form[key])
            elif form[key].lower() == "true":
                form[key] = True
            elif form[key].lower() == "false":
                form[key] = False
        call.data = json_body or form or {}


def _call_or_empty_with_error(call, req, msg, code=500, subcode=0):
    call = call or APICall(
        "", remote_addr=req.remote_addr, headers=dict(req.headers), files=req.files
    )
    call.set_error_result(msg=msg, code=code, subcode=subcode)
    return call


def create_api_call(req):
    call = None
    try:
        # Parse the request path
        endpoint_version, endpoint_name = ServiceRepo.parse_endpoint_path(req.path)

        # Resolve authorization: if cookies contain an authorization token, use it as a starting point.
        # in any case, request headers always take precedence.
        auth_cookie = req.cookies.get(
            config.get("apiserver.auth.session_auth_cookie_name")
        )
        headers = (
            {}
            if not auth_cookie
            else {"Authorization": f"{AuthType.bearer_token} {auth_cookie}"}
        )
        headers.update(
            list(req.headers.items())
        )  # add (possibly override with) the headers

        # Construct call instance
        call = APICall(
            endpoint_name=endpoint_name,
            remote_addr=req.remote_addr,
            endpoint_version=endpoint_version,
            headers=headers,
            files=req.files,
            host=req.host,
        )

        # Update call data from request
        with TimingContext("preprocess", "update_call_data"):
            update_call_data(call, req)

    except PathParsingError as ex:
        call = _call_or_empty_with_error(call, req, ex.args[0], 400)
        call.log_api = False
    except BadRequest as ex:
        call = _call_or_empty_with_error(call, req, ex.description, 400)
    except BaseError as ex:
        call = _call_or_empty_with_error(call, req, ex.msg, ex.code, ex.subcode)
    except Exception as ex:
        log.exception("Error creating call")
        call = _call_or_empty_with_error(call, req, ex.args[0] if ex.args else type(ex).__name__, 500)

    return call


def register_routes(app: Flask):
    app.before_first_request(before_app_first_request)
    app.before_request(before_request)
