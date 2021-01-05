from flask import request, Response, redirect
from werkzeug.exceptions import BadRequest

from apiserver.apierrors.base import BaseError
from apiserver.config_repo import config
from apiserver.service_repo import ServiceRepo, APICall
from apiserver.service_repo.auth import AuthType
from apiserver.service_repo.errors import PathParsingError
from apiserver.utilities import json

log = config.logger(__file__)


class RequestHandlers:
    _request_strip_prefix = config.get("apiserver.request.strip_prefix", None)

    def before_app_first_request(self):
        pass

    def before_request(self):
        if request.method == "OPTIONS":
            return "", 200
        if "/static/" in request.path:
            return

        try:
            call = self._create_api_call(request)
            content, content_type = ServiceRepo.handle_call(call)

            if call.result.redirect:
                response = redirect(call.result.redirect.url, call.result.redirect.code)
            else:
                headers = None
                if call.result.filename:
                    headers = {
                        "Content-Disposition": f"attachment; filename={call.result.filename}"
                    }

                response = Response(
                    content,
                    mimetype=content_type,
                    status=call.result.code,
                    headers=headers,
                )

            if call.result.cookies:
                for key, value in call.result.cookies.items():
                    kwargs = config.get("apiserver.auth.cookies")
                    if value is None:
                        kwargs = kwargs.copy()
                        kwargs["max_age"] = 0
                        kwargs["expires"] = 0
                        response.set_cookie(key, "", **kwargs)
                    else:
                        response.set_cookie(key, value, **kwargs)

            return response
        except Exception as ex:
            log.exception(f"Failed processing request {request.url}: {ex}")
            return f"Failed processing request {request.url}", 500

    def _update_call_data(self, call, req):
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

    def _call_or_empty_with_error(self, call, req, msg, code=500, subcode=0):
        call = call or APICall(
            "", remote_addr=req.remote_addr, headers=dict(req.headers), files=req.files
        )
        call.set_error_result(msg=msg, code=code, subcode=subcode)
        return call

    def _create_api_call(self, req):
        call = None
        try:
            # Parse the request path
            path = req.path
            if self._request_strip_prefix and path.startswith(
                self._request_strip_prefix
            ):
                path = path[len(self._request_strip_prefix) :]
            endpoint_version, endpoint_name = ServiceRepo.parse_endpoint_path(path)

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
                auth_cookie=auth_cookie,
            )

            # Update call data from request
            self._update_call_data(call, req)

        except PathParsingError as ex:
            call = self._call_or_empty_with_error(call, req, ex.args[0], 400)
            call.log_api = False
        except BadRequest as ex:
            call = self._call_or_empty_with_error(call, req, ex.description, 400)
        except BaseError as ex:
            call = self._call_or_empty_with_error(
                call, req, ex.msg, ex.code, ex.subcode
            )
        except Exception as ex:
            log.exception("Error creating call")
            call = self._call_or_empty_with_error(
                call, req, ex.args[0] if ex.args else type(ex).__name__, 500
            )

        return call
