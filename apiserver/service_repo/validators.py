import fastjsonschema
import jsonmodels.errors

from apiserver.apierrors import errors, APIError
from apiserver.config_repo import config
from apiserver.database.model import Company
from apiserver.database.model.auth import Role, User
from apiserver.service_repo import APICall
from apiserver.service_repo.apicall import MissingIdentity
from apiserver.service_repo.endpoint import Endpoint
from .auth import get_auth_func, Identity, authorize_impersonation, Payload
from .errors import CallParsingError

log = config.logger(__file__)


def validate_data(call: APICall, endpoint: Endpoint):
    """ Perform all required call/endpoint validation, update call result appropriately """
    try:
        # todo: remove vaildate_required_fields once all endpoints have json schema
        validate_required_fields(endpoint, call)

        # set models. models will be validated automatically
        call.schema_validator = endpoint.request_schema_validator
        if endpoint.request_data_model:
            call.data_model_cls = endpoint.request_data_model

        call.result.schema_validator = endpoint.response_schema_validator
        if endpoint.response_data_model:
            call.result.data_model_cls = endpoint.response_data_model

        return True

    except CallParsingError as ex:
        raise errors.bad_request.ValidationError(str(ex))
    except jsonmodels.errors.ValidationError as ex:
        raise errors.bad_request.ValidationError(
            " ".join(map(str.lower, map(str, ex.args)))
        )
    except fastjsonschema.exceptions.JsonSchemaException as ex:
        log.exception(f"{endpoint.name}: fastjsonschema exception")
        raise errors.bad_request.ValidationError(ex.args[0])


def validate_role(endpoint, call):
    try:
        if endpoint.authorize and not endpoint.allows(call.identity.role):
            raise errors.forbidden.RoleNotAllowed(role=call.identity.role, allowed=endpoint.allow_roles)
    except MissingIdentity:
        pass


def validate_auth(endpoint, call):
    """ Validate authorization for this endpoint and call.
        If authentication has occurred, the call is updated with the authentication results.
    """
    if not call.authorization:
        # No auth data. Invalid if we need to authorize and valid otherwise
        if endpoint.authorize:
            raise errors.unauthorized.NoCredentials()
        return

    # prepare arguments for validation
    service, _, action = endpoint.name.partition(".")

    # If we have auth data, we'll try to validate anyway (just so we'll have auth-based permissions whenever possible,
    # even if endpoint did not require authorization)
    try:
        auth = call.authorization or ""
        auth_type, _, auth_data = auth.partition(" ")
        authorize_func = get_auth_func(auth_type)
        call.auth = authorize_func(auth_data, service, action, call)
    except Exception:
        if endpoint.authorize:
            # if endpoint requires authorization, re-raise exception
            raise


def validate_impersonation(endpoint, call):
    """ Validate impersonation headers and set impersonated identity and authorization data accordingly.
        :returns True if impersonating, False otherwise
    """
    try:
        act_as = call.act_as
        impersonate_as = call.impersonate_as
        if not impersonate_as and not act_as:
            return
        elif impersonate_as and act_as:
            raise errors.bad_request.InvalidHeaders(
                "only one allowed", headers=tuple(call.impersonation_headers.keys())
            )

        identity = call.auth.identity

        # verify this user is allowed to impersonate at all
        if identity.role not in Role.get_system_roles() | {Role.admin}:
            raise errors.bad_request.ImpersonationError(
                "impersonation not allowed", role=identity.role
            )

        # get the impersonated user's info
        user_id = act_as or impersonate_as
        if identity.role in [Role.root]:
            # only root is allowed to impersonate users in other companies
            query = dict(id=user_id)
        else:
            query = dict(id=user_id, company=identity.company)
        user = User.objects(**query).first()
        if not user:
            raise errors.bad_request.ImpersonationError("unknown user", **query)

        company = Company.objects(id=user.company).only("name").first()
        if not company:
            query.update(company=user.company)
            raise errors.bad_request.ImpersonationError("unknown company for user", **query)

        # create impersonation payload
        if act_as:
            # act as a user, using your own role and permissions
            call.impersonation = Payload(
                auth_type=None,
                identity=Identity(
                    user=user.id,
                    company=user.company,
                    role=identity.role,
                    user_name=f"{identity.user_name} (acting as {user.name})",
                    company_name=company.name,
                ),
            )
        elif impersonate_as:
            # impersonate as a user, using his own identity and permissions (required additional validation to verify
            # impersonated user is allowed to access the endpoint)
            service, _, action = endpoint.name.partition(".")
            call.impersonation = authorize_impersonation(
                user=user,
                identity=Identity(
                    user=user.id,
                    company=user.company,
                    role=user.role,
                    user_name=f"{user.name} (impersonated by {identity.user_name})",
                    company_name=company.name,
                ),
                service=service,
                action=action,
                call=call,
            )
        else:
            return False
        return True

    except APIError:
        raise
    except Exception as ex:
        log.exception(f"Validating impersonation: {str(ex)}")
        raise errors.server_error.InternalError("validating impersonation")


def validate_required_fields(endpoint, call):
    if endpoint.required_fields is None:
        return

    missing = [val for val in endpoint.required_fields if val not in call.data]
    if missing:
        raise errors.bad_request.MissingRequiredFields(missing=missing)
