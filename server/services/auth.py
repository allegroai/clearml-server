from apierrors import errors
from apimodels.auth import (
    GetTokenResponse,
    GetTokenForUserRequest,
    GetTokenRequest,
    ValidateTokenRequest,
    ValidateResponse,
    CreateUserRequest,
    CreateUserResponse,
    CreateCredentialsResponse,
    GetCredentialsResponse,
    RevokeCredentialsResponse,
    CredentialsResponse,
    RevokeCredentialsRequest,
    EditUserReq,
)
from apimodels.base import UpdateResponse
from bll.auth import AuthBLL
from config import config
from database.errors import translate_errors_context
from database.model.auth import (
    User,
)
from service_repo import APICall, endpoint
from service_repo.auth import Token

log = config.logger(__file__)


@endpoint(
    "auth.login",
    request_data_model=GetTokenRequest,
    response_data_model=GetTokenResponse,
)
def login(call):
    """ Generates a token based on the authenticated user (intended for use with credentials) """
    assert isinstance(call, APICall)

    call.result.data_model = AuthBLL.get_token_for_user(
        user_id=call.identity.user,
        company_id=call.identity.company,
        expiration_sec=call.data_model.expiration_sec,
    )


@endpoint(
    "auth.get_token_for_user",
    request_data_model=GetTokenForUserRequest,
    response_data_model=GetTokenResponse,
)
def get_token_for_user(call):
    """ Generates a token based on a requested user and company. INTERNAL. """
    assert isinstance(call, APICall)
    call.result.data_model = AuthBLL.get_token_for_user(
        user_id=call.data_model.user,
        company_id=call.data_model.company,
        expiration_sec=call.data_model.expiration_sec,
    )


@endpoint(
    "auth.validate_token",
    request_data_model=ValidateTokenRequest,
    response_data_model=ValidateResponse,
)
def validate_token_endpoint(call):
    """ Validate a token and return identity if valid. INTERNAL. """
    assert isinstance(call, APICall)

    try:
        # if invalid, decoding will fail
        token = Token.from_encoded_token(call.data_model.token)
        call.result.data_model = ValidateResponse(
            valid=True, user=token.identity.user, company=token.identity.company
        )
    except Exception as e:
        call.result.data_model = ValidateResponse(valid=False, msg=e.args[0])


@endpoint(
    "auth.create_user",
    request_data_model=CreateUserRequest,
    response_data_model=CreateUserResponse,
)
def create_user(call: APICall, _, request: CreateUserRequest):
    """ Create a user from. INTERNAL. """
    user_id = AuthBLL.create_user(request=request, call=call)
    call.result.data_model = CreateUserResponse(id=user_id)


@endpoint("auth.create_credentials", response_data_model=CreateCredentialsResponse)
def create_credentials(call: APICall, _, __):
    credentials = AuthBLL.create_credentials(
        user_id=call.identity.user,
        company_id=call.identity.company,
        role=call.identity.role,
    )
    call.result.data_model = CreateCredentialsResponse(credentials=credentials)


@endpoint(
    "auth.revoke_credentials",
    request_data_model=RevokeCredentialsRequest,
    response_data_model=RevokeCredentialsResponse,
)
def revoke_credentials(call):
    assert isinstance(call, APICall)
    identity = call.identity
    access_key = call.data_model.access_key

    with translate_errors_context():
        query = dict(
            id=identity.user, company=identity.company, credentials__key=access_key
        )
        updated = User.objects(**query).update_one(pull__credentials__key=access_key)
        if not updated:
            raise errors.bad_request.InvalidUser(
                "invalid user or invalid access key", **query
            )

        call.result.data_model = RevokeCredentialsResponse(revoked=updated)


@endpoint("auth.get_credentials", response_data_model=GetCredentialsResponse)
def get_credentials(call):
    """ Validate a user by his email. INTERNAL. """
    assert isinstance(call, APICall)
    identity = call.identity

    with translate_errors_context():
        query = dict(id=identity.user, company=identity.company)
        user = User.objects(**query).first()
        if not user:
            raise errors.bad_request.InvalidUserId(**query)

        # we return ONLY the key IDs, never the secrets (want a secret? create new credentials)
        call.result.data_model = GetCredentialsResponse(
            credentials=[
                CredentialsResponse(access_key=c.key) for c in user.credentials
            ]
        )


@endpoint(
    "auth.edit_user", request_data_model=EditUserReq, response_data_model=UpdateResponse
)
def update(call, company_id, _):
    assert isinstance(call, APICall)

    fields = {
        k: v
        for k, v in call.data_model.to_struct().items()
        if k != "user" and v is not None
    }

    with translate_errors_context():
        result = User.objects(company=company_id, id=call.data_model.user).update(
            **fields, full_result=True, upsert=False
        )

        if not result.matched_count:
            raise errors.bad_request.InvalidUserId()

    call.result.data_model = UpdateResponse(
        updated=result.modified_count, fields=fields
    )
