from jsonmodels.fields import IntField, StringField, BoolField, EmbeddedField, DateTimeField
from jsonmodels.models import Base
from jsonmodels.validators import Max, Enum

from apiserver.apimodels import ListField, EnumField
from apiserver.config_repo import config
from apiserver.database.model.auth import Role
from apiserver.database.utils import get_options


class GetTokenRequest(Base):
    """ User requests a token """

    expiration_sec = IntField(
        validators=Max(config.get("apiserver.auth.max_expiration_sec")), nullable=True
    )
    """ Expiration time for token in seconds. """


class GetTaskTokenRequest(GetTokenRequest):
    """ User requests a task token """

    task = StringField(required=True)


class GetTokenForUserRequest(GetTokenRequest):
    """ System requests a token for a user """

    user = StringField(required=True)
    company = StringField()


class GetTaskTokenForUserRequest(GetTokenForUserRequest):
    """ System requests a token for a user, for a specific task """

    task = StringField(required=True)


class GetTokenResponse(Base):
    token = StringField(required=True)


class ValidateTokenRequest(Base):
    token = StringField(required=True)


class ValidateUserRequest(Base):
    email = StringField(required=True)


class ValidateResponse(Base):
    valid = BoolField(required=True)
    msg = StringField()
    user = StringField()
    company = StringField()


class CreateUserRequest(Base):
    name = StringField(required=True)
    company = StringField(required=True)
    role = StringField(
        validators=Enum(*(set(get_options(Role)))),
        default=Role.user,
    )
    email = StringField(required=True)
    family_name = StringField()
    given_name = StringField()
    avatar = StringField()


class CreateUserResponse(Base):
    id = StringField(required=True)


class Credentials(Base):
    access_key = StringField(required=True)
    secret_key = StringField(required=True)
    label = StringField()


class CredentialsResponse(Credentials):
    secret_key = StringField()
    last_used = DateTimeField(default=None)
    last_used_from = StringField()


class CreateCredentialsRequest(Base):
    label = StringField()


class CreateCredentialsResponse(Base):
    credentials = EmbeddedField(Credentials)


class GetCredentialsResponse(Base):
    credentials = ListField(CredentialsResponse)


class EditCredentialsRequest(Base):
    access_key = StringField(required=True)
    label = StringField()


class RevokeCredentialsRequest(Base):
    access_key = StringField(required=True)


class RevokeCredentialsResponse(Base):
    revoked = IntField(required=True)


class AddUserRequest(CreateUserRequest):
    company = StringField()
    secret_key = StringField()


class AddUserResponse(CreateUserResponse):
    secret = StringField()


class DeleteUserRequest(Base):
    user = StringField(required=True)
    company = StringField()


class EditUserReq(Base):
    user = StringField(required=True)
    role = EnumField(Role.get_company_roles())
