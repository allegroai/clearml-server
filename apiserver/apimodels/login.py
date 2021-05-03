from jsonmodels.fields import StringField, BoolField, EmbeddedField, ListField
from jsonmodels.models import Base

from apiserver.apimodels import DictField, callable_default


class GetSupportedModesRequest(Base):
    state = StringField(help_text="ASCII base64 encoded application state")
    callback_url_prefix = StringField()


class BasicGuestMode(Base):
    enabled = BoolField(default=False)
    name = StringField()
    username = StringField()
    password = StringField()


class BasicMode(Base):
    enabled = BoolField(default=False)
    guest = callable_default(EmbeddedField)(BasicGuestMode, default=BasicGuestMode)


class ServerErrors(Base):
    missed_es_upgrade = BoolField(default=False)
    es_connection_error = BoolField(default=False)


class GetSupportedModesResponse(Base):
    basic = EmbeddedField(BasicMode)
    server_errors = EmbeddedField(ServerErrors)
    sso = DictField([str, type(None)])
    sso_providers = ListField([dict])
    authenticated = BoolField(default=False)
