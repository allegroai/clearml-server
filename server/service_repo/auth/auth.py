import base64
from datetime import datetime

import jwt
from mongoengine import Q

from apierrors import errors
from config import config
from database.errors import translate_errors_context
from database.model.auth import User, Entities, Credentials
from database.model.company import Company
from database.utils import get_options
from timing_context import TimingContext
from .fixed_user import FixedUser
from .identity import Identity
from .payload import Payload, Token, Basic, AuthType

log = config.logger(__file__)

entity_keys = set(get_options(Entities))

verify_user_tokens = config.get("apiserver.auth.verify_user_tokens", True)


def get_auth_func(auth_type):
    if auth_type == AuthType.bearer_token:
        return authorize_token
    elif auth_type == AuthType.basic:
        return authorize_credentials
    raise errors.unauthorized.BadAuthType()


def authorize_token(jwt_token, *_, **__):
    """ Validate token against service/endpoint and requests data (dicts).
        Returns a parsed token object (auth payload)
    """
    try:
        return Token.from_encoded_token(jwt_token)

    except jwt.exceptions.InvalidKeyError as ex:
        raise errors.unauthorized.InvalidToken(
            "jwt invalid key error", reason=ex.args[0]
        )
    except jwt.InvalidTokenError as ex:
        raise errors.unauthorized.InvalidToken("invalid jwt token", reason=ex.args[0])
    except ValueError as ex:
        log.exception("Failed while processing token: %s" % ex.args[0])
        raise errors.unauthorized.InvalidToken(
            "failed processing token", reason=ex.args[0]
        )


def authorize_credentials(auth_data, service, action, call_data_items):
    """ Validate credentials against service/action and request data (dicts).
        Returns a new basic object (auth payload)
    """
    try:
        access_key, _, secret_key = base64.b64decode(auth_data.encode()).decode('latin-1').partition(':')
    except Exception as e:
        log.exception('malformed credentials')
        raise errors.unauthorized.BadCredentials(str(e))

    query = Q(credentials__match=Credentials(key=access_key, secret=secret_key))

    if FixedUser.enabled():
        fixed_user = FixedUser.get_by_username(access_key)
        if fixed_user:
            if secret_key != fixed_user.password:
                raise errors.unauthorized.InvalidCredentials('bad username or password')
            query = Q(id=fixed_user.user_id)

    with TimingContext("mongo", "user_by_cred"), translate_errors_context('authorizing request'):
        user = User.objects(query).first()
        if not user:
            raise errors.unauthorized.InvalidCredentials('failed to locate provided credentials')

        if not FixedUser.enabled():
            # In case these are proper credentials, update last used time
            User.objects(id=user.id, credentials__key=access_key).update(
                **{"set__credentials__$__last_used": datetime.utcnow()}
            )

    with TimingContext("mongo", "company_by_id"):
        company = Company.objects(id=user.company).only('id', 'name').first()

    if not company:
        raise errors.unauthorized.InvalidCredentials('invalid user company')

    identity = Identity(user=user.id, company=user.company, role=user.role,
                        user_name=user.name, company_name=company.name)

    basic = Basic(user_key=access_key, identity=identity)

    return basic


def authorize_impersonation(user, identity, service, action, call):
    """ Returns a new basic object (auth payload)"""
    if not user:
        raise ValueError("missing user")

    company = Company.objects(id=user.company).only("id", "name").first()
    if not company:
        raise errors.unauthorized.InvalidCredentials("invalid user company")

    return Payload(auth_type=None, identity=identity)
