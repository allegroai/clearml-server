import base64
import jwt

from database.errors import translate_errors_context
from database.model.company import Company
from database.utils import get_options
from database.model.auth import User, Entities, Credentials
from apierrors import errors
from config import config
from timing_context import TimingContext

from .payload import Payload, Token, Basic, AuthType
from .identity import Identity


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
        raise errors.unauthorized.InvalidToken('jwt invalid key error', reason=ex.args[0])
    except jwt.InvalidTokenError as ex:
        raise errors.unauthorized.InvalidToken('invalid jwt token', reason=ex.args[0])
    except ValueError as ex:
        log.exception('Failed while processing token: %s' % ex.args[0])
        raise errors.unauthorized.InvalidToken('failed processing token', reason=ex.args[0])


def authorize_credentials(auth_data, service, action, call_data_items):
    """ Validate credentials against service/action and request data (dicts).
        Returns a new basic object (auth payload)
    """
    try:
        access_key, _, secret_key = base64.b64decode(auth_data.encode()).decode('latin-1').partition(':')
    except Exception as e:
        log.exception('malformed credentials')
        raise errors.unauthorized.BadCredentials(str(e))

    with TimingContext("mongo", "user_by_cred"), translate_errors_context('authorizing request'):
        user = User.objects(credentials__match=Credentials(key=access_key, secret=secret_key)).first()

    if not user:
        raise errors.unauthorized.InvalidCredentials('failed to locate provided credentials')

    with TimingContext("mongo", "company_by_id"):
        company = Company.objects(id=user.company).only('id', 'name').first()

    if not company:
        raise errors.unauthorized.InvalidCredentials('invalid user company')

    identity = Identity(user=user.id, company=user.company, role=user.role,
                        user_name=user.name, company_name=company.name)

    basic = Basic(user_key=access_key, identity=identity)

    return basic


def authorize_impersonation(user, identity, service, action, call_data_items):
    """ Returns a new basic object (auth payload)"""
    if not user:
        raise ValueError('missing user')

    company = Company.objects(id=user.company).only('id', 'name').first()
    if not company:
        raise errors.unauthorized.InvalidCredentials('invalid user company')

    return Payload(auth_type=None, identity=identity)
