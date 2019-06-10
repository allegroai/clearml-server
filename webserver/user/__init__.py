from factory import Factory

from .simple import SimpleUser, CreateUserError, AUTH_TOKEN_COOKIE_KEY


class UserFactory(Factory):
    default_cls = SimpleUser
