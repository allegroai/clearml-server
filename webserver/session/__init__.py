from factory import Factory
from .simple import SimpleSession


class SessionFactory(Factory):
    default_cls = SimpleSession
