import functools


class Factory:
    default_cls = None
    registered_cls = None

    @classmethod
    @functools.lru_cache(maxsize=None)
    def get(cls, *args, **kwargs):
        return cls.get_class()(*args, **kwargs)

    @classmethod
    @functools.lru_cache(maxsize=None)
    def get_class(cls):
        return cls.registered_cls or cls.default_cls

    @classmethod
    def register(cls, registered_cls):
        cls.registered_cls = registered_cls
