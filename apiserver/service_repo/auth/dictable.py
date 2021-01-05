class Dictable(object):
    _cached_props = None

    @classmethod
    def _get_cached_props(cls):
        if cls._cached_props is None:
            props = set()
            for c in cls.mro():
                props.update(k for k, v in vars(c).items() if isinstance(v, property) and not k.startswith('_'))
            cls._cached_props = list(props)
        return cls._cached_props

    def to_dict(self, **extra):
        props = self._get_cached_props()
        d = {k: getattr(self, k) for k in props if getattr(self, k)}
        res = {k: (v.to_dict() if isinstance(v, Dictable) else v) for k, v in d.items()}
        if extra:
            # add the extra items to our result, make sure not to overwrite existing properties (claims etc)
            res.update({k: v for k, v in extra.items() if k not in props})
        return res

    @classmethod
    def from_dict(cls, d):
        return cls(**d)
