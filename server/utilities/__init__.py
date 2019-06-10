import dpath


def strict_map(*args, **kwargs):
    return list(map(*args, **kwargs))


def safe_get(obj, glob, default=None):
    try:
        return dpath.get(obj, glob)
    except KeyError:
        return default
