from distutils.util import strtobool
from os import getenv
from typing import Optional


def get_env_bool(*keys: str, default: bool = None) -> Optional[bool]:
    try:
        value = next(env for env in (getenv(key) for key in keys) if env is not None)
    except StopIteration:
        return default
    try:
        return bool(strtobool(value))
    except ValueError:
        return bool(value)
