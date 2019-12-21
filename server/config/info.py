from functools import lru_cache
from os import getenv
from pathlib import Path
from version import __version__

root = Path(__file__).parent.parent


def _get(prop_name, env_suffix=None, default=""):
    value = getenv(f"TRAINS_SERVER_{env_suffix or prop_name}")
    if value:
        return value

    try:
        return (root / prop_name).read_text().strip()
    except FileNotFoundError:
        return default


@lru_cache()
def get_build_number():
    return _get("BUILD")


@lru_cache()
def get_version():
    return _get("VERSION", default=__version__)


@lru_cache()
def get_commit_number():
    return _get("COMMIT")


@lru_cache()
def get_deployment_type() -> str:
    return _get("DEPLOY", env_suffix="DEPLOYMENT_TYPE", default="manual")
