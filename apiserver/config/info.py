from functools import lru_cache
from os import getenv
from pathlib import Path

from boltons.iterutils import first

from apiserver.config_repo import config
from apiserver.version import __version__

root = Path(__file__).parent.parent


def _get(prop_name, env_suffix=None, default=""):
    suffix = env_suffix or prop_name
    keys = [f"{p}_SERVER_{suffix}" for p in ("CLEARML", "TRAINS")]
    value = first(map(getenv, keys))
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


def get_default_company():
    return config.get("apiserver.default_company")


missed_es_upgrade = False
es_connection_error = False
