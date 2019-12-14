from functools import lru_cache
from pathlib import Path
from os import getenv

root = Path(__file__).parent.parent


@lru_cache()
def get_build_number():
    try:
        return (root / "BUILD").read_text().strip()
    except FileNotFoundError:
        return ""


@lru_cache()
def get_version():
    try:
        return (root / "VERSION").read_text().strip()
    except FileNotFoundError:
        return ""


@lru_cache()
def get_commit_number():
    try:
        return (root / "COMMIT").read_text().strip()
    except FileNotFoundError:
        return ""


@lru_cache()
def get_deployment_type() -> str:
    value = getenv("TRAINS_SERVER_DEPLOYMENT_TYPE")
    if value:
        return value

    try:
        value = (root / "DEPLOY").read_text().strip()
    except FileNotFoundError:
        pass

    return value or "manual"
