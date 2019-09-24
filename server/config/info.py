from functools import lru_cache
from pathlib import Path

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
