import hashlib
from functools import lru_cache
from typing import Sequence, TypeVar

import attr

from config import config
from config.info import get_default_company

T = TypeVar("T", bound="FixedUser")


class FixedUsersError(Exception):
    pass


@attr.s(auto_attribs=True)
class FixedUser:
    username: str
    password: str
    name: str
    company: str = get_default_company()

    def __attrs_post_init__(self):
        self.user_id = hashlib.md5(f"{self.company}:{self.username}".encode()).hexdigest()

    @classmethod
    def enabled(cls):
        return config.get("apiserver.auth.fixed_users.enabled", False)

    @classmethod
    def validate(cls):
        if not cls.enabled():
            return
        users = cls.from_config()
        if len({user.username for user in users}) < len(users):
            raise FixedUsersError(
                "Duplicate user names found in fixed users configuration"
            )

    @classmethod
    @lru_cache()
    def from_config(cls) -> Sequence[T]:
        return [
            cls(**user) for user in config.get("apiserver.auth.fixed_users.users", [])
        ]

    @classmethod
    @lru_cache()
    def get_by_username(cls, username) -> T:
        return next(
            (user for user in cls.from_config() if user.username == username), None
        )

    def __hash__(self):
        return hash(self.user_id)
