import hashlib
from functools import lru_cache
from typing import Sequence, TypeVar

import attr

from config import config

T = TypeVar("T", bound="FixedUser")


@attr.s(auto_attribs=True)
class FixedUser:
    username: str
    password: str
    name: str

    def __attrs_post_init__(self):
        self.user_id = hashlib.md5(f"{self.username}:{self.password}".encode()).hexdigest()

    @classmethod
    def enabled(cls):
        return config.get("apiserver.auth.fixed_users.enabled", False)

    @classmethod
    @lru_cache()
    def from_config(cls) -> Sequence[T]:
        return [cls(**user) for user in config.get("apiserver.auth.fixed_users.users", [])]

    @classmethod
    @lru_cache()
    def get_by_username(cls, username) -> T:
        return next(
            (user for user in cls.from_config() if user.username == username), None
        )

    def __hash__(self):
        return hash(self.user_id)
