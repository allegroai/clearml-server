import functools
import re
from uuid import uuid4

import attr
from flask import request
from flask_login import UserMixin

from config import config
from session import SessionFactory

log = config.logger(__file__)

AUTH_TOKEN_COOKIE_KEY = config["webserver.auth.session_auth_cookie_name"]


@attr.s(auto_attribs=True)
class UserData:
    id: str = None
    company: str = None
    name: str = None
    family_name: str = None
    given_name: str = None

    @classmethod
    def from_dict(cls, d):
        return cls(**{k: v for k, v in d.items() if k in attr.fields_dict(cls)})


class CreateUserError(Exception):
    pass


class UsersGetAllError(Exception):
    def __init__(self, res, *args):
        self.res = res
        super(UsersGetAllError, self).__init__(res, *args)


class SimpleUser(UserMixin):
    _cache = None

    @property
    def user_data(self) -> UserData:
        return self._user_data

    @property
    def token(self):
        return self._get_token()

    def __init__(self, user_data: UserData):
        super(SimpleUser, self).__init__()
        self._user_data = user_data

    def get_id(self):
        return self._user_data.id

    @classmethod
    def get(cls, user_id):
        res = SessionFactory.get().send_request(
            "users.get_by_id", json={"user": user_id}
        )
        if not res.ok:
            return None
        return cls(user_data=UserData.from_dict(res.json()["data"]["user"]))

    @classmethod
    def get_all(cls):
        res = SessionFactory.get().send_request("users.get_all")
        if not res.ok:
            raise UsersGetAllError(res)
        return [
            cls(user_data=UserData.from_dict(user))
            for user in res.json()["data"]["users"]
        ]

    @classmethod
    def create_by_name(cls, name: str):
        name = re.sub(r"\s+", " ", name.strip())
        existing_user = next(
            (user for user in cls.get_all() if user.user_data.name.lower() == name.lower()),
            None,
        )

        if existing_user:
            return existing_user

        company_id = config.get("webserver.default_company")
        unique_email = f"{str(uuid4()).replace('-', '')}@example.com"
        given_name, _, family_name = name.partition(" ")

        res = SessionFactory.get().send_request(
            "auth.create_user",
            json={
                "email": unique_email,
                "name": name,
                "company": company_id,
                "given_name": given_name,
                "family_name": family_name,
            },
        )

        if not res.ok:
            resp = res.json()
            log.error(f"Failed creating user {name} ({resp['meta']})")
            raise CreateUserError(
                f"Failed creating user: {res.json().get(resp['meta']['result_msg'])}"
            )

        return cls.get(res.json()["data"]["id"])

    @property
    def is_authenticated(self) -> bool:
        if AUTH_TOKEN_COOKIE_KEY not in request.cookies:
            return False

        token = request.cookies[AUTH_TOKEN_COOKIE_KEY]

        # Assume we're authenticated if we have a token
        return bool(token)

    @functools.lru_cache(maxsize=None)
    def _get_token(self):
        res = SessionFactory.get().send_request(
            "auth.get_token_for_user",
            json={"user": self._user_data.id, "company": self._user_data.company},
        )
        if not res.ok:
            log.error(
                f"Failed generating token for user {self._user_data.id} ({res.json()['meta']})"
            )
            raise ValueError(f"Failed generating token for user {self._user_data.id}")
        return res.json()["data"]["token"]
