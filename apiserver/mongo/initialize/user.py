from datetime import datetime
from logging import Logger

import attr

from apiserver.database.model.auth import Role
from apiserver.database.model.auth import User as AuthUser, Credentials
from apiserver.database.model.user import User
from apiserver.service_repo.auth.fixed_user import FixedUser


def _ensure_auth_user(user_data: dict, company_id: str, log: Logger, revoke: bool = False):
    key, secret = user_data.get("key"), user_data.get("secret")
    if not (key and secret):
        credentials = None
    else:
        creds = Credentials(key=key, secret=secret)

        user = AuthUser.objects(credentials__match=creds).first()
        if user:
            if revoke:
                user.credentials = []
                user.save()
            return user.id

        credentials = [] if revoke else [creds]

    user_id = user_data.get("id", f"__{user_data['name']}__")

    log.info(f"Creating user: {user_data['name']}")

    user = AuthUser(
        id=user_id,
        name=user_data["name"],
        company=company_id,
        role=user_data["role"],
        email=user_data["email"],
        created=datetime.utcnow(),
        credentials=credentials,
    )

    user.save()

    return user.id


def _ensure_backend_user(user_id: str, company_id: str, user_name: str):
    given_name, _, family_name = user_name.partition(" ")

    User(
        id=user_id,
        company=company_id,
        name=user_name,
        given_name=given_name,
        family_name=family_name,
        created=datetime.utcnow(),
    ).save()

    return user_id


def ensure_fixed_user(user: FixedUser, log: Logger):
    db_user = User.objects(company=user.company, id=user.user_id).first()
    if db_user:
        # noinspection PyBroadException
        try:
            log.info(f"Updating user name: {user.name}")
            given_name, _, family_name = user.name.partition(" ")
            db_user.update(name=user.name, given_name=given_name, family_name=family_name)
        except Exception:
            pass
        return

    data = attr.asdict(user)
    data["id"] = user.user_id
    data["email"] = f"{user.user_id}@example.com"
    data["role"] = Role.guest if user.is_guest else Role.user

    _ensure_auth_user(user_data=data, company_id=user.company, log=log)

    return _ensure_backend_user(user.user_id, user.company, user.name)
