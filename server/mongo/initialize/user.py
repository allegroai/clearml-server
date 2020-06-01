from datetime import datetime
from logging import Logger

import attr

from database.model.auth import Role
from database.model.auth import User as AuthUser, Credentials
from database.model.user import User
from service_repo.auth.fixed_user import FixedUser


def _ensure_auth_user(user_data: dict, company_id: str, log: Logger, revoke: bool = False):
    ensure_credentials = {"key", "secret"}.issubset(user_data)
    if ensure_credentials:
        user = AuthUser.objects(
            credentials__match=Credentials(
                key=user_data["key"], secret=user_data["secret"]
            )
        ).first()
        if user:
            if revoke:
                user.credentials = []
                user.save()
            return user.id

    user_id = user_data.get("id", f"__{user_data['name']}__")

    log.info(f"Creating user: {user_data['name']}")
    user = AuthUser(
        id=user_id,
        name=user_data["name"],
        company=company_id,
        role=user_data["role"],
        email=user_data["email"],
        created=datetime.utcnow(),
        credentials=[Credentials(key=user_data["key"], secret=user_data["secret"])] if not revoke else []
        if ensure_credentials
        else None,
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
    ).save()

    return user_id


def ensure_fixed_user(user: FixedUser, company_id: str, log: Logger):
    if User.objects(id=user.user_id).first():
        return

    data = attr.asdict(user)
    data["id"] = user.user_id
    data["email"] = f"{user.user_id}@example.com"
    data["role"] = Role.user

    _ensure_auth_user(user_data=data, company_id=company_id, log=log)

    given_name, _, family_name = user.name.partition(" ")

    User(
        id=user.user_id,
        company=company_id,
        name=user.name,
        given_name=given_name,
        family_name=family_name,
    ).save()
