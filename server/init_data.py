from datetime import datetime

import attr
from furl import furl

from config import config
from database.model.auth import Role
from database.model.auth import User as AuthUser, Credentials
from database.model.company import Company
from database.model.user import User
from elastic.apply_mappings import apply_mappings_to_host
from es_factory import get_cluster_config
from service_repo.auth.fixed_user import FixedUser

log = config.logger(__file__)


class MissingElasticConfiguration(Exception):
    """
    Exception when cluster configuration is not found in config files
    """

    pass


def init_es_data():
    hosts_config = get_cluster_config("events").get("hosts")
    if not hosts_config:
        raise MissingElasticConfiguration("for cluster 'events'")

    for conf in hosts_config:
        host = furl(scheme="http", host=conf["host"], port=conf["port"]).url
        log.info(f"Applying mappings to host: {host}")
        res = apply_mappings_to_host(host)
        log.info(res)


def _ensure_company():
    company_id = config.get("apiserver.default_company")
    company = Company.objects(id=company_id).only("id").first()
    if company:
        return company_id

    company_name = "trains"
    log.info(f"Creating company: {company_name}")
    company = Company(id=company_id, name=company_name)
    company.save()
    return company_id


def _ensure_auth_user(user_data, company_id):
    ensure_credentials = {"key", "secret"}.issubset(user_data.keys())
    if ensure_credentials:
        user = AuthUser.objects(
            credentials__match=Credentials(
                key=user_data["key"], secret=user_data["secret"]
            )
        ).first()
        if user:
            return user.id

    log.info(f"Creating user: {user_data['name']}")
    user = AuthUser(
        id=user_data.get("id", f"__{user_data['name']}__"),
        name=user_data["name"],
        company=company_id,
        role=user_data["role"],
        email=user_data["email"],
        created=datetime.utcnow(),
        credentials=[Credentials(key=user_data["key"], secret=user_data["secret"])]
        if ensure_credentials
        else None,
    )

    user.save()

    return user.id


def _ensure_user(user: FixedUser, company_id: str):
    if User.objects(id=user.user_id).first():
        return

    data = attr.asdict(user)
    data["id"] = user.user_id
    data["email"] = f"{user.user_id}@example.com"
    data["role"] = Role.user

    _ensure_auth_user(
        user_data=data,
        company_id=company_id,
    )

    given_name, _, family_name = user.name.partition(" ")

    User(
        id=user.user_id,
        company=company_id,
        name=user.name,
        given_name=given_name,
        family_name=family_name,
    ).save()


def init_mongo_data():
    try:
        company_id = _ensure_company()
        users = [
            {"name": "apiserver", "role": Role.system, "email": "apiserver@example.com"},
            {"name": "webserver", "role": Role.system, "email": "webserver@example.com"},
            {"name": "tests", "role": Role.user, "email": "tests@example.com"},
        ]

        for user in users:
            credentials = config.get(f"secure.credentials.{user['name']}")
            user["key"] = credentials.user_key
            user["secret"] = credentials.user_secret
            _ensure_auth_user(user, company_id)

        if FixedUser.enabled():
            log.info("Fixed users mode is enabled")
            for user in FixedUser.from_config():
                try:
                    _ensure_user(user, company_id)
                except Exception as ex:
                    log.error(f"Failed creating fixed user {user['name']}: {ex}")
    except Exception as ex:
        pass
