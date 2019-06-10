from datetime import datetime
from furl import furl

from database.model.auth import User, Credentials
from config import config
from database.model.auth import Role
from database.model.company import Company
from elastic.apply_mappings import apply_mappings_to_host

log = config.logger(__file__)


class MissingElasticConfiguration(Exception):
    """
    Exception when cluster configuration is not found in config files
    """

    pass


def init_es_data():
    hosts_key = "hosts.elastic.events.hosts"
    hosts_config = config.get(hosts_key, None)
    if not hosts_config:
        raise MissingElasticConfiguration(hosts_key)

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


def _ensure_user(user_data, company_id):
    user = User.objects(
        credentials__match=Credentials(key=user_data["key"], secret=user_data["secret"])
    ).first()
    if user:
        return user.id

    log.info(f"Creating user: {user_data['name']}")
    user = User(
        id=f"__{user_data['name']}__",
        name=user_data["name"],
        company=company_id,
        role=user_data["role"],
        email=user_data["email"],
        created=datetime.utcnow(),
        credentials=[Credentials(key=user_data["key"], secret=user_data["secret"])],
    )

    user.save()

    return user.id


def init_mongo_data():
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
        _ensure_user(user, company_id)
