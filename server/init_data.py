import importlib.util
from datetime import datetime
from pathlib import Path

import attr
from furl import furl
from mongoengine.connection import get_db
from semantic_version import Version

import database.utils
from config import config
from database import Database
from database.model.auth import Role
from database.model.auth import User as AuthUser, Credentials
from database.model.company import Company
from database.model.user import User
from database.model.version import Version as DatabaseVersion
from elastic.apply_mappings import apply_mappings_to_host
from es_factory import get_cluster_config
from service_repo.auth.fixed_user import FixedUser

log = config.logger(__file__)

migration_dir = (Path(__file__) / "../../migration/mongodb").resolve()


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


def _apply_migrations():
    if not migration_dir.is_dir():
        raise ValueError(f"Invalid migration dir {migration_dir}")

    try:
        previous_versions = sorted(
            (Version(ver.num) for ver in DatabaseVersion.objects().only("num")),
            reverse=True,
        )
    except ValueError as ex:
        raise ValueError(f"Invalid database version number encountered: {ex}")

    last_version = previous_versions[0] if previous_versions else Version("0.0.0")

    try:
        new_scripts = {
            ver: path
            for ver, path in (
                (Version(f.stem), f) for f in migration_dir.glob("*.py")
            )
            if ver > last_version
        }
    except ValueError as ex:
        raise ValueError(f"Failed parsing migration version from file: {ex}")

    dbs = {Database.auth: "migrate_auth", Database.backend: "migrate_backend"}

    migration_log = log.getChild("mongodb_migration")

    for script_version in sorted(new_scripts.keys()):
        script = new_scripts[script_version]
        spec = importlib.util.spec_from_file_location(script.stem, str(script))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        for alias, func_name in dbs.items():
            func = getattr(module, func_name, None)
            if not func:
                continue
            try:
                migration_log.info(f"Applying {script.stem}/{func_name}()")
                func(get_db(alias))
            except Exception:
                migration_log.exception(f"Failed applying {script}:{func_name}()")
                raise ValueError("Migration failed, aborting. Please restore backup.")

        DatabaseVersion(
            id=database.utils.id(),
            num=script.stem,
            created=datetime.utcnow(),
            desc="Applied on server startup",
        ).save()


def init_mongo_data():
    try:
        _apply_migrations()

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
        log.exception("Failed initializing mongodb")
