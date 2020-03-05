from pathlib import Path

from config import config
from database.model.auth import Role
from service_repo.auth.fixed_user import FixedUser
from .migration import _apply_migrations
from .pre_populate import PrePopulate
from .user import ensure_fixed_user, _ensure_auth_user, _ensure_backend_user
from .util import _ensure_company, _ensure_default_queue, _ensure_uuid

log = config.logger(__package__)


def init_mongo_data():
    try:
        empty_dbs = _apply_migrations(log)

        _ensure_uuid()

        company_id = _ensure_company(log)

        _ensure_default_queue(company_id)

        if empty_dbs and config.get("apiserver.mongo.pre_populate.enabled", False):
            zip_file = config.get("apiserver.mongo.pre_populate.zip_file")
            if not zip_file or not Path(zip_file).is_file():
                msg = f"Failed pre-populating database: invalid zip file {zip_file}"
                if config.get("apiserver.mongo.pre_populate.fail_on_error", False):
                    log.error(msg)
                    raise ValueError(msg)
                else:
                    log.warning(msg)
            else:

                user_id = _ensure_backend_user(
                    "__allegroai__", company_id, "Allegro.ai"
                )

                PrePopulate.import_from_zip(zip_file, user_id=user_id)

        users = [
            {
                "name": "apiserver",
                "role": Role.system,
                "email": "apiserver@example.com",
            },
            {
                "name": "webserver",
                "role": Role.system,
                "email": "webserver@example.com",
            },
            {"name": "tests", "role": Role.user, "email": "tests@example.com"},
        ]

        for user in users:
            credentials = config.get(f"secure.credentials.{user['name']}")
            user["key"] = credentials.user_key
            user["secret"] = credentials.user_secret
            _ensure_auth_user(user, company_id, log=log)

        if FixedUser.enabled():
            log.info("Fixed users mode is enabled")
            FixedUser.validate()
            for user in FixedUser.from_config():
                try:
                    ensure_fixed_user(user, company_id, log=log)
                except Exception as ex:
                    log.error(f"Failed creating fixed user {user.name}: {ex}")
    except Exception as ex:
        log.exception("Failed initializing mongodb")
