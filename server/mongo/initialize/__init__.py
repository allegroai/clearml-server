from pathlib import Path
from typing import Sequence, Union

from config import config
from config.info import get_default_company
from database.model.auth import Role
from service_repo.auth.fixed_user import FixedUser
from .migration import _apply_migrations
from .pre_populate import PrePopulate
from .user import ensure_fixed_user, _ensure_auth_user, _ensure_backend_user
from .util import _ensure_company, _ensure_default_queue, _ensure_uuid

log = config.logger(__package__)


def _pre_populate(company_id: str, zip_file: str):
    if not zip_file or not Path(zip_file).is_file():
        msg = f"Invalid pre-populate zip file: {zip_file}"
        if config.get("apiserver.pre_populate.fail_on_error", False):
            log.error(msg)
            raise ValueError(msg)
        else:
            log.warning(msg)
    else:
        log.info(f"Pre-populating using {zip_file}")

        user_id = _ensure_backend_user(
            "__allegroai__", company_id, "Allegro.ai"
        )

        PrePopulate.import_from_zip(
            zip_file,
            company_id="",
            user_id=user_id,
            artifacts_path=config.get(
                "apiserver.pre_populate.artifacts_path", None
            ),
        )


def _resolve_zip_files(zip_files: Union[Sequence[str], str]) -> Sequence[str]:
    if isinstance(zip_files, str):
        zip_files = [zip_files]
    for p in map(Path, zip_files):
        if p.is_file():
            yield p
        if p.is_dir():
            yield from p.glob("*.zip")
        log.warning(f"Invalid pre-populate entry {str(p)}, skipping")


def pre_populate_data():
    for zip_file in _resolve_zip_files(config.get("apiserver.pre_populate.zip_files")):
        _pre_populate(company_id=get_default_company(), zip_file=zip_file)


def init_mongo_data() -> bool:
    try:
        empty_dbs = _apply_migrations(log)

        _ensure_uuid()

        company_id = _ensure_company(log)

        _ensure_default_queue(company_id)

        fixed_mode = FixedUser.enabled()

        for user, credentials in config.get("secure.credentials", {}).items():
            user_data = {
                "name": user,
                "role": credentials.role,
                "email": f"{user}@example.com",
                "key": credentials.user_key,
                "secret": credentials.user_secret,
            }
            revoke = fixed_mode and credentials.get("revoke_in_fixed_mode", False)
            user_id = _ensure_auth_user(user_data, company_id, log=log, revoke=revoke)
            if credentials.role == Role.user:
                _ensure_backend_user(user_id, company_id, credentials.display_name)

        if fixed_mode:
            log.info("Fixed users mode is enabled")
            FixedUser.validate()
            for user in FixedUser.from_config():
                try:
                    ensure_fixed_user(user, company_id, log=log)
                except Exception as ex:
                    log.error(f"Failed creating fixed user {user.name}: {ex}")

        return empty_dbs
    except Exception as ex:
        log.exception("Failed initializing mongodb")
