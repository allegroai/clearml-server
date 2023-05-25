import importlib.util
from datetime import datetime
from inspect import signature
from logging import Logger
from pathlib import Path

import pymongo.database
from mongoengine.connection import get_db
from packaging.version import Version, parse

from apiserver.database import utils
from apiserver.database import Database
from apiserver.database.model.version import Version as DatabaseVersion

_migrations = "migrations"
_parent_dir = Path(__file__).resolve().parents[1]
_migration_dir = _parent_dir / _migrations


def check_mongo_empty() -> bool:
    for alias in utils.get_options(Database):
        collection_names = get_db(alias).list_collection_names()
        if collection_names and any(
            name in collection_names
            for name in ["company", "user", "versions"]
        ):
            return False

    return True


def get_last_server_version() -> Version:
    try:
        previous_versions = sorted(
            (Version(ver.num) for ver in DatabaseVersion.objects().only("num")),
            reverse=True,
        )
    except ValueError as ex:
        raise ValueError(f"Invalid database version number encountered: {ex}")

    return previous_versions[0] if previous_versions else Version("0.0.0")


def _apply_migrations(log: Logger):
    """
    Apply migrations as found in the migration dir.
    Returns a boolean indicating whether the database was empty prior to migration.
    """
    log = log.getChild(Path(__file__).stem)

    log.info(f"Started mongodb migrations")

    if not _migration_dir.is_dir():
        raise ValueError(f"Invalid migration dir {_migration_dir}")

    empty_dbs = check_mongo_empty()
    last_version = get_last_server_version()

    try:
        new_scripts = {
            ver: path
            for ver, path in (
                (parse(f.stem.replace("_", ".")), f)
                for f in _migration_dir.glob("*.py")
            )
            if ver > last_version
        }
    except ValueError as ex:
        raise ValueError(f"Failed parsing migration version from file: {ex}")

    dbs = {Database.auth: "migrate_auth", Database.backend: "migrate_backend"}

    for script_version in sorted(new_scripts):
        script = new_scripts[script_version]

        if empty_dbs:
            log.info(f"Skipping migration {script.name} (empty databases)")
        else:
            spec = importlib.util.spec_from_file_location(
                ".".join(("apiserver", _parent_dir.name, _migrations, script.stem)),
                str(script),
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            for alias, func_name in dbs.items():
                func = getattr(module, func_name, None)
                if not func:
                    continue
                try:
                    sig = signature(func)
                    kwargs = {}
                    if len(sig.parameters) == 2:
                        name, param = list(sig.parameters.items())[-1]
                        key = name.replace("_", "-")
                        if issubclass(param.annotation, pymongo.database.Database) and key in dbs:
                            kwargs[name] = get_db(key)
                    log.info(f"Applying {script.stem}/{func_name}()")
                    func(get_db(alias), **kwargs)
                except Exception:
                    log.exception(f"Failed applying {script}:{func_name}()")
                    raise ValueError(
                        "Migration failed, aborting. Please restore backup."
                    )

        DatabaseVersion(
            id=utils.id(),
            num=str(script_version),
            created=datetime.utcnow(),
            desc="Applied on server startup",
        ).save()

    log.info("Finished mongodb migrations")
