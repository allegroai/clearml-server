from typing import Any, Optional, Sequence, Tuple

from mongoengine import Document, StringField, DynamicField, Q
from mongoengine.errors import NotUniqueError

from apiserver.database import Database, strict
from apiserver.database.model import DbModelMixin


class SettingKeys:
    server__uuid = "server.uuid"


class Settings(DbModelMixin, Document):
    meta = {
        "db_alias": Database.backend,
        "strict": strict,
    }

    key = StringField(primary_key=True)
    value = DynamicField()

    @classmethod
    def get_by_key(cls, key: str, default: Optional[Any] = None, sep: str = ".") -> Any:
        key = key.strip(sep)
        res = Settings.objects(key=key).first()
        if not res:
            return default
        return res.value

    @classmethod
    def get_by_prefix(
        cls, key_prefix: str, default: Optional[Any] = None, sep: str = "."
    ) -> Sequence[Tuple[str, Any]]:
        key_prefix = key_prefix.strip(sep)
        query = Q(key=key_prefix) | Q(key__startswith=key_prefix + sep)
        res = Settings.objects(query)
        if not res:
            return default
        return [(x.key, x.value) for x in res]

    @classmethod
    def set_or_add_value(cls, key: str, value: Any, sep: str = ".") -> bool:
        """ Sets a new value or adds a new key/value setting (if key does not exist) """
        key = key.strip(sep)
        res = Settings.objects(key=key).update(key=key, value=value, upsert=True)
        return bool(res)

    @classmethod
    def add_value(cls, key: str, value: Any, sep: str = ".") -> bool:
        """ Adds a new key/value settings. Fails if key already exists. """
        key = key.strip(sep)
        try:
            res = cls(key=key, value=value).save(force_insert=True)
            return bool(res)
        except NotUniqueError:
            return False
