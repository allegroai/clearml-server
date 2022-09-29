from enum import Enum

from mongoengine import StringField, DateTimeField, IntField, EnumField

from apiserver.database import Database, strict
from apiserver.database.model import AttributedDocument


class StorageType(str, Enum):
    fileserver = "fileserver"
    unknown = "unknown"


class FileType(str, Enum):
    file = "file"
    folder = "folder"


class DeletionStatus(str, Enum):
    created = "created"
    retrying = "retrying"
    failed = "failed"


class UrlToDelete(AttributedDocument):
    _field_collation_overrides = {
        "url": AttributedDocument._numeric_locale,
    }

    meta = {
        "db_alias": Database.backend,
        "strict": strict,
        "indexes": [
            ("company", "user", "task"),
            "storage_type",
            "created",
            "retry_count",
            "type",
        ],
    }

    id = StringField(primary_key=True)
    url = StringField(required=True, unique_with="company")
    task = StringField(required=True)
    created = DateTimeField(required=True)
    storage_type = EnumField(StorageType, default=StorageType.unknown)
    type = EnumField(FileType, default=FileType.file)
    retry_count = IntField(default=0)
    last_failure_time = DateTimeField()
    last_failure_reason = StringField()
    status = EnumField(DeletionStatus, default=DeletionStatus.created)
