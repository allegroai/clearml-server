from mongoengine import Document, DateTimeField, StringField

from apiserver.database import Database, strict
from apiserver.database.model import DbModelMixin


class Version(DbModelMixin, Document):
    meta = {
        "collection": "versions",  # custom collection name ('version' is not a proper collection name...)
        "db_alias": Database.backend,  # although we'll use this model for all databases, a default must be defined
        "strict": strict,
        "indexes": [("-created", "-num")],
    }

    id = StringField(primary_key=True)
    num = StringField(required=True)
    created = DateTimeField(required=True)
    desc = StringField()
