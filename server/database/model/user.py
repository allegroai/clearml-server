from mongoengine import Document, StringField, DynamicField

from database import Database, strict
from database.model import DbModelMixin
from database.model.company import Company


class User(DbModelMixin, Document):
    meta = {
        'db_alias': Database.backend,
        'strict': strict,
    }

    id = StringField(primary_key=True)
    company = StringField(required=True, reference_field=Company)
    name = StringField(required=True, user_set_allowed=True)
    family_name = StringField(user_set_allowed=True)
    given_name = StringField(user_set_allowed=True)
    avatar = StringField()
    preferences = DynamicField(default="", exclude_by_default=True)
