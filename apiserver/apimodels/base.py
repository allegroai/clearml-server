from jsonmodels import models, fields
from jsonmodels.validators import Length
from mongoengine.base import BaseDocument

from apiserver.apimodels import DictField, ListField


class MongoengineFieldsDict(DictField):
    """
    DictField representing mongoengine field names/value mapping.
    Used to convert mongoengine-style field/subfield notation to user-presentable syntax, including handling update
        operators.
    """

    mongoengine_update_operators = (
        "inc",
        "dec",
        "push",
        "push_all",
        "pop",
        "pull",
        "pull_all",
        "add_to_set",
    )

    @staticmethod
    def _normalize_mongo_value(value):
        if isinstance(value, BaseDocument):
            return value.to_mongo()
        return value

    @classmethod
    def _normalize_mongo_field_path(cls, path, value):
        parts = path.split("__")
        if len(parts) > 1:
            if parts[0] == "set":
                parts = parts[1:]
            elif parts[0] == "unset":
                parts = parts[1:]
                value = None
            elif parts[0] in cls.mongoengine_update_operators:
                return None, None
        return ".".join(parts), cls._normalize_mongo_value(value)

    def parse_value(self, value):
        value = super(MongoengineFieldsDict, self).parse_value(value)
        return {
            k: v
            for k, v in (self._normalize_mongo_field_path(*p) for p in value.items())
            if k is not None
        }


class UpdateResponse(models.Base):
    updated = fields.IntField(required=True)
    fields = MongoengineFieldsDict()


class PagedRequest(models.Base):
    page = fields.IntField()
    page_size = fields.IntField()


class IdResponse(models.Base):
    id = fields.StringField(required=True)


class MakePublicRequest(models.Base):
    ids = ListField(items_types=str, validators=[Length(minimum_value=1)])


class MoveRequest(models.Base):
    ids = ListField([str], validators=Length(minimum_value=1))
    project = fields.StringField()
    project_name = fields.StringField()
