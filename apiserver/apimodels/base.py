from jsonmodels import models, fields
from jsonmodels.validators import Length

from apiserver.apimodels import MongoengineFieldsDict, ListField


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
