from enum import auto

from jsonmodels import fields, models
from jsonmodels.validators import Length

from apiserver.apimodels import DictField, ActualEnumField
from apiserver.utilities.stringenum import StringEnum


class Filter(models.Base):
    tags = fields.ListField([str])
    system_tags = fields.ListField([str])


class TagsRequest(models.Base):
    include_system = fields.BoolField(default=False)
    filter = fields.EmbeddedField(Filter)


class EntitiesCountRequest(models.Base):
    projects = DictField()
    tasks = DictField()
    models = DictField()
    pipelines = DictField()
    datasets = DictField()
    reports = DictField()
    active_users = fields.ListField(str)
    search_hidden = fields.BoolField(default=False)
    allow_public = fields.BoolField(default=True)


class DownloadType(StringEnum):
    csv = auto()


class EntityType(StringEnum):
    task = auto()
    model = auto()


class PrepareDownloadForGetAll(models.Base):
    download_type = ActualEnumField(DownloadType, default=DownloadType.csv)
    entity_type = ActualEnumField(EntityType)
    allow_public = fields.BoolField(default=True)
    search_hidden = fields.BoolField(default=False)
    only_fields = fields.ListField(
        items_types=[str], validators=[Length(1)], required=True
    )


class DownloadForGetAll(models.Base):
    prepare_id = fields.StringField(required=True)
