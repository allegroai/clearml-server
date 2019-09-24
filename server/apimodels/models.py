from jsonmodels import models, fields
from six import string_types

from apimodels import ListField, DictField
from apimodels.base import UpdateResponse
from apimodels.tasks import PublishResponse as TaskPublishResponse


class CreateModelRequest(models.Base):
    name = fields.StringField(required=True)
    uri = fields.StringField(required=True)
    labels = DictField(value_types=string_types+(int,), required=True)
    tags = ListField(items_types=string_types)
    system_tags = ListField(items_types=string_types)
    comment = fields.StringField()
    public = fields.BoolField(default=False)
    project = fields.StringField()
    parent = fields.StringField()
    framework = fields.StringField()
    design = DictField()
    ready = fields.BoolField(default=True)
    ui_cache = DictField()
    task = fields.StringField()


class CreateModelResponse(models.Base):
    id = fields.StringField(required=True)
    created = fields.BoolField(required=True)


class PublishModelRequest(models.Base):
    model = fields.StringField(required=True)
    force_publish_task = fields.BoolField(default=False)
    publish_task = fields.BoolField(default=True)


class ModelTaskPublishResponse(models.Base):
    id = fields.StringField(required=True)
    data = fields.EmbeddedField(TaskPublishResponse)


class PublishModelResponse(UpdateResponse):
    published_task = fields.EmbeddedField(ModelTaskPublishResponse)
    updated = fields.IntField()
