from jsonmodels import models, fields
from six import string_types

from apiserver.apimodels import ListField, DictField
from apiserver.apimodels.base import UpdateResponse
from apiserver.apimodels.batch import BatchRequest
from apiserver.apimodels.metadata import (
    MetadataItem,
    DeleteMetadata,
    AddOrUpdateMetadata,
)


class GetFrameworksRequest(models.Base):
    projects = fields.ListField(items_types=[str])


class CreateModelRequest(models.Base):
    name = fields.StringField(required=True)
    uri = fields.StringField(required=True)
    labels = DictField(value_types=string_types + (int,))
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
    metadata = DictField(value_types=[MetadataItem])


class CreateModelResponse(models.Base):
    id = fields.StringField(required=True)
    created = fields.BoolField(required=True)


class ModelRequest(models.Base):
    model = fields.StringField(required=True)


class DeleteModelRequest(ModelRequest):
    force = fields.BoolField(default=False)
    delete_external_artifacts = fields.BoolField(default=True)


class ModelsDeleteManyRequest(BatchRequest):
    force = fields.BoolField(default=False)
    delete_external_artifacts = fields.BoolField(default=True)


class PublishModelRequest(ModelRequest):
    force_publish_task = fields.BoolField(default=False)
    publish_task = fields.BoolField(default=True)


class ModelTaskPublishResponse(models.Base):
    id = fields.StringField(required=True)
    data = fields.EmbeddedField(UpdateResponse)


class PublishModelResponse(UpdateResponse):
    published_task = fields.EmbeddedField(ModelTaskPublishResponse)


class ModelsPublishManyRequest(BatchRequest):
    force_publish_task = fields.BoolField(default=False)
    publish_task = fields.BoolField(default=True)


class DeleteMetadataRequest(DeleteMetadata):
    model = fields.StringField(required=True)


class AddOrUpdateMetadataRequest(AddOrUpdateMetadata):
    model = fields.StringField(required=True)


class ModelsGetRequest(models.Base):
    include_stats = fields.BoolField(default=False)
    allow_public = fields.BoolField(default=True)
