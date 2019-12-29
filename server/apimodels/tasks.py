import six
from jsonmodels import models
from jsonmodels.fields import StringField, BoolField, IntField, EmbeddedField
from jsonmodels.validators import Enum

from apimodels import DictField, ListField
from apimodels.base import UpdateResponse
from database.model.task.task import TaskType
from database.utils import get_options


class ArtifactTypeData(models.Base):
    preview = StringField()
    content_type = StringField()
    data_hash = StringField()


class Artifact(models.Base):
    key = StringField(required=True)
    type = StringField(required=True)
    mode = StringField(validators=Enum("input", "output"), default="output")
    uri = StringField()
    hash = StringField()
    content_size = IntField()
    timestamp = IntField()
    type_data = EmbeddedField(ArtifactTypeData)
    display_data = ListField([list])


class StartedResponse(UpdateResponse):
    started = IntField()


class EnqueueResponse(UpdateResponse):
    queued = IntField()


class DequeueResponse(UpdateResponse):
    dequeued = IntField()


class ResetResponse(UpdateResponse):
    deleted_indices = ListField(items_types=six.string_types)
    dequeued = DictField()
    frames = DictField()
    events = DictField()
    model_deleted = IntField()


class TaskRequest(models.Base):
    task = StringField(required=True)


class UpdateRequest(TaskRequest):
    status_reason = StringField(default="")
    status_message = StringField(default="")
    force = BoolField(default=False)


class EnqueueRequest(UpdateRequest):
    queue = StringField()


class DeleteRequest(UpdateRequest):
    move_to_trash = BoolField(default=True)


class SetRequirementsRequest(TaskRequest):
    requirements = DictField(required=True)


class PublishRequest(UpdateRequest):
    publish_model = BoolField(default=True)


class PublishResponse(UpdateResponse):
    pass


class TaskData(models.Base):
    """
    This is a partial description of task can be updated incrementally
    """


class CreateRequest(TaskData):
    name = StringField(required=True)
    type = StringField(required=True, validators=Enum(*get_options(TaskType)))


class PingRequest(TaskRequest):
    pass


class CloneRequest(TaskRequest):
    new_task_name = StringField()
    new_task_comment = StringField()
    new_task_tags = ListField([str])
    new_task_system_tags = ListField([str])
    new_task_parent = StringField()
    new_task_project = StringField()
    execution_overrides = DictField()


class AddOrUpdateArtifactsRequest(TaskRequest):
    artifacts = ListField([Artifact], required=True)


class AddOrUpdateArtifactsResponse(models.Base):
    added = ListField([str])
    updated = ListField([str])
