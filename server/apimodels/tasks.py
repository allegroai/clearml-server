import six
from jsonmodels import models
from jsonmodels.fields import StringField, BoolField, IntField
from jsonmodels.validators import Enum

from apimodels import DictField, ListField
from apimodels.base import UpdateResponse
from database.model.task.task import TaskType
from database.utils import get_options


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
