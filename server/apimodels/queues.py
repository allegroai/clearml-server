from jsonmodels import validators
from jsonmodels.fields import StringField, IntField, BoolField, FloatField
from jsonmodels.models import Base

from apimodels import ListField


class GetDefaultResp(Base):
    id = StringField(required=True)
    name = StringField(required=True)


class CreateRequest(Base):
    name = StringField(required=True)
    tags = ListField(items_types=[str])
    system_tags = ListField(items_types=[str])


class QueueRequest(Base):
    queue = StringField(required=True)


class DeleteRequest(QueueRequest):
    force = BoolField(default=False)


class UpdateRequest(QueueRequest):
    name = StringField()
    tags = ListField(items_types=[str])
    system_tags = ListField(items_types=[str])


class TaskRequest(QueueRequest):
    task = StringField(required=True)


class MoveTaskRequest(TaskRequest):
    count = IntField(default=1)


class MoveTaskResponse(Base):
    position = IntField()


class GetMetricsRequest(Base):
    queue_ids = ListField([str])
    from_date = FloatField(required=True, validators=validators.Min(0))
    to_date = FloatField(required=True, validators=validators.Min(0))
    interval = IntField(required=True, validators=validators.Min(1))


class QueueMetrics(Base):
    queue = StringField()
    dates = ListField(int)
    avg_waiting_times = ListField([float, int])
    queue_lengths = ListField(int)


class GetMetricsResponse(Base):
    queues = ListField(QueueMetrics)
