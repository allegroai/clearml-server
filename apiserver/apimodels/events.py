from enum import auto
from typing import Sequence, Optional

from jsonmodels import validators
from jsonmodels.fields import StringField, BoolField
from jsonmodels.models import Base
from jsonmodels.validators import Length, Min, Max

from apiserver.apimodels import ListField, IntField, ActualEnumField
from apiserver.bll.event.event_common import EventType
from apiserver.bll.event.scalar_key import ScalarKeyEnum
from apiserver.config_repo import config
from apiserver.utilities.stringenum import StringEnum


class HistogramRequestBase(Base):
    samples: int = IntField(default=6000, validators=[Min(1), Max(6000)])
    key: ScalarKeyEnum = ActualEnumField(ScalarKeyEnum, default=ScalarKeyEnum.iter)


class ScalarMetricsIterHistogramRequest(HistogramRequestBase):
    task: str = StringField(required=True)


class MultiTaskScalarMetricsIterHistogramRequest(HistogramRequestBase):
    tasks: Sequence[str] = ListField(
        items_types=str,
        validators=[
            Length(
                minimum_value=1,
                maximum_value=config.get(
                    "services.tasks.multi_task_histogram_limit", 10
                ),
            )
        ],
    )


class TaskMetric(Base):
    task: str = StringField(required=True)
    metric: str = StringField(default=None)


class DebugImagesRequest(Base):
    metrics: Sequence[TaskMetric] = ListField(
        items_types=TaskMetric, validators=[Length(minimum_value=1)]
    )
    iters: int = IntField(default=1, validators=validators.Min(1))
    navigate_earlier: bool = BoolField(default=True)
    refresh: bool = BoolField(default=False)
    scroll_id: str = StringField()


class TaskMetricVariant(Base):
    task: str = StringField(required=True)
    metric: str = StringField(required=True)
    variant: str = StringField(required=True)


class GetDebugImageSampleRequest(TaskMetricVariant):
    iteration: Optional[int] = IntField()
    scroll_id: Optional[str] = StringField()
    refresh: bool = BoolField(default=False)


class NextDebugImageSampleRequest(Base):
    task: str = StringField(required=True)
    scroll_id: Optional[str] = StringField()
    navigate_earlier: bool = BoolField(default=True)


class LogOrderEnum(StringEnum):
    asc = auto()
    desc = auto()


class LogEventsRequest(Base):
    task: str = StringField(required=True)
    batch_size: int = IntField(default=500)
    navigate_earlier: bool = BoolField(default=True)
    from_timestamp: Optional[int] = IntField()
    order: Optional[str] = ActualEnumField(LogOrderEnum)


class IterationEvents(Base):
    iter: int = IntField()
    events: Sequence[dict] = ListField(items_types=dict)


class MetricEvents(Base):
    task: str = StringField()
    metric: str = StringField()
    iterations: Sequence[IterationEvents] = ListField(items_types=IterationEvents)


class DebugImageResponse(Base):
    metrics: Sequence[MetricEvents] = ListField(items_types=MetricEvents)
    scroll_id: str = StringField()


class TaskMetricsRequest(Base):
    tasks: Sequence[str] = ListField(
        items_types=str, validators=[Length(minimum_value=1)]
    )
    event_type: EventType = ActualEnumField(EventType, required=True)
