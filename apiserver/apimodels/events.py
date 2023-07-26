from enum import auto
from typing import Sequence, Optional

from jsonmodels import validators
from jsonmodels.fields import StringField, BoolField, EmbeddedField
from jsonmodels.models import Base
from jsonmodels.validators import Length, Min, Max

from apiserver.apimodels import ListField, IntField, ActualEnumField
from apiserver.bll.event.event_common import EventType
from apiserver.bll.event.scalar_key import ScalarKeyEnum
from apiserver.config_repo import config
from apiserver.utilities.stringenum import StringEnum


class HistogramRequestBase(Base):
    samples: int = IntField(default=2000, validators=[Min(1), Max(6000)])
    key: ScalarKeyEnum = ActualEnumField(ScalarKeyEnum, default=ScalarKeyEnum.iter)


class MetricVariants(Base):
    metric: str = StringField(required=True)
    variants: Sequence[str] = ListField(items_types=str)


class ScalarMetricsIterHistogramRequest(HistogramRequestBase):
    task: str = StringField(required=True)
    metrics: Sequence[MetricVariants] = ListField(items_types=MetricVariants)
    model_events: bool = BoolField(default=False)


class MultiTaskScalarMetricsIterHistogramRequest(HistogramRequestBase):
    tasks: Sequence[str] = ListField(
        items_types=str,
        validators=[
            Length(
                minimum_value=1,
                maximum_value=config.get(
                    "services.tasks.multi_task_histogram_limit", 100
                ),
            )
        ],
    )
    model_events: bool = BoolField(default=False)


class TaskMetric(Base):
    task: str = StringField(required=True)
    metric: str = StringField(default=None)
    variants: Sequence[str] = ListField(items_types=str)


class MetricEventsRequest(Base):
    metrics: Sequence[TaskMetric] = ListField(
        items_types=TaskMetric, validators=[Length(minimum_value=1)]
    )
    iters: int = IntField(default=1, validators=validators.Min(1))
    navigate_earlier: bool = BoolField(default=True)
    refresh: bool = BoolField(default=False)
    scroll_id: str = StringField()
    model_events: bool = BoolField()


class GetVariantSampleRequest(Base):
    task: str = StringField(required=True)
    metric: str = StringField(required=True)
    variant: str = StringField(required=True)
    iteration: Optional[int] = IntField()
    refresh: bool = BoolField(default=False)
    scroll_id: Optional[str] = StringField()
    navigate_current_metric: bool = BoolField(default=True)
    model_events: bool = BoolField(default=False)


class GetMetricSamplesRequest(Base):
    task: str = StringField(required=True)
    metric: str = StringField(required=True)
    iteration: Optional[int] = IntField()
    refresh: bool = BoolField(default=False)
    scroll_id: Optional[str] = StringField()
    navigate_current_metric: bool = BoolField(default=True)
    model_events: bool = BoolField(default=False)


class NextHistorySampleRequest(Base):
    task: str = StringField(required=True)
    scroll_id: Optional[str] = StringField()
    navigate_earlier: bool = BoolField(default=True)
    next_iteration: bool = BoolField(default=False)
    model_events: bool = BoolField(default=False)


class LogOrderEnum(StringEnum):
    asc = auto()
    desc = auto()


class TaskEventsRequestBase(Base):
    task: str = StringField(required=True)
    batch_size: int = IntField(default=500)


class TaskEventsRequest(TaskEventsRequestBase):
    metrics: Sequence[MetricVariants] = ListField(items_types=MetricVariants)
    event_type: EventType = ActualEnumField(EventType, default=EventType.all)
    order: Optional[str] = ActualEnumField(LogOrderEnum, default=LogOrderEnum.asc)
    scroll_id: str = StringField()
    count_total: bool = BoolField(default=True)
    model_events: bool = BoolField(default=False)


class LogEventsRequest(TaskEventsRequestBase):
    batch_size: int = IntField(default=5000)
    navigate_earlier: bool = BoolField(default=True)
    from_timestamp: Optional[int] = IntField()
    order: Optional[str] = ActualEnumField(LogOrderEnum)


class ScalarMetricsIterRawRequest(TaskEventsRequestBase):
    batch_size: int = IntField()
    key: ScalarKeyEnum = ActualEnumField(ScalarKeyEnum, default=ScalarKeyEnum.iter)
    metric: MetricVariants = EmbeddedField(MetricVariants, required=True)
    count_total: bool = BoolField(default=False)
    scroll_id: str = StringField()
    model_events: bool = BoolField(default=False)


class IterationEvents(Base):
    iter: int = IntField()
    events: Sequence[dict] = ListField(items_types=dict)


class MetricEvents(Base):
    task: str = StringField()
    iterations: Sequence[IterationEvents] = ListField(items_types=IterationEvents)


class MetricEventsResponse(Base):
    metrics: Sequence[MetricEvents] = ListField(items_types=MetricEvents)
    scroll_id: str = StringField()


class MultiTasksRequestBase(Base):
    tasks: Sequence[str] = ListField(
        items_types=str, validators=[Length(minimum_value=1)]
    )
    model_events: bool = BoolField(default=False)


class SingleValueMetricsRequest(MultiTasksRequestBase):
    pass


class TaskMetricsRequest(MultiTasksRequestBase):
    event_type: EventType = ActualEnumField(EventType, required=True)


class MultiTaskPlotsRequest(MultiTasksRequestBase):
    iters: int = IntField(default=1)
    scroll_id: str = StringField()
    no_scroll: bool = BoolField(default=False)
    last_iters_per_task_metric: bool = BoolField(default=True)


class TaskPlotsRequest(Base):
    task: str = StringField(required=True)
    iters: int = IntField(default=1)
    scroll_id: str = StringField()
    no_scroll: bool = BoolField(default=False)
    metrics: Sequence[MetricVariants] = ListField(items_types=MetricVariants)
    model_events: bool = BoolField(default=False)


class ClearScrollRequest(Base):
    scroll_id: str = StringField()


class ClearTaskLogRequest(Base):
    task: str = StringField(required=True)
    threshold_sec = IntField()
    allow_locked = BoolField(default=False)
