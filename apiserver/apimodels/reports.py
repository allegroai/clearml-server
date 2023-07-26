from typing import Sequence

from jsonmodels import validators
from jsonmodels.fields import StringField, ListField, BoolField, EmbeddedField, IntField
from jsonmodels.models import Base
from jsonmodels.validators import Length

from apiserver.apimodels.events import MetricVariants, HistogramRequestBase


class UpdateReportRequest(Base):
    task = StringField(required=True)
    name = StringField(nullable=True, validators=Length(minimum_value=3))
    tags = ListField(items_types=[str])
    comment = StringField()
    report = StringField()
    report_assets = ListField(items_types=[str])


class CreateReportRequest(Base):
    name = StringField(required=True, validators=Length(minimum_value=3))
    tags = ListField(items_types=[str])
    comment = StringField()
    report = StringField()
    project = StringField()
    report_assets = ListField(items_types=[str])


class PublishReportRequest(Base):
    task = StringField(required=True)
    message = StringField(default="")


class ArchiveReportRequest(Base):
    task = StringField(required=True)
    message = StringField(default="")


class ShareReportRequest(Base):
    task = StringField(required=True)
    share = BoolField(default=True)


class DeleteReportRequest(Base):
    task = StringField(required=True)
    force = BoolField(default=False)


class MoveReportRequest(Base):
    task = StringField(required=True)
    project = StringField()
    project_name = StringField()


class EventsRequest(Base):
    iters = IntField(default=1, validators=validators.Min(1))
    metrics: Sequence[MetricVariants] = ListField(items_types=MetricVariants)


class PlotEventsRequest(EventsRequest):
    last_iters_per_task_metric: bool = BoolField(default=True)


class ScalarMetricsIterHistogram(HistogramRequestBase):
    metrics: Sequence[MetricVariants] = ListField(items_types=MetricVariants)


class SingleValueMetrics(Base):
    pass


class GetTasksDataRequest(Base):
    debug_images: EventsRequest = EmbeddedField(EventsRequest)
    plots: PlotEventsRequest = EmbeddedField(PlotEventsRequest)
    scalar_metrics_iter_histogram: ScalarMetricsIterHistogram = EmbeddedField(
        ScalarMetricsIterHistogram
    )
    single_value_metrics: SingleValueMetrics = EmbeddedField(SingleValueMetrics)
    allow_public = BoolField(default=True)
    model_events: bool = BoolField(default=False)


class GetAllRequest(Base):
    allow_public = BoolField(default=True)
