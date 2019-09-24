from typing import Sequence

from jsonmodels.fields import StringField
from jsonmodels.models import Base

from apimodels import ListField, IntField, ActualEnumField
from bll.event.scalar_key import ScalarKeyEnum


class HistogramRequestBase(Base):
    samples: int = IntField(default=10000)
    key: ScalarKeyEnum = ActualEnumField(ScalarKeyEnum, default=ScalarKeyEnum.iter)


class ScalarMetricsIterHistogramRequest(HistogramRequestBase):
    task: str = StringField(required=True)


class MultiTaskScalarMetricsIterHistogramRequest(HistogramRequestBase):
    tasks: Sequence[str] = ListField(items_types=str)
