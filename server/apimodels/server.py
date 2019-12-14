from jsonmodels.fields import BoolField, DateTimeField, StringField
from jsonmodels.models import Base


class ReportStatsOptionRequest(Base):
    enabled = BoolField(default=None, nullable=True)


class ReportStatsOptionResponse(Base):
    supported = BoolField(default=True)
    enabled = BoolField()
    enabled_time = DateTimeField(nullable=True)
    enabled_version = StringField(nullable=True)
    enabled_user = StringField(nullable=True)
    current_version = StringField()
