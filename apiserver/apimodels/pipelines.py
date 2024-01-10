from jsonmodels import models, fields
from jsonmodels.validators import Length

from apiserver.apimodels import ListField


class Arg(models.Base):
    name = fields.StringField(required=True)
    value = fields.StringField(required=True)


class DeleteRunsRequest(models.Base):
    project = fields.StringField(required=True)
    ids = ListField([str], required=True, validators=[Length(1)])


class StartPipelineRequest(models.Base):
    task = fields.StringField(required=True)
    queue = fields.StringField(required=True)
    args = ListField(Arg)
    verify_watched_queue = fields.BoolField(default=False)
