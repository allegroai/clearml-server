from jsonmodels import models, fields

from apiserver.apimodels import ListField


class Arg(models.Base):
    name = fields.StringField(required=True)
    value = fields.StringField(required=True)


class StartPipelineRequest(models.Base):
    task = fields.StringField(required=True)
    queue = fields.StringField(required=True)
    args = ListField(Arg)


class StartPipelineResponse(models.Base):
    pipeline = fields.StringField(required=True)
    enqueued = fields.BoolField(required=True)
