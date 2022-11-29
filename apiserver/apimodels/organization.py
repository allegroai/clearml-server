from jsonmodels import fields, models

from apiserver.apimodels import DictField


class Filter(models.Base):
    tags = fields.ListField([str])
    system_tags = fields.ListField([str])


class TagsRequest(models.Base):
    include_system = fields.BoolField(default=False)
    filter = fields.EmbeddedField(Filter)


class EntitiesCountRequest(models.Base):
    projects = DictField()
    tasks = DictField()
    models = DictField()
    pipelines = DictField()
    datasets = DictField()
    active_users = fields.ListField(str)
    search_hidden = fields.BoolField(default=False)
