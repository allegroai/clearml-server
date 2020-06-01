from jsonmodels import fields, models


class Filter(models.Base):
    system_tags = fields.ListField([str])


class TagsRequest(models.Base):
    include_system = fields.BoolField(default=False)
    filter = fields.EmbeddedField(Filter)
