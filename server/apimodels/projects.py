from jsonmodels import models, fields

from apimodels import ListField
from apimodels.organization import TagsRequest


class ProjectReq(models.Base):
    project = fields.StringField()


class GetHyperParamReq(ProjectReq):
    page = fields.IntField(default=0)
    page_size = fields.IntField(default=500)


class ProjectTagsRequest(TagsRequest):
    projects = ListField(str)
