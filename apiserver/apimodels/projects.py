from jsonmodels import models, fields

from apiserver.apimodels import ListField, ActualEnumField
from apiserver.apimodels.organization import TagsRequest
from apiserver.database.model import EntityVisibility


class ProjectReq(models.Base):
    project = fields.StringField(required=True)


class DeleteRequest(ProjectReq):
    force = fields.BoolField(default=False)
    delete_contents = fields.BoolField(default=False)


class GetHyperParamReq(ProjectReq):
    page = fields.IntField(default=0)
    page_size = fields.IntField(default=500)


class ProjectTagsRequest(TagsRequest):
    projects = ListField(str)


class MultiProjectReq(models.Base):
    projects = fields.ListField(str)


class ProjectTaskParentsRequest(MultiProjectReq):
    tasks_state = ActualEnumField(EntityVisibility)


class ProjectHyperparamValuesRequest(MultiProjectReq):
    section = fields.StringField(required=True)
    name = fields.StringField(required=True)
    allow_public = fields.BoolField(default=True)


class ProjectsGetRequest(models.Base):
    include_stats = fields.BoolField(default=False)
    stats_for_state = ActualEnumField(EntityVisibility, default=EntityVisibility.active)
    non_public = fields.BoolField(default=False)
    active_users = fields.ListField(str)
