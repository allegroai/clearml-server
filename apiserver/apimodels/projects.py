from jsonmodels import models, fields

from apiserver.apimodels import ListField, ActualEnumField
from apiserver.apimodels.organization import TagsRequest
from apiserver.database.model import EntityVisibility


class ProjectRequest(models.Base):
    project = fields.StringField(required=True)


class MergeRequest(ProjectRequest):
    destination_project = fields.StringField()


class MoveRequest(ProjectRequest):
    new_location = fields.StringField()


class DeleteRequest(ProjectRequest):
    force = fields.BoolField(default=False)
    delete_contents = fields.BoolField(default=False)


class ProjectOrNoneRequest(models.Base):
    project = fields.StringField()
    include_subprojects = fields.BoolField(default=True)


class GetHyperParamRequest(ProjectOrNoneRequest):
    page = fields.IntField(default=0)
    page_size = fields.IntField(default=500)


class ProjectTagsRequest(TagsRequest):
    projects = ListField(str)


class MultiProjectRequest(models.Base):
    projects = fields.ListField(str)
    include_subprojects = fields.BoolField(default=True)


class ProjectTaskParentsRequest(MultiProjectRequest):
    tasks_state = ActualEnumField(EntityVisibility)


class ProjectHyperparamValuesRequest(MultiProjectRequest):
    section = fields.StringField(required=True)
    name = fields.StringField(required=True)
    allow_public = fields.BoolField(default=True)


class ProjectsGetRequest(models.Base):
    include_stats = fields.BoolField(default=False)
    stats_for_state = ActualEnumField(EntityVisibility, default=EntityVisibility.active)
    non_public = fields.BoolField(default=False)
    active_users = fields.ListField(str)
    check_own_contents = fields.BoolField(default=False)
    shallow_search = fields.BoolField(default=False)
