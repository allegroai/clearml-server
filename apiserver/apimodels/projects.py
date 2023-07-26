from enum import Enum, auto

from jsonmodels import models, fields

from apiserver.apimodels import ListField, ActualEnumField, DictField
from apiserver.apimodels.organization import TagsRequest
from apiserver.database.model import EntityVisibility
from apiserver.utilities.stringenum import StringEnum


class ProjectRequest(models.Base):
    project = fields.StringField(required=True)


class MergeRequest(ProjectRequest):
    destination_project = fields.StringField()


class MoveRequest(ProjectRequest):
    new_location = fields.StringField()


class DeleteRequest(ProjectRequest):
    force = fields.BoolField(default=False)
    delete_contents = fields.BoolField(default=False)
    delete_external_artifacts = fields.BoolField(default=True)


class ProjectOrNoneRequest(models.Base):
    project = fields.StringField()
    include_subprojects = fields.BoolField(default=True)


class GetUniqueMetricsRequest(ProjectOrNoneRequest):
    model_metrics = fields.BoolField(default=False)


class GetParamsRequest(ProjectOrNoneRequest):
    page = fields.IntField(default=0)
    page_size = fields.IntField(default=500)


class ProjectTagsRequest(TagsRequest):
    projects = ListField(str)


class MultiProjectRequest(models.Base):
    projects = fields.ListField(str)
    include_subprojects = fields.BoolField(default=True)


class ProjectTaskParentsRequest(MultiProjectRequest):
    tasks_state = ActualEnumField(EntityVisibility)
    task_name = fields.StringField()


class EntityTypeEnum(StringEnum):
    task = auto()
    model = auto()


class ProjectUserNamesRequest(MultiProjectRequest):
    entity = ActualEnumField(EntityTypeEnum, default=EntityTypeEnum.task)


class MultiProjectPagedRequest(MultiProjectRequest):
    allow_public = fields.BoolField(default=True)
    page = fields.IntField(default=0)
    page_size = fields.IntField(default=500)


class ProjectHyperparamValuesRequest(MultiProjectPagedRequest):
    section = fields.StringField(required=True)
    name = fields.StringField(required=True)


class ProjectModelMetadataValuesRequest(MultiProjectPagedRequest):
    key = fields.StringField(required=True)


class ProjectChildrenType(Enum):
    pipeline = "pipeline"
    report = "report"
    dataset = "dataset"


class ProjectsGetRequest(models.Base):
    include_dataset_stats = fields.BoolField(default=False)
    include_stats = fields.BoolField(default=False)
    include_stats_filter = DictField()
    stats_with_children = fields.BoolField(default=True)
    stats_for_state = ActualEnumField(EntityVisibility, default=EntityVisibility.active)
    non_public = fields.BoolField(default=False)  # legacy, use allow_public instead
    active_users = fields.ListField(str)
    check_own_contents = fields.BoolField(default=False)
    shallow_search = fields.BoolField(default=False)
    search_hidden = fields.BoolField(default=False)
    allow_public = fields.BoolField(default=True)
    children_type = ActualEnumField(ProjectChildrenType)
    children_tags = fields.ListField(str)
