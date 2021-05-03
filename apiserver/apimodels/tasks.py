from typing import Sequence

import six
from jsonmodels import models
from jsonmodels.fields import StringField, BoolField, IntField, EmbeddedField
from jsonmodels.validators import Enum, Length

from apiserver.apimodels import DictField, ListField
from apiserver.apimodels.base import UpdateResponse
from apiserver.apimodels.batch import BatchRequest, BatchResponse
from apiserver.database.model.task.task import (
    TaskType,
    ArtifactModes,
    DEFAULT_ARTIFACT_MODE,
    TaskModelTypes,
)
from apiserver.database.utils import get_options


class ArtifactTypeData(models.Base):
    preview = StringField()
    content_type = StringField()
    data_hash = StringField()


class Artifact(models.Base):
    key = StringField(required=True)
    type = StringField(required=True)
    mode = StringField(
        validators=Enum(*get_options(ArtifactModes)), default=DEFAULT_ARTIFACT_MODE
    )
    uri = StringField()
    hash = StringField()
    content_size = IntField()
    timestamp = IntField()
    type_data = EmbeddedField(ArtifactTypeData)
    display_data = ListField([list])


class StartedResponse(UpdateResponse):
    started = IntField()


class EnqueueResponse(UpdateResponse):
    queued = IntField()


class DequeueResponse(UpdateResponse):
    dequeued = IntField()


class ResetResponse(UpdateResponse):
    deleted_indices = ListField(items_types=six.string_types)
    dequeued = DictField()
    frames = DictField()
    events = DictField()
    deleted_models = IntField()
    urls = DictField()


class TaskRequest(models.Base):
    task = StringField(required=True)


class TaskUpdateRequest(TaskRequest):
    force = BoolField(default=False)


class UpdateRequest(TaskUpdateRequest):
    status_reason = StringField(default="")
    status_message = StringField(default="")


class EnqueueRequest(UpdateRequest):
    queue = StringField()


class DeleteRequest(UpdateRequest):
    move_to_trash = BoolField(default=True)
    return_file_urls = BoolField(default=False)
    delete_output_models = BoolField(default=True)


class SetRequirementsRequest(TaskRequest):
    requirements = DictField(required=True)


class PublishRequest(UpdateRequest):
    publish_model = BoolField(default=True)


class PublishResponse(UpdateResponse):
    pass


class TaskData(models.Base):
    """
    This is a partial description of task can be updated incrementally
    """


class CreateRequest(TaskData):
    name = StringField(required=True)
    type = StringField(required=True, validators=Enum(*get_options(TaskType)))


class PingRequest(TaskRequest):
    pass


class GetTypesRequest(models.Base):
    projects = ListField(items_types=[str])


class TaskInputModel(models.Base):
    name = StringField()
    model = StringField()


class CloneRequest(TaskRequest):
    new_task_name = StringField()
    new_task_comment = StringField()
    new_task_tags = ListField([str])
    new_task_system_tags = ListField([str])
    new_task_parent = StringField()
    new_task_project = StringField()
    new_task_hyperparams = DictField()
    new_task_configuration = DictField()
    new_task_container = DictField()
    new_task_input_models = ListField([TaskInputModel])
    execution_overrides = DictField()
    validate_references = BoolField(default=False)
    new_project_name = StringField()


class AddOrUpdateArtifactsRequest(TaskUpdateRequest):
    artifacts = ListField([Artifact], validators=Length(minimum_value=1))


class ArtifactId(models.Base):
    key = StringField(required=True)
    mode = StringField(
        validators=Enum(*get_options(ArtifactModes)), default=DEFAULT_ARTIFACT_MODE
    )


class DeleteArtifactsRequest(TaskUpdateRequest):
    artifacts = ListField([ArtifactId], validators=Length(minimum_value=1))


class ResetRequest(UpdateRequest):
    clear_all = BoolField(default=False)
    return_file_urls = BoolField(default=False)
    delete_output_models = BoolField(default=True)


class MultiTaskRequest(models.Base):
    tasks = ListField([str], validators=Length(minimum_value=1))


class GetHyperParamsRequest(MultiTaskRequest):
    pass


class HyperParamItem(models.Base):
    section = StringField(required=True, validators=Length(minimum_value=1))
    name = StringField(required=True, validators=Length(minimum_value=1))
    value = StringField(required=True)
    type = StringField()
    description = StringField()


class ReplaceHyperparams(object):
    none = "none"
    section = "section"
    all = "all"


class EditHyperParamsRequest(TaskUpdateRequest):
    hyperparams: Sequence[HyperParamItem] = ListField(
        [HyperParamItem], validators=Length(minimum_value=1)
    )
    replace_hyperparams = StringField(
        validators=Enum(*get_options(ReplaceHyperparams)),
        default=ReplaceHyperparams.none,
    )


class HyperParamKey(models.Base):
    section = StringField(required=True, validators=Length(minimum_value=1))
    name = StringField(nullable=True)


class DeleteHyperParamsRequest(TaskUpdateRequest):
    hyperparams: Sequence[HyperParamKey] = ListField(
        [HyperParamKey], validators=Length(minimum_value=1)
    )


class GetConfigurationsRequest(MultiTaskRequest):
    names = ListField([str])


class GetConfigurationNamesRequest(MultiTaskRequest):
    skip_empty = BoolField(default=True)


class Configuration(models.Base):
    name = StringField(required=True, validators=Length(minimum_value=1))
    value = StringField(required=True)
    type = StringField()
    description = StringField()


class EditConfigurationRequest(TaskUpdateRequest):
    configuration: Sequence[Configuration] = ListField(
        [Configuration], validators=Length(minimum_value=1)
    )
    replace_configuration = BoolField(default=False)


class DeleteConfigurationRequest(TaskUpdateRequest):
    configuration: Sequence[str] = ListField([str], validators=Length(minimum_value=1))


class ArchiveRequest(MultiTaskRequest):
    status_reason = StringField(default="")
    status_message = StringField(default="")


class ArchiveResponse(models.Base):
    archived = IntField()


class TaskBatchRequest(BatchRequest):
    status_reason = StringField(default="")
    status_message = StringField(default="")


class StopManyRequest(TaskBatchRequest):
    force = BoolField(default=False)


class StopManyResponse(BatchResponse):
    stopped = IntField(required=True)


class ArchiveManyRequest(TaskBatchRequest):
    pass


class ArchiveManyResponse(BatchResponse):
    archived = IntField(required=True)


class EnqueueManyRequest(TaskBatchRequest):
    queue = StringField()


class EnqueueManyResponse(BatchResponse):
    queued = IntField()


class DeleteManyRequest(TaskBatchRequest):
    move_to_trash = BoolField(default=True)
    return_file_urls = BoolField(default=False)
    delete_output_models = BoolField(default=True)
    force = BoolField(default=False)


class ResetManyRequest(TaskBatchRequest):
    clear_all = BoolField(default=False)
    return_file_urls = BoolField(default=False)
    delete_output_models = BoolField(default=True)
    force = BoolField(default=False)


class PublishManyRequest(TaskBatchRequest):
    publish_model = BoolField(default=True)
    force = BoolField(default=False)


class AddUpdateModelRequest(TaskRequest):
    name = StringField(required=True)
    model = StringField(required=True)
    type = StringField(required=True, validators=Enum(*get_options(TaskModelTypes)))
    iteration = IntField()


class ModelItemKey(models.Base):
    name = StringField(required=True)
    type = StringField(required=True, validators=Enum(*get_options(TaskModelTypes)))


class DeleteModelsRequest(TaskRequest):
    models: Sequence[ModelItemKey] = ListField(
        [ModelItemKey], validators=Length(minimum_value=1)
    )
