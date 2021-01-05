from mongoengine import (
    StringField,
    EmbeddedDocumentField,
    EmbeddedDocument,
    DateTimeField,
    IntField,
    ListField,
    LongField,
)

from database import Database, strict
from database.fields import (
    StrippedStringField,
    SafeMapField,
    SafeDictField,
    UnionField,
    EmbeddedDocumentSortedListField,
    SafeSortedListField,
)
from database.model import AttributedDocument
from database.model.base import ProperDictMixin, GetMixin
from database.model.model_labels import ModelLabels
from database.model.project import Project
from database.utils import get_options
from .metrics import MetricEvent, MetricEventStats
from .output import Output

DEFAULT_LAST_ITERATION = 0


class TaskStatus(object):
    created = "created"
    queued = "queued"
    in_progress = "in_progress"
    stopped = "stopped"
    publishing = "publishing"
    published = "published"
    closed = "closed"
    failed = "failed"
    completed = "completed"
    unknown = "unknown"


class TaskStatusMessage(object):
    stopping = "stopping"


class TaskSystemTags(object):
    development = "development"


class Script(EmbeddedDocument, ProperDictMixin):
    binary = StringField(default="python")
    repository = StringField(default="")
    tag = StringField()
    branch = StringField()
    version_num = StringField()
    entry_point = StringField(default="")
    working_dir = StringField()
    requirements = SafeDictField()
    diff = StringField()


class ArtifactTypeData(EmbeddedDocument):
    preview = StringField()
    content_type = StringField()
    data_hash = StringField()


class ArtifactModes:
    input = "input"
    output = "output"


class Artifact(EmbeddedDocument):
    key = StringField(required=True)
    type = StringField(required=True)
    mode = StringField(choices=get_options(ArtifactModes), default=ArtifactModes.output)
    uri = StringField()
    hash = StringField()
    content_size = LongField()
    timestamp = LongField()
    type_data = EmbeddedDocumentField(ArtifactTypeData)
    display_data = SafeSortedListField(ListField(UnionField((int, float, str))))


class ParamsItem(EmbeddedDocument, ProperDictMixin):
    section = StringField(required=True)
    name = StringField(required=True)
    value = StringField(required=True)
    type = StringField()
    description = StringField()


class ConfigurationItem(EmbeddedDocument, ProperDictMixin):
    name = StringField(required=True)
    value = StringField(required=True)
    type = StringField()
    description = StringField()


class Execution(EmbeddedDocument, ProperDictMixin):
    meta = {"strict": strict}
    test_split = IntField(default=0)
    parameters = SafeDictField(default=dict)
    model = StringField(reference_field="Model")
    model_desc = SafeMapField(StringField(default=""))
    model_labels = ModelLabels()
    framework = StringField()
    artifacts = EmbeddedDocumentSortedListField(Artifact)
    docker_cmd = StringField()
    queue = StringField()
    """ Queue ID where task was queued """


class TaskType(object):
    training = "training"
    testing = "testing"
    inference = "inference"
    data_processing = "data_processing"
    application = "application"
    monitor = "monitor"
    controller = "controller"
    optimizer = "optimizer"
    service = "service"
    qc = "qc"
    custom = "custom"


external_task_types = set(get_options(TaskType))


class Task(AttributedDocument):
    _numeric_locale = {"locale": "en_US", "numericOrdering": True}
    _field_collation_overrides = {
        "execution.parameters.": _numeric_locale,
        "last_metrics.": _numeric_locale,
        "hyperparams.": _numeric_locale,
        "configuration.": _numeric_locale,
    }

    meta = {
        "db_alias": Database.backend,
        "strict": strict,
        "indexes": [
            "created",
            "started",
            "completed",
            "parent",
            "project",
            ("company", "name"),
            ("company", "user"),
            ("company", "type", "system_tags", "status"),
            ("company", "project", "type", "system_tags", "status"),
            ("status", "last_update"),  # for maintenance tasks
            {
                "name": "%s.task.main_text_index" % Database.backend,
                "fields": [
                    "$name",
                    "$id",
                    "$comment",
                    "$execution.model",
                    "$output.model",
                    "$script.repository",
                    "$script.entry_point",
                ],
                "default_language": "english",
                "weights": {
                    "name": 10,
                    "id": 10,
                    "comment": 10,
                    "execution.model": 2,
                    "output.model": 2,
                    "script.repository": 1,
                    "script.entry_point": 1,
                },
            },
        ],
    }
    get_all_query_options = GetMixin.QueryParameterOptions(
        list_fields=("id", "user", "tags", "system_tags", "type", "status", "project"),
        datetime_fields=("status_changed",),
        pattern_fields=("name", "comment"),
        fields=("parent",),
    )

    id = StringField(primary_key=True)
    name = StrippedStringField(
        required=True, user_set_allowed=True, sparse=False, min_length=3
    )

    type = StringField(required=True, choices=get_options(TaskType))
    status = StringField(default=TaskStatus.created, choices=get_options(TaskStatus))
    status_reason = StringField()
    status_message = StringField()
    status_changed = DateTimeField()
    comment = StringField(user_set_allowed=True)
    created = DateTimeField(required=True, user_set_allowed=True)
    started = DateTimeField()
    completed = DateTimeField()
    published = DateTimeField()
    parent = StringField()
    project = StringField(reference_field=Project, user_set_allowed=True)
    output: Output = EmbeddedDocumentField(Output, default=Output)
    execution: Execution = EmbeddedDocumentField(Execution, default=Execution)
    tags = SafeSortedListField(StringField(required=True), user_set_allowed=True)
    system_tags = SafeSortedListField(StringField(required=True), user_set_allowed=True)
    script: Script = EmbeddedDocumentField(Script, default=Script)
    last_worker = StringField()
    last_worker_report = DateTimeField()
    last_update = DateTimeField()
    last_iteration = IntField(default=DEFAULT_LAST_ITERATION)
    last_metrics = SafeMapField(field=SafeMapField(EmbeddedDocumentField(MetricEvent)))
    metric_stats = SafeMapField(field=EmbeddedDocumentField(MetricEventStats))
    company_origin = StringField(exclude_by_default=True)
    duration = IntField()  # task duration in seconds
    hyperparams = SafeMapField(field=SafeMapField(EmbeddedDocumentField(ParamsItem)))
    configuration = SafeMapField(field=EmbeddedDocumentField(ConfigurationItem))
    runtime = SafeDictField(default=dict)

    def get_index_company(self) -> str:
        """
        Returns the company ID used for locating indices containing task data.
        In case the task has a valid company, this is the company ID.
        Otherwise, if the task has a company_origin, this is a task that has been made public and the
         origin company should be used.
        Otherwise, an empty company is used.
        """
        return self.company or self.company_origin or ""
