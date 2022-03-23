from mongoengine import (
    StringField,
    DateTimeField,
    BooleanField,
    EmbeddedDocumentField,
)

from apiserver.database import Database, strict
from apiserver.database.fields import (
    StrippedStringField,
    SafeDictField,
    SafeSortedListField,
    SafeMapField,
)
from apiserver.database.model import AttributedDocument
from apiserver.database.model.base import GetMixin
from apiserver.database.model.metadata import MetadataItem
from apiserver.database.model.model_labels import ModelLabels
from apiserver.database.model.project import Project
from apiserver.database.model.task.task import Task


class Model(AttributedDocument):
    _field_collation_overrides = {
        "metadata.": AttributedDocument._numeric_locale,
    }

    meta = {
        "db_alias": Database.backend,
        "strict": strict,
        "indexes": [
            "parent",
            "project",
            "task",
            "last_update",
            ("company", "framework"),
            ("company", "name"),
            ("company", "user"),
            {
                "name": "%s.model.main_text_index" % Database.backend,
                "fields": ["$name", "$id", "$comment", "$parent", "$task", "$project"],
                "default_language": "english",
                "weights": {
                    "name": 10,
                    "id": 10,
                    "comment": 10,
                    "parent": 5,
                    "task": 3,
                    "project": 3,
                },
            },
        ],
    }
    get_all_query_options = GetMixin.QueryParameterOptions(
        pattern_fields=("name", "comment"),
        fields=("ready",),
        list_fields=(
            "tags",
            "system_tags",
            "framework",
            "uri",
            "id",
            "user",
            "project",
            "task",
            "parent",
            "metadata.*",
        ),
        datetime_fields=("last_update",),
    )

    id = StringField(primary_key=True)
    name = StrippedStringField(user_set_allowed=True, min_length=3)
    parent = StringField(reference_field="Model", required=False)
    project = StringField(reference_field=Project, user_set_allowed=True)
    created = DateTimeField(required=True, user_set_allowed=True)
    task = StringField(reference_field=Task)
    comment = StringField(user_set_allowed=True)
    tags = SafeSortedListField(StringField(required=True), user_set_allowed=True)
    system_tags = SafeSortedListField(StringField(required=True), user_set_allowed=True)
    uri = StrippedStringField(default="", user_set_allowed=True)
    framework = StringField()
    design = SafeDictField()
    labels = ModelLabels()
    ready = BooleanField(required=True)
    last_update = DateTimeField()
    ui_cache = SafeDictField(
        default=dict, user_set_allowed=True, exclude_by_default=True
    )
    company_origin = StringField(exclude_by_default=True)
    metadata = SafeMapField(
        field=EmbeddedDocumentField(MetadataItem), user_set_allowed=True
    )
