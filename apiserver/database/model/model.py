from mongoengine import Document, StringField, DateTimeField, BooleanField

from database import Database, strict
from database.fields import StrippedStringField, SafeDictField, SafeSortedListField
from database.model import DbModelMixin
from database.model.base import GetMixin
from database.model.model_labels import ModelLabels
from database.model.company import Company
from database.model.project import Project
from database.model.task.task import Task
from database.model.user import User


class Model(DbModelMixin, Document):
    meta = {
        "db_alias": Database.backend,
        "strict": strict,
        "indexes": [
            "parent",
            "project",
            "task",
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
        ),
    )

    id = StringField(primary_key=True)
    name = StrippedStringField(user_set_allowed=True, min_length=3)
    parent = StringField(reference_field="Model", required=False)
    user = StringField(required=True, reference_field=User)
    company = StringField(required=True, reference_field=Company)
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
    ui_cache = SafeDictField(
        default=dict, user_set_allowed=True, exclude_by_default=True
    )
    company_origin = StringField(exclude_by_default=True)
