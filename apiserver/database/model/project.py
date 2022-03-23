from mongoengine import StringField, DateTimeField, IntField, ListField

from apiserver.database import Database, strict
from apiserver.database.fields import StrippedStringField, SafeSortedListField
from apiserver.database.model import AttributedDocument
from apiserver.database.model.base import GetMixin


class Project(AttributedDocument):

    get_all_query_options = GetMixin.QueryParameterOptions(
        pattern_fields=("name", "description"),
        list_fields=("tags", "system_tags", "id", "parent", "path"),
        range_fields=("last_update",),
    )

    meta = {
        "db_alias": Database.backend,
        "strict": strict,
        "indexes": [
            "parent",
            "path",
            ("company", "name"),
            {
                "name": "%s.project.main_text_index" % Database.backend,
                "fields": ["$name", "$id", "$description"],
                "default_language": "english",
                "weights": {"name": 10, "id": 10, "description": 10},
            },
        ],
    }

    id = StringField(primary_key=True)
    name = StrippedStringField(
        required=True,
        unique_with=AttributedDocument.company.name,
        min_length=3,
        sparse=True,
    )
    description = StringField()
    created = DateTimeField(required=True)
    tags = SafeSortedListField(StringField(required=True))
    system_tags = SafeSortedListField(StringField(required=True))
    default_output_destination = StrippedStringField()
    last_update = DateTimeField()
    featured = IntField(default=9999)
    logo_url = StringField()
    logo_blob = StringField(exclude_by_default=True)
    company_origin = StringField(exclude_by_default=True)
    parent = StringField(reference_field="Project")
    path = ListField(StringField(required=True), exclude_by_default=True)
