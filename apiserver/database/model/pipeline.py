from mongoengine import StringField, DateTimeField, ListField,EmbeddedDocumentField,ReferenceField
import mongoengine
from apiserver.database import Database, strict
from apiserver.database.fields import StrippedStringField, SafeSortedListField,SafeDictField
from apiserver.database.model import AttributedDocument
from apiserver.database.model.base import GetMixin

class Pipeline(AttributedDocument):

    get_all_query_options = GetMixin.QueryParameterOptions(
        pattern_fields=("name", "basename", "description","project"),
        list_fields=("tags", "system_tags", "id", "parent", "path","parameters"),
        range_fields=("last_update",),
    )

    meta = {
        "db_alias": Database.backend,
        "strict": strict,
        "indexes": [
            "parent",
            "path",
            ("company", "name"),
            ("company", "basename"),
            {
                "name": "%s.pipeline.main_text_index" % Database.backend,
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
    basename = StrippedStringField(required=True)
    description = StringField()
    created = DateTimeField(required=True)
    tags = SafeSortedListField(StringField(required=True))
    system_tags = SafeSortedListField(StringField(required=True))
    default_output_destination = StrippedStringField()
    last_update = DateTimeField()
    company_origin = StringField(exclude_by_default=True)
    parent = StringField(reference_field="Project")
    path = ListField(StringField(required=True), exclude_by_default=True)
    parameters = SafeSortedListField(SafeDictField(default=dict),required= True)
    project = StringField()
    flow_display = SafeDictField(default=dict)

class PipelineStep(AttributedDocument):
    
    get_all_query_options = GetMixin.QueryParameterOptions(
        pattern_fields=("name", "basename", "description","pipeline_id","experiment"),
        list_fields=("tags", "system_tags", "id","parameters"),
        range_fields=("last_update",),
    )

    meta = {
        "db_alias": Database.backend,
        "strict": strict,
        "indexes": [
            "experiment",
            "pipeline_id",
            ("company", "name"),
            ("company", "basename"),
            {
                "name": "%s.pipelinestep.main_text_index" % Database.backend,
                "fields": ["$name", "$id", "$description"],
                "default_language": "english",
                "weights": {"name": 10, "id": 10, "description": 10},
            },
        ],
    }

    id = StringField(primary_key=True)
    name = StrippedStringField(
        required=True,
        unique_with = AttributedDocument.company.name,
        min_length=3,
        sparse=True,
    )
    basename = StrippedStringField(required=True)
    experiment = StringField(required=True)
    description = StringField()
    created = DateTimeField(required=True)
    tags = SafeSortedListField(StringField(required=True))
    system_tags = SafeSortedListField(StringField(required=True))
    default_output_destination = StrippedStringField()
    last_update = DateTimeField()
    company_origin = StringField(exclude_by_default=True)
    pipeline_id = StringField()
    parameters = SafeSortedListField(SafeDictField(),default=list)


