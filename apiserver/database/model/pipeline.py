from mongoengine import StringField, DateTimeField, ListField,EmbeddedDocumentListField,EmbeddedDocument,IntField
from apiserver.database import Database, strict
from apiserver.database.fields import StrippedStringField, SafeSortedListField,SafeDictField
from apiserver.database.model import AttributedDocument
from apiserver.database.model.base import GetMixin
from apiserver.database.model.base import ProperDictMixin

class PipelineNode(EmbeddedDocument):
    

    id = StringField(primary_key=True)
    name = StrippedStringField()
    experiment = StringField(required=True)
    experiment_details = SafeDictField(default=dict)
    description = StringField()
    created = DateTimeField(required=True)
    last_update = DateTimeField()
    parameters = SafeSortedListField(SafeDictField(),default=list)
    code = StringField()


class Projectextendpipeline(AttributedDocument):

    get_all_query_options = GetMixin.QueryParameterOptions(
        pattern_fields=("name", "basename", "description"),
        list_fields=("tags", "system_tags", "id", "parent", "path"),
        range_fields=("last_update",),
    )

    meta = {
        "collection":'project',
        "db_alias": Database.backend,
        "strict": strict,
        "indexes": [
            "parent",
            "path",
            ("company", "name"),
            ("company", "basename"),
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
    basename = StrippedStringField(required=True)
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
    parameters = SafeSortedListField(SafeDictField(default=dict),required= True)
    flow_display = SafeDictField(default=dict)
    pipeline_code = StringField()
    pipeline_setting = SafeDictField(default=dict)
    nodes = EmbeddedDocumentListField(PipelineNode,exclude_by_default=True)

    def node_exists(self, node_name):
        return any(node.name == node_name for node in self.nodes)