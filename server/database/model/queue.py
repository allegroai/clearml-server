from mongoengine import (
    Document,
    EmbeddedDocument,
    StringField,
    DateTimeField,
    EmbeddedDocumentListField,
    ListField,
)

from database import Database, strict
from database.fields import StrippedStringField
from database.model import DbModelMixin
from database.model.base import ProperDictMixin, GetMixin
from database.model.company import Company
from database.model.task.task import Task


class Entry(EmbeddedDocument, ProperDictMixin):
    """ Entry representing a task waiting in the queue """
    task = StringField(required=True, reference_field=Task)
    ''' Task ID '''
    added = DateTimeField(required=True)
    ''' Added to the queue '''


class Queue(DbModelMixin, Document):

    get_all_query_options = GetMixin.QueryParameterOptions(
        pattern_fields=("name",),
        list_fields=("tags", "system_tags", "id"),
    )

    meta = {
        'db_alias': Database.backend,
        'strict': strict,
    }

    id = StringField(primary_key=True)
    name = StrippedStringField(
        required=True, unique_with="company", min_length=3, user_set_allowed=True
    )
    company = StringField(required=True, reference_field=Company)
    created = DateTimeField(required=True)
    tags = ListField(StringField(required=True), default=list, user_set_allowed=True)
    system_tags = ListField(StringField(required=True), user_set_allowed=True)
    entries = EmbeddedDocumentListField(Entry, default=list)
    last_update = DateTimeField()
