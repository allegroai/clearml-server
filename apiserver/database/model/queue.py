from typing import Sequence

from mongoengine import (
    Document,
    EmbeddedDocument,
    StringField,
    DateTimeField,
    EmbeddedDocumentListField,
)

from apiserver.database import Database, strict
from apiserver.database.fields import StrippedStringField, SafeSortedListField
from apiserver.database.model import DbModelMixin
from apiserver.database.model.base import ProperDictMixin, GetMixin
from apiserver.database.model.company import Company
from apiserver.database.model.metadata import MetadataItem
from apiserver.database.model.task.task import Task


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
        "indexes": ["metadata.key", "metadata.type"],
    }

    id = StringField(primary_key=True)
    name = StrippedStringField(
        required=True, unique_with="company", min_length=3, user_set_allowed=True
    )
    company = StringField(required=True, reference_field=Company)
    created = DateTimeField(required=True)
    tags = SafeSortedListField(StringField(required=True), default=list, user_set_allowed=True)
    system_tags = SafeSortedListField(StringField(required=True), user_set_allowed=True)
    entries = EmbeddedDocumentListField(Entry, default=list)
    last_update = DateTimeField()
    metadata: Sequence[MetadataItem] = EmbeddedDocumentListField(
        MetadataItem, default=list, user_set_allowed=True
    )
