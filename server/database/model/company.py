from mongoengine import Document, EmbeddedDocument, EmbeddedDocumentField, StringField, Q

from database import Database, strict
from database.fields import StrippedStringField
from database.model import DbModelMixin


class CompanyDefaults(EmbeddedDocument):
    cluster = StringField()


class Company(DbModelMixin, Document):
    meta = {
        'db_alias': Database.backend,
        'strict': strict,
    }

    id = StringField(primary_key=True)
    name = StrippedStringField(unique=True, min_length=3)
    defaults = EmbeddedDocumentField(CompanyDefaults)

    @classmethod
    def _prepare_perm_query(cls, company, allow_public=False):
        """ Override default behavior since a 'company' constraint is not supported for this document... """
        return Q()
