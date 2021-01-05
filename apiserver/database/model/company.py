from mongoengine import (
    Document,
    EmbeddedDocument,
    EmbeddedDocumentField,
    StringField,
    Q,
    BooleanField,
    DateTimeField,
)

from apiserver.database import Database, strict
from apiserver.database.fields import StrippedStringField
from apiserver.database.model import DbModelMixin


class ReportStatsOption(EmbeddedDocument):
    enabled = BooleanField(default=False)  # opt-in for statistics reporting
    enabled_version = StringField()  # server version when enabled
    enabled_time = DateTimeField()  # time when enabled
    enabled_user = StringField()  # ID of user who enabled


class CompanyDefaults(EmbeddedDocument):
    cluster = StringField()
    stats_option = EmbeddedDocumentField(ReportStatsOption, default=ReportStatsOption)


class Company(DbModelMixin, Document):
    meta = {"db_alias": Database.backend, "strict": strict}

    id = StringField(primary_key=True)
    name = StrippedStringField(min_length=3)
    defaults = EmbeddedDocumentField(CompanyDefaults, default=CompanyDefaults)

    @classmethod
    def _prepare_perm_query(cls, company, allow_public=False):
        """ Override default behavior since a 'company' constraint is not supported for this document... """
        return Q()
