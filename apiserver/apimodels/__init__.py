from __future__ import absolute_import

from textwrap import shorten

from luqum.exceptions import ParseError
from luqum.parser import parser
from validators import email as email_validator, domain as domain_validator

from apiserver.apierrors import errors
from .base import *


def validate_lucene_query(value):
    if value == "":
        return
    try:
        parser.parse(value)
    except ParseError as e:
        raise errors.bad_request.InvalidLuceneSyntax(
            error=str(e), query=shorten(value, 50, placeholder="...")
        )


class LuceneQueryField(fields.StringField):
    def validate(self, value):
        super(LuceneQueryField, self).validate(value)
        if value is None:
            return
        validate_lucene_query(value)


class EmailField(fields.StringField):
    def validate(self, value):
        super().validate(value)
        if value is None:
            return
        if email_validator(value) is not True:
            raise errors.bad_request.InvalidEmailAddress()


class DomainField(fields.StringField):
    def validate(self, value):
        super().validate(value)
        if value is None:
            return
        if domain_validator(value) is not True:
            raise errors.bad_request.InvalidDomainName()
