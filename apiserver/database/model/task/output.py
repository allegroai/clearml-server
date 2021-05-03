from mongoengine import EmbeddedDocument, StringField

from apiserver.database.fields import StrippedStringField
from apiserver.database.utils import get_options


class Result(object):
    success = 'success'
    failure = 'failure'


class Output(EmbeddedDocument):
    destination = StrippedStringField()
    error = StringField(user_set_allowed=True)
    result = StringField(choices=get_options(Result))
