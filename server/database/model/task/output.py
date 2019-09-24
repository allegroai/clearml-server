from mongoengine import EmbeddedDocument, StringField

from database.fields import StrippedStringField
from database.utils import get_options


class Result(object):
    success = 'success'
    failure = 'failure'


class Output(EmbeddedDocument):
    destination = StrippedStringField()
    model = StringField(reference_field='Model')
    error = StringField(user_set_allowed=True)
    result = StringField(choices=get_options(Result))
