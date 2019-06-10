from mongoengine import EmbeddedDocument, StringField
from database.utils import get_options

from database.fields import OutputDestinationField


class Result(object):
    success = 'success'
    failure = 'failure'


class Output(EmbeddedDocument):
    destination = OutputDestinationField()
    model = StringField(reference_field='Model')
    error = StringField(user_set_allowed=True)
    result = StringField(choices=get_options(Result))
