import validators
from jsonmodels.errors import ValidationError


class ForEach(object):
    def __init__(self, validator):
        self.validator = validator

    def validate(self, values):
        for value in values:
            self.validator.validate(value)

    def modify_schema(self, field_schema):
        return self.validator.modify_schema(field_schema)


class Hostname(object):

    def validate(self, value):
        if validators.domain(value) is not True:
            raise ValidationError(f"Value '{value}' is not a valid hostname")

    def modify_schema(self, field_schema):
        field_schema["format"] = "hostname"


class Email(object):

    def validate(self, value):
        if validators.email(value) is not True:
            raise ValidationError(f"Value '{value}' is not a valid email address")

    def modify_schema(self, field_schema):
        field_schema["format"] = "email"
