import re
from sys import maxsize

import six
from mongoengine import (
    EmbeddedDocumentListField,
    ListField,
    FloatField,
    StringField,
    EmbeddedDocumentField,
    SortedListField,
    MapField,
    DictField,
)


class LengthRangeListField(ListField):
    def __init__(self, field=None, max_length=maxsize, min_length=0, **kwargs):
        self.__min_length = min_length
        self.__max_length = max_length
        super(LengthRangeListField, self).__init__(field, **kwargs)

    def validate(self, value):
        min, val, max = self.__min_length, len(value), self.__max_length
        if not min <= val <= max:
            self.error("Item count %d exceeds range [%d, %d]" % (val, min, max))
        super(LengthRangeListField, self).validate(value)


class LengthRangeEmbeddedDocumentListField(LengthRangeListField):
    def __init__(self, field=None, *args, **kwargs):
        super(LengthRangeEmbeddedDocumentListField, self).__init__(
            EmbeddedDocumentField(field), *args, **kwargs
        )


class UniqueEmbeddedDocumentListField(EmbeddedDocumentListField):
    def __init__(self, document_type, key, **kwargs):
        """
        Create a unique embedded document list field for a document type with a unique comparison key func/property
        :param document_type: The type of :class:`~mongoengine.EmbeddedDocument` the list will hold.
        :param key: A callable to extract a key from each item
        """
        if not callable(key):
            raise KeyError("key must be callable")
        self.__key = key
        super(UniqueEmbeddedDocumentListField, self).__init__(document_type)

    def validate(self, value):
        if len({self.__key(i) for i in value}) != len(value):
            self.error("Items with duplicate key exist in the list")
        super(UniqueEmbeddedDocumentListField, self).validate(value)


def object_to_key_value_pairs(obj):
    if isinstance(obj, dict):
        return [(key, object_to_key_value_pairs(value)) for key, value in obj.items()]
    if isinstance(obj, list):
        return list(map(object_to_key_value_pairs, obj))
    return obj


class EmbeddedDocumentSortedListField(EmbeddedDocumentListField):
    """
    A sorted list of embedded documents
    """

    def to_mongo(self, value, use_db_field=True, fields=None):
        value = super(EmbeddedDocumentSortedListField, self).to_mongo(
            value, use_db_field, fields
        )
        return sorted(value, key=object_to_key_value_pairs)


class LengthRangeSortedListField(LengthRangeListField, SortedListField):
    pass


class CustomFloatField(FloatField):
    def __init__(self, greater_than=None, **kwargs):
        self.greater_than = greater_than
        super(CustomFloatField, self).__init__(**kwargs)

    def validate(self, value):
        super(CustomFloatField, self).validate(value)

        if self.greater_than is not None and value <= self.greater_than:
            self.error("Float value must be greater than %s" % str(self.greater_than))


# TODO: bucket name should be at most 63 characters....
aws_s3_bucket_only_regex = (
    r"^s3://"
    r"(?:(?:\w[A-Z0-9\-]+\w)\.)*(?:\w[A-Z0-9\-]+\w)"  # bucket name
)

aws_s3_url_with_bucket_regex = (
    r"^s3://"
    r"(?:(?:\w[A-Z0-9\-]+\w)\.)*(?:\w[A-Z0-9\-]+\w)"  # bucket name
    r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}(?<!-)\.?))"  # domain...
)

non_aws_s3_regex = (
    r"^s3://"
    r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}(?<!-)\.?)|"  # domain...
    r"localhost|"  # localhost...
    r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|"  # ...or ipv4
    r"\[?[A-F0-9]*:[A-F0-9:]+\]?)"  # ...or ipv6
    r"(?::\d+)?"  # optional port
    r"(?:/(?:(?:\w[A-Z0-9\-]+\w)\.)*(?:\w[A-Z0-9\-]+\w))"  # bucket name
)

google_gs_bucket_only_regex = (
    r"^gs://"
    r"(?:(?:\w[A-Z0-9\-_]+\w)\.)*(?:\w[A-Z0-9\-_]+\w)"  # bucket name
)

file_regex = r"^file://"

generic_url_regex = (
    r"^%s://"  # scheme placeholder
    r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}(?<!-)\.?)|"  # domain...
    r"localhost|"  # localhost...
    r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|"  # ...or ipv4
    r"\[?[A-F0-9]*:[A-F0-9:]+\]?)"  # ...or ipv6
    r"(?::\d+)?"  # optional port
)

path_suffix = r"(?:/?|[/?]\S+)$"
file_path_suffix = r"(?:/\S*[^/]+)$"


class _RegexURLField(StringField):
    _regex = []

    def __init__(self, regex, **kwargs):
        super(_RegexURLField, self).__init__(**kwargs)
        regex = regex if isinstance(regex, (tuple, list)) else [regex]
        self._regex = [
            re.compile(e, re.IGNORECASE) if isinstance(e, six.string_types) else e
            for e in regex
        ]

    def validate(self, value):
        # Check first if the scheme is valid
        if not any(regex for regex in self._regex if regex.match(value)):
            self.error("Invalid URL: {}".format(value))
            return


class OutputDestinationField(_RegexURLField):
    """ A field representing task output URL """

    schemes = ["s3", "gs", "file"]
    _expressions = (
        aws_s3_bucket_only_regex + path_suffix,
        aws_s3_url_with_bucket_regex + path_suffix,
        non_aws_s3_regex + path_suffix,
        google_gs_bucket_only_regex + path_suffix,
        file_regex + path_suffix,
    )

    def __init__(self, **kwargs):
        super(OutputDestinationField, self).__init__(self._expressions, **kwargs)


class SupportedURLField(_RegexURLField):
    """ A field representing a model URL """

    schemes = ["s3", "gs", "file", "http", "https"]

    _expressions = tuple(
        pattern + file_path_suffix
        for pattern in (
            aws_s3_bucket_only_regex,
            aws_s3_url_with_bucket_regex,
            non_aws_s3_regex,
            google_gs_bucket_only_regex,
            file_regex,
            (generic_url_regex % "http"),
            (generic_url_regex % "https"),
        )
    )

    def __init__(self, **kwargs):
        super(SupportedURLField, self).__init__(self._expressions, **kwargs)


class StrippedStringField(StringField):
    def __init__(
        self, regex=None, max_length=None, min_length=None, strip_chars=None, **kwargs
    ):
        super(StrippedStringField, self).__init__(
            regex, max_length, min_length, **kwargs
        )
        self._strip_chars = strip_chars

    def __set__(self, instance, value):
        if value is not None:
            try:
                value = value.strip(self._strip_chars)
            except AttributeError:
                pass
        super(StrippedStringField, self).__set__(instance, value)

    def prepare_query_value(self, op, value):
        if not isinstance(op, six.string_types):
            return value
        if value is not None:
            value = value.strip(self._strip_chars)
        return super(StrippedStringField, self).prepare_query_value(op, value)


def contains_empty_key(d):
    """
    Helper function to recursively determine if any key in a
    dictionary is empty (based on mongoengine.fields.key_not_string)
    """
    for k, v in list(d.items()):
        if not k or (isinstance(v, dict) and contains_empty_key(v)):
            return True


class SafeMapField(MapField):
    def validate(self, value):
        super(SafeMapField, self).validate(value)

        if contains_empty_key(value):
            self.error("Empty keys are not allowed in a MapField")


class SafeDictField(DictField):
    def validate(self, value):
        super(SafeDictField, self).validate(value)

        if contains_empty_key(value):
            self.error("Empty keys are not allowed in a DictField")
