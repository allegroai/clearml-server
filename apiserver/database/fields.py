from operator import itemgetter
from sys import maxsize
from typing import Type, Tuple

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
    DynamicField,
)
from mongoengine.fields import key_not_string, key_starts_with_dollar, EmailField

NoneType = type(None)


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


class CanonicEmailField(EmailField):
    """email field that is always lower cased"""
    def __set__(self, instance, value: str):
        if value is not None:
            try:
                value = value.lower()
            except AttributeError:
                pass
        super().__set__(instance, value)

    def prepare_query_value(self, op, value):
        if not isinstance(op, six.string_types):
            return value
        if value is not None:
            value = value.lower()
        return super().prepare_query_value(op, value)


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


class DictValidationMixin:
    """
    DictField validation in MongoEngine requires default alias and permissions to access DB version:
    https://github.com/MongoEngine/mongoengine/issues/2239
    This is a stripped down implementation that does not require any of the above and implies Mongo ver 3.6+
    """

    def _safe_validate(self: DictField, value):
        if not isinstance(value, dict):
            self.error("Only dictionaries may be used in a DictField")

        if key_not_string(value):
            msg = "Invalid dictionary key - documents must have only string keys"
            self.error(msg)

        if key_starts_with_dollar(value):
            self.error(
                'Invalid dictionary key name - keys may not startswith "$" characters'
            )
        super(DictField, self).validate(value)


class SafeMapField(MapField, DictValidationMixin):
    def validate(self, value):
        self._safe_validate(value)

        if contains_empty_key(value):
            self.error("Empty keys are not allowed in a MapField")


class NullableStringField(StringField):
    def validate(self, value):
        if value is None:
            return
        super(NullableStringField, self).validate(value)


class SafeDictField(DictField, DictValidationMixin):
    def validate(self, value):
        self._safe_validate(value)

        if contains_empty_key(value):
            self.error("Empty keys are not allowed in a DictField")


class SafeSortedListField(SortedListField):
    """
    SortedListField that does not raise an error in case items are not comparable
    (in which case they will be sorted by their string representation)
    """

    def to_mongo(self, *args, **kwargs):
        try:
            return super(SafeSortedListField, self).to_mongo(*args, **kwargs)
        except TypeError:
            return self._safe_to_mongo(*args, **kwargs)

    def _safe_to_mongo(self, value, use_db_field=True, fields=None):
        value = super(SortedListField, self).to_mongo(value, use_db_field, fields)
        if self._ordering is not None:

            def key(v):
                return str(itemgetter(self._ordering)(v))

        else:
            key = str
        return sorted(value, key=key, reverse=self._order_reverse)


class UnionField(DynamicField):
    def __init__(self, types, *args, **kwargs):
        super(UnionField, self).__init__(*args, **kwargs)
        self.types: Tuple[Type] = tuple(types)

    def validate(self, value, clean=True):
        if not isinstance(value, self.types):
            type_names = [t.__name__ for t in self.types]
            expected = " or ".join(
                filter(
                    None,
                    (", ".join(type_names[:-1]), type_names[-1]))
            )
            self.error(
                f"Expected {expected}, got {type(value).__name__}: {value}"
            )
        super(UnionField, self).validate(value, clean)
