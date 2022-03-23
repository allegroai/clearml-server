from collections import OrderedDict
from operator import attrgetter
from threading import Lock
from typing import Sequence

import six
from mongoengine import (
    EmbeddedDocumentField,
    EmbeddedDocumentListField,
    EmbeddedDocument,
    Document,
)
from mongoengine.base import get_document

from apiserver.database.fields import (
    LengthRangeEmbeddedDocumentListField,
    UniqueEmbeddedDocumentListField,
    EmbeddedDocumentSortedListField,
)
from apiserver.database.utils import get_fields, get_fields_attr


class PropsMixin(object):
    __cached_fields = None
    __cached_reference_fields = None
    __cached_exclude_fields = None
    __cached_fields_with_instance = None
    __cached_all_fields_with_instance = None

    __cached_dpath_computed_fields_lock = Lock()
    __cached_dpath_computed_fields = None

    _document_classes = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if issubclass(cls, (Document, EmbeddedDocument)):
            PropsMixin._document_classes[cls._class_name] = cls

    @classmethod
    def get_fields(cls):
        if cls.__cached_fields is None:
            cls.__cached_fields = get_fields(cls)
        return cls.__cached_fields

    @classmethod
    def get_all_fields_with_instance(cls):
        if cls.__cached_all_fields_with_instance is None:
            cls.__cached_all_fields_with_instance = get_fields(
                cls, return_instance=True, subfields=True
            )
        return cls.__cached_all_fields_with_instance

    @classmethod
    def get_fields_with_instance(cls, doc_cls):
        if cls.__cached_fields_with_instance is None:
            cls.__cached_fields_with_instance = {}
        if doc_cls not in cls.__cached_fields_with_instance:
            cls.__cached_fields_with_instance[doc_cls] = get_fields(
                doc_cls, return_instance=True
            )
        return cls.__cached_fields_with_instance[doc_cls]

    @staticmethod
    def _get_fields_with_attr(cls_, attr):
        """ Get all fields with the specified attribute (supports nested fields) """
        res = get_fields_attr(cls_, attr=attr)

        def resolve_doc(v):
            if not isinstance(v, six.string_types):
                return v

            if v == "self":
                return cls_.owner_document

            doc_cls = PropsMixin._document_classes.get(v)
            if doc_cls:
                return doc_cls

            return get_document(v)

        fields = {k: resolve_doc(v) for k, v in res.items()}

        def collect_embedded_docs(doc_cls, embedded_doc_field_getter):
            for field, embedded_doc_field in get_fields(
                cls_, of_type=doc_cls, return_instance=True
            ):
                embedded_doc_cls = embedded_doc_field_getter(
                    embedded_doc_field
                ).document_type
                fields.update(
                    {
                        ".".join((field, subfield)): doc
                        for subfield, doc in PropsMixin._get_fields_with_attr(
                            embedded_doc_cls, attr
                        ).items()
                    }
                )

        collect_embedded_docs(EmbeddedDocumentField, lambda x: x)
        collect_embedded_docs(EmbeddedDocumentListField, attrgetter("field"))
        collect_embedded_docs(LengthRangeEmbeddedDocumentListField, attrgetter("field"))
        collect_embedded_docs(UniqueEmbeddedDocumentListField, attrgetter("field"))
        collect_embedded_docs(EmbeddedDocumentSortedListField, attrgetter("field"))

        return fields

    @classmethod
    def _translate_fields_path(cls, parts):
        current_cls = cls
        translated_parts = []
        for depth, part in enumerate(parts):
            if current_cls is None:
                raise ValueError(
                    "Invalid path (non-document encountered at %s)" % parts[: depth - 1]
                )
            try:
                field_name, field = next(
                    (k, v)
                    for k, v in cls.get_fields_with_instance(current_cls)
                    if k == part
                )
            except StopIteration:
                raise ValueError("Invalid field path %s" % parts[:depth])

            translated_parts.append(part)

            if isinstance(field, EmbeddedDocumentField):
                current_cls = field.document_type
            elif isinstance(
                field,
                (
                    EmbeddedDocumentListField,
                    LengthRangeEmbeddedDocumentListField,
                    UniqueEmbeddedDocumentListField,
                    EmbeddedDocumentSortedListField,
                ),
            ):
                current_cls = field.field.document_type
                translated_parts.append("*")
            else:
                current_cls = None

        return translated_parts

    @classmethod
    def get_reference_fields(cls):
        if cls.__cached_reference_fields is None:
            fields = cls._get_fields_with_attr(cls, "reference_field")
            cls.__cached_reference_fields = OrderedDict(sorted(fields.items()))
        return cls.__cached_reference_fields

    @classmethod
    def get_extra_projection(cls, fields: Sequence) -> tuple:
        if isinstance(fields, str):
            fields = [fields]
        return tuple(
            set(fields).union(cls.get_fields()).difference(cls.get_exclude_fields())
        )

    @classmethod
    def get_exclude_fields(cls):
        if cls.__cached_exclude_fields is None:
            fields = cls._get_fields_with_attr(cls, "exclude_by_default")
            cls.__cached_exclude_fields = OrderedDict(sorted(fields.items()))
        return cls.__cached_exclude_fields

    @classmethod
    def get_dpath_translated_path(cls, path, separator="."):
        if cls.__cached_dpath_computed_fields is None:
            cls.__cached_dpath_computed_fields = {}
        if path not in cls.__cached_dpath_computed_fields:
            with cls.__cached_dpath_computed_fields_lock:
                parts = path.split(separator)
                translated = cls._translate_fields_path(parts)
                result = separator.join(translated)
                cls.__cached_dpath_computed_fields[path] = result
        return cls.__cached_dpath_computed_fields[path]

    def get_field_value(self, field_path: str, default=None):
        """
        Return the document field_path value by the field_path name.
        The path may contain '.'. If on any level the path is
        not found then the default value is returned
        """
        path_elements = field_path.split(".")
        current = self
        for name in path_elements:
            current = getattr(current, name, default)
            if current == default:
                break

        return current
