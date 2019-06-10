from __future__ import unicode_literals

from collections import OrderedDict

import attr
import related
import six
from related import StringField, FloatField, IntegerField, BooleanField


@attr.s
class AtomicType(object):
    python = attr.ib()
    related = attr.ib()


ATOMIC_TYPES = {
    'integer': AtomicType(int, IntegerField),
    'number': AtomicType(float, FloatField),
    'string': AtomicType(str, StringField),
    'boolean': AtomicType(bool, BooleanField),
}


def resolve_ref(definitions, value):
    ref = value.get('$ref')
    if ref:
        name = ref.split('/')[-1]
        return definitions[name]
    one_of = value.get('oneOf')
    if one_of:
        one_of = list(one_of)
        one_of.remove(dict(type='null'))
        assert len(one_of) == 1
        return dict(value, **resolve_ref(definitions, one_of.pop()))
    return value


def normalize_type(typ):
    if isinstance(typ, six.string_types):
        return typ
    return (set(typ) - {'null'}).pop()


@attr.s
class RelatedBuilder(object):
    """
    Converts jsonschema to related class or field.
    :param name: Object name. Will be used as the name of the class and to detect recursive objects,
                    which are not supported.
    :param schema: jsonschema which is the base of the object
    :param required: In case of child fields, whether the field is required. Only used in ``to_field``.
    :param definitions: Dictionary to resolve definitions. Defaults to schema['definitions'].
    :param default: Default value of field. Only used in ``to_field``.
    """

    name = attr.ib(type=six.text_type)
    schema = attr.ib(type=dict, repr=False)
    required = attr.ib(type=bool, default=False)
    definitions = attr.ib(type=dict, default=None, repr=False)
    default = attr.ib(default=attr.Factory(lambda: attr.NOTHING))

    def __attrs_post_init__(self):
        self.schema = resolve_ref(self.definitions, self.schema)
        self.type = normalize_type(self.schema['type'])
        self.definitions = self.definitions or self.schema.get('definitions')

    def to_field(self):
        """
        Creates the appropriate ``related`` field from instance.
        NOTE: Items and nesting level of nested arrays will not be checked.
        """
        if self.type in ATOMIC_TYPES:
            field = ATOMIC_TYPES[self.type].related
            return field(default=self.default, required=self.required)
        if self.type == 'array':
            sub_schema = self.schema['items']
            builder = RelatedBuilder(
                '{}_items'.format(self.name), sub_schema, definitions=self.definitions
            )
            return related.SequenceField(
                list if builder.type == 'array' else builder.to_class(),
                default=attr.Factory(list),
            )
        if self.schema.get('additionalProperties') is True:
            return attr.ib(type=dict, default=None)

        return related.ChildField(
            self.to_class(), default=self.default, required=self.required
        )

    def to_class(self):
        """
        Creates a related class.
        """
        required = self.schema.get('required', [])

        if self.type in ATOMIC_TYPES:
            return ATOMIC_TYPES[self.type].python

        if self.type == 'array':
            raise RuntimeError(self, 'Cannot convert array to related class')

        assert self.type and normalize_type(self.type) == 'object', (
            self.type,
            list(self.schema),
        )
        properties = sorted(
            tuple(
                (
                    inner_name,
                    RelatedBuilder(
                        name=inner_name,
                        schema=value,
                        required=inner_name in required,
                        definitions=definitions,
                    ),
                )
                for inner_name, value in self.schema['properties'].items()
                if inner_name != self.name
            ),
            key=lambda pair: pair[1].required,
            reverse=True,
        )
        return related.mutable(
            type(
                self.name,
                (object,),
                OrderedDict([(key, builder.to_field()) for key, builder in properties]),
            )
        )


class Visitor(object):
    """Base class for visitors."""

    def visit(self, node, *args, **kwargs):
        """Visit a node.

        Calls ``visit_CLASSNAME`` on itself passing ``node``, where
        ``CLASSNAME`` is the node's class. If the visitor does not implement an
        appropriate visitation method, will go up the
        `MRO <https://www.python.org/download/releases/2.3/mro/>`_ until a
        match is found.

        If the search exhausts all classes of node, raises a
        :class:`~exceptions.NotImplementedError`.

        :param node: The node to visit.
        :return: The return value of the called visitation function.
        """
        if isinstance(node, type):
            mro = node.mro()
        else:
            mro = type(node).mro()
        for cls in mro:
            meth = getattr(self, 'visit_' + cls.__name__, None)
            if meth is None:
                continue
            return meth(node, *args, **kwargs)

        raise NotImplementedError(
            'No visitation method visit_{}'.format(node.__class__.__name__)
        )


class SchemaCleaner(Visitor):
    def __init__(self, definitions):
        self.definitions = definitions

    def visit_dict(self, obj, schema):
        schema = resolve_ref(self.definitions, schema)
        if schema.get('additionalProperties') is True:
            return
        props = schema['properties']
        for key, value in list(obj.items()):
            if key in props:
                self.visit(value, props[key])
            else:
                del obj[key]

    def visit_list(self, obj, schema):
        for value in obj:
            self.visit(value, schema['items'])

    @staticmethod
    def visit_object(obj, schema):
        pass
