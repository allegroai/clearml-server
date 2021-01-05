import copy
import re
from typing import Union

from mongoengine import Q
from mongoengine.queryset.visitor import (
    QueryCompilerVisitor,
    SimplificationVisitor,
    QCombination,
    QNode,
)


class RegexWrapper(object):
    def __init__(self, pattern, flags=None):
        super(RegexWrapper, self).__init__()
        self.pattern = pattern
        self.flags = flags

    @property
    def regex(self):
        return re.compile(self.pattern, self.flags if self.flags is not None else 0)


class RegexMixin(object):
    def to_query(self: Union["RegexMixin", QNode], document):
        query = self.accept(SimplificationVisitor())
        query = query.accept(RegexQueryCompilerVisitor(document))
        return query

    def _combine(self: Union["RegexMixin", QNode], other, operation):
        """Combine this node with another node into a QCombination
        object.
        """
        if getattr(other, "empty", True):
            return self

        if self.empty:
            return other

        return RegexQCombination(operation, [self, other])


class RegexQCombination(RegexMixin, QCombination):
    pass


class RegexQ(RegexMixin, Q):
    pass


class RegexQueryCompilerVisitor(QueryCompilerVisitor):
    """
    Improved mongoengine complied queries visitor class that supports compiled regex expressions as part of the query.

    We need this class since mongoengine's Q (QNode) class uses copy.deepcopy() as part of the tree simplification
     stage, which does not support re.compiled objects (since Python 2.5).
    This class allows users to provide regex strings wrapped in QueryRegex instances, which are lazily evaluated to
     to re.compile instances just before being visited for compilation (this is done after the simplification stage)
    """

    def visit_query(self, query):
        query = copy.deepcopy(query)
        query.query = self._transform_query(query.query)
        return super(RegexQueryCompilerVisitor, self).visit_query(query)

    def _transform_query(self, query):
        return {k: v.regex if isinstance(v, RegexWrapper) else v for k, v in query.items()}
