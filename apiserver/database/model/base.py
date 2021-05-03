import re
from collections import namedtuple
from functools import reduce
from typing import Collection, Sequence, Union, Optional, Type, Tuple, Mapping, Any

from boltons.iterutils import first, bucketize, partition
from dateutil.parser import parse as parse_datetime
from mongoengine import Q, Document, ListField, StringField
from pymongo.command_cursor import CommandCursor

from apiserver.apierrors import errors
from apiserver.apierrors.base import BaseError
from apiserver.config_repo import config
from apiserver.database.errors import MakeGetAllQueryError
from apiserver.database.projection import project_dict, ProjectionHelper
from apiserver.database.props import PropsMixin
from apiserver.database.query import RegexQ, RegexWrapper
from apiserver.database.utils import (
    get_company_or_none_constraint,
    get_fields_choices,
    field_does_not_exist,
    field_exists,
)

log = config.logger("dbmodel")

ACCESS_REGEX = re.compile(r"^(?P<prefix>>=|>|<=|<)?(?P<value>.*)$")
ACCESS_MODIFIER = {">=": "gte", ">": "gt", "<=": "lte", "<": "lt"}

ABSTRACT_FLAG = {"abstract": True}


class AuthDocument(Document):
    meta = ABSTRACT_FLAG


class ProperDictMixin(object):
    def to_proper_dict(
        self: Union["ProperDictMixin", Document],
        strip_private=True,
        only=None,
        extra_dict=None,
    ) -> dict:
        return self.properize_dict(
            self.to_mongo(use_db_field=False).to_dict(),
            strip_private=strip_private,
            only=only,
            extra_dict=extra_dict,
        )

    @classmethod
    def properize_dict(
        cls, d, strip_private=True, only=None, extra_dict=None, normalize_id=True
    ):
        res = d
        if normalize_id and "_id" in res:
            res["id"] = res.pop("_id")
        if strip_private:
            res = {k: v for k, v in res.items() if k[0] != "_"}
        if only:
            res = project_dict(res, only)
        if extra_dict:
            res.update(extra_dict)
        return res


class GetMixin(PropsMixin):
    _text_score = "$text_score"
    _projection_key = "projection"
    _ordering_key = "order_by"
    _search_text_key = "search_text"

    _multi_field_param_sep = "__"
    _multi_field_param_prefix = {
        ("_any_", "_or_"): lambda a, b: a | b,
        ("_all_", "_and_"): lambda a, b: a & b,
    }
    MultiFieldParameters = namedtuple("MultiFieldParameters", "pattern fields")

    _field_collation_overrides = {}

    class QueryParameterOptions(object):
        def __init__(
            self,
            pattern_fields=("name",),
            list_fields=("tags", "system_tags", "id"),
            datetime_fields=None,
            fields=None,
            range_fields=None,
        ):
            """
            :param pattern_fields: Fields for which a "string contains" condition should be generated
            :param list_fields: Fields for which a "list contains" condition should be generated
            :param datetime_fields: Fields for which datetime condition should be generated (see ACCESS_MODIFIER)
            :param fields: Fields which which a simple equality condition should be generated (basically filters out all
                other unsupported query fields)
            """
            self.fields = fields
            self.datetime_fields = datetime_fields
            self.list_fields = list_fields
            self.range_fields = range_fields
            self.pattern_fields = pattern_fields

    class ListFieldBucketHelper:
        op_prefix = "__$"
        legacy_exclude_prefix = "-"

        _default = "in"
        _ops = {
            "not": ("nin", False),
            "all": ("all", True),
            "and": ("all", True),
        }
        _next = _default
        _sticky = False

        def __init__(self, legacy=False):
            self._legacy = legacy

        def key(self, v):
            if v is None:
                self._next = self._default
                return self._default
            elif self._legacy and v.startswith(self.legacy_exclude_prefix):
                self._next = self._default
                return self._ops["not"][0]
            elif v.startswith(self.op_prefix):
                self._next, self._sticky = self._ops.get(
                    v[len(self.op_prefix) :], (self._default, self._sticky)
                )
                return None

            next_ = self._next
            if not self._sticky:
                self._next = self._default
            return next_

        def value_transform(self, v):
            if self._legacy and v and v.startswith(self.legacy_exclude_prefix):
                return v[len(self.legacy_exclude_prefix) :]
            return v

    get_all_query_options = QueryParameterOptions()

    @classmethod
    def get(
        cls: Union["GetMixin", Document],
        company,
        id,
        *,
        _only=None,
        include_public=False,
        **kwargs,
    ) -> "GetMixin":
        q = cls.objects(
            cls._prepare_perm_query(company, allow_public=include_public)
            & Q(id=id, **kwargs)
        )
        if _only:
            q = q.only(*_only)
        return q.first()

    @classmethod
    def prepare_query(
        cls,
        company: str,
        parameters: dict = None,
        parameters_options: QueryParameterOptions = None,
        allow_public=False,
    ):
        """
        Prepare a query object based on the provided query dictionary and various fields.
        :param parameters_options: Specifies options for parsing the parameters (see ParametersOptions)
        :param company: Company ID (required)
        :param allow_public: Allow results from public objects
        :param parameters: Query dictionary (relevant keys are these specified by the various field names parameters).
            Supported parameters:
            - <field_name>: <value> Will query for items with this value in the field (see QueryParameterOptions for
                specific rules on handling values). Only items matching ALL of these conditions will be retrieved.
            - <any|all>: {fields: [<field1>, <field2>, ...], pattern: <pattern>} Will query for items where any or all
                provided fields match the provided pattern.
        :return: mongoengine.Q query object
        """
        return cls._prepare_query_no_company(
            parameters, parameters_options
        ) & cls._prepare_perm_query(company, allow_public=allow_public)

    @staticmethod
    def _pop_matching_params(
        patterns: Sequence[str], parameters: dict
    ) -> Mapping[str, Any]:
        """
        Pop the parameters that match the specified patterns and return
        the dictionary of matching parameters
        Pop None parameters since they are not the real queries
        """
        if not patterns:
            return {}

        fields = set()
        for pattern in patterns:
            if pattern.endswith("*"):
                prefix = pattern[:-1]
                fields.update(
                    {field for field in parameters if field.startswith(prefix)}
                )
            elif pattern in parameters:
                fields.add(pattern)

        pairs = ((field, parameters.pop(field, None)) for field in fields)
        return {k: v for k, v in pairs if v is not None}

    @classmethod
    def _try_convert_to_numeric(cls, value: Union[str, Sequence[str]]):
        def convert_str(val: str) -> Union[float, str]:
            try:
                return float(val)
            except ValueError:
                return val

        if isinstance(value, str):
            return convert_str(value)

        if isinstance(value, (list, tuple)):
            return [convert_str(v) if isinstance(v, str) else v for v in value]

        return value

    @classmethod
    def _get_fixed_field_value(cls, field: str, value):
        if field.startswith("last_metrics."):
            return cls._try_convert_to_numeric(value)
        return value

    @classmethod
    def _prepare_query_no_company(
        cls, parameters=None, parameters_options=QueryParameterOptions()
    ):
        """
        Prepare a query object based on the provided query dictionary and various fields.

        NOTE: BE VERY CAREFUL WITH THIS CALL, as it allows creating queries that span across companies.

        :param parameters_options: Specifies options for parsing the parameters (see ParametersOptions)
        :param parameters: Query dictionary (relevant keys are these specified by the various field names parameters).
            Supported parameters:
            - <field_name>: <value> Will query for items with this value in the field (see QueryParameterOptions for
                specific rules on handling values). Only items matching ALL of these conditions will be retrieved.
            - <any|all>: {fields: [<field1>, <field2>, ...], pattern: <pattern>} Will query for items where any or all
                provided fields match the provided pattern.
        :return: mongoengine.Q query object
        """
        parameters_options = parameters_options or cls.get_all_query_options
        dict_query = {}
        query = RegexQ()
        if parameters:
            parameters = {
                k: cls._get_fixed_field_value(k, v) for k, v in parameters.items()
            }
            opts = parameters_options
            for field in opts.pattern_fields:
                pattern = parameters.pop(field, None)
                if pattern:
                    dict_query[field] = RegexWrapper(pattern)

            for field, data in cls._pop_matching_params(
                patterns=opts.list_fields, parameters=parameters
            ).items():
                query &= cls.get_list_field_query(field, data)

            for field, data in cls._pop_matching_params(
                patterns=opts.range_fields, parameters=parameters
            ).items():
                query &= cls.get_range_field_query(field, data)

            for field in opts.fields or []:
                data = parameters.pop(field, None)
                if data is not None:
                    dict_query[field] = data

            for field in opts.datetime_fields or []:
                data = parameters.pop(field, None)
                if data is not None:
                    if not isinstance(data, list):
                        data = [data]
                    for d in data:  # type: str
                        m = ACCESS_REGEX.match(d)
                        if not m:
                            continue
                        try:
                            value = parse_datetime(m.group("value"))
                            prefix = m.group("prefix")
                            modifier = ACCESS_MODIFIER.get(prefix)
                            f = field if not modifier else "__".join((field, modifier))
                            dict_query[f] = value
                        except (ValueError, OverflowError):
                            pass

            for field, value in parameters.items():
                for keys, func in cls._multi_field_param_prefix.items():
                    if field not in keys:
                        continue
                    try:
                        data = cls.MultiFieldParameters(**value)
                    except Exception:
                        raise MakeGetAllQueryError("incorrect field format", field)
                    if not data.fields:
                        break
                    if any("._" in f for f in data.fields):
                        q = reduce(
                            lambda a, x: func(a, Q(__raw__={x: {"$regex": data.pattern, "$options": "i"}})),
                            data.fields,
                            Q()
                        )
                    else:
                        regex = RegexWrapper(data.pattern, flags=re.IGNORECASE)
                        sep_fields = [f.replace(".", "__") for f in data.fields]
                        q = reduce(
                            lambda a, x: func(a, RegexQ(**{x: regex})), sep_fields, RegexQ()
                        )
                    query = query & q

        return query & RegexQ(**dict_query)

    @classmethod
    def get_range_field_query(cls, field: str, data: Sequence[Optional[str]]) -> Q:
        """
        Return a range query for the provided field. The data should contain min and max values
        Both intervals are included. For open range queries either min or max can be None
        In case the min value is None the records with missing or None value from db are included
        """
        if not isinstance(data, (list, tuple)) or len(data) != 2:
            raise errors.bad_request.ValidationError(
                f"Min and max values should be specified for range field {field}"
            )

        min_val, max_val = data
        if min_val is None and max_val is None:
            raise errors.bad_request.ValidationError(
                f"At least one of min or max values should be provided for field {field}"
            )

        mongoengine_field = field.replace(".", "__")
        query = {}
        if min_val is not None:
            query[f"{mongoengine_field}__gte"] = min_val
        if max_val is not None:
            query[f"{mongoengine_field}__lte"] = max_val

        q = Q(**query)
        if min_val is None:
            q |= Q(**{mongoengine_field: None})

        return q

    @classmethod
    def get_list_field_query(cls, field: str, data: Sequence[Optional[str]]) -> Q:
        """
        Get a proper mongoengine Q object that represents an "or" query for the provided values
        with respect to the given list field, with support for "none of empty" in case a None value
        is included.

        - Exclusion can be specified by a leading "-" for each value (API versions <2.8)
            or by a preceding "__$not" value (operator)
        - AND can be achieved using a preceding "__$all" or "__$and" value (operator)
        """
        if not isinstance(data, (list, tuple)):
            data = [data]
            # raise MakeGetAllQueryError("expected list", field)

        # TODO: backwards compatibility only for older API versions
        helper = cls.ListFieldBucketHelper(legacy=True)
        actions = bucketize(
            data, key=helper.key, value_transform=helper.value_transform
        )

        allow_empty = None in actions.get("in", {})
        mongoengine_field = field.replace(".", "__")

        q = RegexQ()
        for action in filter(None, actions):
            q &= RegexQ(
                **{f"{mongoengine_field}__{action}": list(set(actions[action]))}
            )

        if not allow_empty:
            return q

        return (
            q
            | Q(**{f"{mongoengine_field}__exists": False})
            | Q(**{mongoengine_field: []})
        )

    @classmethod
    def _prepare_perm_query(cls, company, allow_public=False):
        if allow_public:
            return get_company_or_none_constraint(company)
        return Q(company=company)

    @classmethod
    def validate_order_by(cls, parameters, search_text) -> Sequence:
        """
        Validate and extract order_by params as a list
        """
        order_by = parameters.get(cls._ordering_key)
        if not order_by:
            return []

        order_by = order_by if isinstance(order_by, list) else [order_by]
        order_by = [cls._text_score if x == "@text_score" else x for x in order_by]
        if not search_text and cls._text_score in order_by:
            raise errors.bad_request.FieldsValueError(
                "text score cannot be used in order_by when search text is not used"
            )

        return order_by

    @classmethod
    def validate_paging(
        cls, parameters=None, default_page=None, default_page_size=None
    ):
        """ Validate and extract paging info from from the provided dictionary. Supports default values. """
        if parameters is None:
            parameters = {}
        default_page = parameters.get("page", default_page)
        if default_page is None:
            return None, None
        default_page_size = parameters.get("page_size", default_page_size)
        if not default_page_size:
            raise errors.bad_request.MissingRequiredFields(
                "page_size is required when page is requested", field="page_size"
            )
        elif default_page < 0:
            raise errors.bad_request.ValidationError("page must be >=0", field="page")
        elif default_page_size < 1:
            raise errors.bad_request.ValidationError(
                "page_size must be >0", field="page_size"
            )
        return default_page, default_page_size

    @classmethod
    def get_projection(cls, parameters, override_projection=None, **__):
        """ Extract a projection list from the provided dictionary. Supports an override projection. """
        if override_projection is not None:
            return override_projection
        if not parameters:
            return []
        return parameters.get(cls._projection_key) or parameters.get("only_fields", [])

    @classmethod
    def split_projection(
        cls, projection: Sequence[str]
    ) -> Tuple[Collection[str], Collection[str]]:
        """Return include and exclude lists based on passed projection and class definition"""
        if projection:
            include, exclude = partition(
                projection, key=lambda x: x[0] != ProjectionHelper.exclusion_prefix,
            )
        else:
            include, exclude = [], []
        exclude = {x.lstrip(ProjectionHelper.exclusion_prefix) for x in exclude}
        return include, set(cls.get_exclude_fields()).union(exclude).difference(include)

    @classmethod
    def set_projection(cls, parameters: dict, value: Sequence[str]) -> Sequence[str]:
        parameters.pop("only_fields", None)
        parameters[cls._projection_key] = value
        return value

    @classmethod
    def get_ordering(cls, parameters: dict) -> Optional[Sequence[str]]:
        return parameters.get(cls._ordering_key)

    @classmethod
    def set_ordering(cls, parameters: dict, value: Sequence[str]) -> Sequence[str]:
        parameters[cls._ordering_key] = value
        return value

    @classmethod
    def set_default_ordering(cls, parameters: dict, value: Sequence[str]) -> None:
        cls.set_ordering(parameters, cls.get_ordering(parameters) or value)

    @classmethod
    def get_many_with_join(
        cls,
        company,
        query_dict=None,
        query_options=None,
        query=None,
        allow_public=False,
        override_projection=None,
        expand_reference_ids=True,
    ):
        """
        Fetch all documents matching a provided query with support for joining referenced documents according to the
        requested projection. See get_many() for more info.
        :param expand_reference_ids: If True, reference fields that contain just an ID string are expanded into
            a sub-document in the format {_id: <ID>}. Otherwise, field values are left as a string.
        """
        if issubclass(cls, AuthDocument):
            # Refuse projection (join) for auth documents (auth.User etc.) to avoid inadvertently disclosing
            # auth-related secrets and prevent security leaks
            log.error(
                f"Attempted projection of {cls.__name__} auth document (ignored)",
                stack_info=True,
            )
            return []

        override_projection = cls.get_projection(
            parameters=query_dict, override_projection=override_projection
        )

        helper = ProjectionHelper(
            doc_cls=cls,
            projection=override_projection,
            expand_reference_ids=expand_reference_ids,
        )

        # Make the main query
        results = cls.get_many(
            override_projection=helper.doc_projection,
            company=company,
            parameters=query_dict,
            query_dict=query_dict,
            query=query,
            query_options=query_options,
            allow_public=allow_public,
        )

        def projection_func(doc_type, projection, ids):
            return doc_type.get_many_with_join(
                company=company,
                override_projection=projection,
                query=Q(id__in=ids),
                expand_reference_ids=expand_reference_ids,
                allow_public=allow_public,
            )

        return helper.project(results, projection_func)

    @classmethod
    def _get_collation_override(cls, field: str) -> Optional[dict]:
        return first(
            v for k, v in cls._field_collation_overrides.items() if field.startswith(k)
        )

    @classmethod
    def get_many(
        cls,
        company,
        parameters: dict = None,
        query_dict: dict = None,
        query_options: QueryParameterOptions = None,
        query: Q = None,
        allow_public=False,
        override_projection: Collection[str] = None,
        return_dicts=True,
    ):
        """
        Fetch all documents matching a provided query. Supported several built-in options
         (aside from those provided by the parameters):
            - Ordering: using query field `order_by` which can contain a string or a list of strings corresponding to
                    field names. Using field names not defined in the document will cause an error.
            - Paging: using query fields page and page_size. page must be larger than or equal to 0, page_size must be
                    larger than 0 and is required when specifying a page.
            - Text search: using query field `search_text`. If used, text score can be used in the ordering, using the
                    `@text_score` keyword. A text index must be defined on the document type, otherwise an error will
                    be raised.
        :param return_dicts: Return a list of dictionaries. If True, a list of dicts is returned (if projection was
            requested, each contains only the requested projection). If False, a QuerySet object is returned
            (lazy evaluated). If return_dicts is requested then the entities with the None value in order_by field
            are returned last in the ordering.
        :param company: Company ID (required)
        :param parameters: Parameters dict from which paging ordering and searching parameters are extracted.
        :param query_dict: If provided, passed to prepare_query() along with all of the relevant arguments to produce
            a query. The resulting query is AND'ed with the `query` parameter (if provided).
        :param query_options: query parameters options (see ParametersOptions)
        :param query: Optional query object (mongoengine.Q)
        :param override_projection: A list of projection fields overriding any projection specified in the `param_dict`
            argument
        :param allow_public: If True, objects marked as public (no associated company) are also queried.
        :return: A list of objects matching the query.
        """
        override_collation = None
        if query_dict:
            for field in query_dict:
                override_collation = cls._get_collation_override(field)
                if override_collation:
                    break

        if query_dict is not None:
            q = cls.prepare_query(
                parameters=query_dict,
                company=company,
                parameters_options=query_options,
                allow_public=allow_public,
            )
        else:
            q = cls._prepare_perm_query(company, allow_public=allow_public)
        _query = (q & query) if query else q

        if return_dicts:
            return cls._get_many_override_none_ordering(
                query=_query,
                parameters=parameters,
                override_projection=override_projection,
                override_collation=override_collation,
            )

        return cls._get_many_no_company(
            query=_query,
            parameters=parameters,
            override_projection=override_projection,
            override_collation=override_collation,
        )

    @classmethod
    def get_many_public(
        cls, query: Q = None, projection: Collection[str] = None,
    ):
        """
        Fetch all public documents matching a provided query.
        :param query: Optional query object (mongoengine.Q).
        :param projection: A list of projection fields.
        :return: A list of documents matching the query.
        """
        q = get_company_or_none_constraint()
        _query = (q & query) if query else q

        return cls._get_many_no_company(query=_query, override_projection=projection)

    @classmethod
    def _get_many_no_company(
        cls: Union["GetMixin", Document],
        query: Q,
        parameters=None,
        override_projection=None,
        override_collation=None,
    ):
        """
        Fetch all documents matching a provided query.
        This is a company-less version for internal uses. We assume the caller has either added any necessary
        constraints to the query or that no constraints are required.

        NOTE: BE VERY CAREFUL WITH THIS CALL, as it allows returning data across companies.

        :param query: Query object (mongoengine.Q)
        :param parameters: Parameters dict from which paging ordering and searching parameters are extracted.
        :param override_projection: A list of projection fields overriding any projection specified in the `param_dict`
            argument
        """
        if not query:
            raise ValueError("query or call_data must be provided")

        parameters = parameters or {}
        search_text = parameters.get(cls._search_text_key)
        order_by = cls.validate_order_by(parameters=parameters, search_text=search_text)
        if order_by and not override_collation:
            override_collation = cls._get_collation_override(order_by[0])
        page, page_size = cls.validate_paging(parameters=parameters)
        include, exclude = cls.split_projection(
            cls.get_projection(parameters, override_projection)
        )

        qs = cls.objects(query)
        if override_collation:
            qs = qs.collation(collation=override_collation)
        if search_text:
            qs = qs.search_text(search_text)
        if order_by:
            # add ordering
            qs = qs.order_by(*order_by)

        if include:
            # add projection
            qs = qs.only(*include)

        if exclude:
            qs = qs.exclude(*exclude)

        if page is not None and page_size:
            # add paging
            qs = qs.skip(page * page_size).limit(page_size)

        return qs

    @classmethod
    def _get_queries_for_order_field(
        cls, query: Q, order_field: str
    ) -> Union[None, Tuple[Q, Q]]:
        """
        In case the order_field is one of the cls fields and the sorting is ascending
        then return the tuple of 2 queries:
        1. original query with not empty constraint on the order_by field
        2. original query with empty constraint on the order_by field
        """
        if not order_field or order_field.startswith("-") or "[" in order_field:
            return

        mongo_field_name = order_field.replace(".", "__")
        mongo_field = first(
            v for k, v in cls.get_all_fields_with_instance() if k == mongo_field_name
        )

        if isinstance(mongo_field, ListField):
            params = {"is_list": True}
        elif isinstance(mongo_field, StringField):
            params = {"empty_value": ""}
        else:
            params = {}
        non_empty = query & field_exists(mongo_field_name, **params)
        empty = query & field_does_not_exist(mongo_field_name, **params)
        return non_empty, empty

    @classmethod
    def _get_many_override_none_ordering(
        cls: Union[Document, "GetMixin"],
        query: Q = None,
        parameters: dict = None,
        override_projection: Collection[str] = None,
        override_collation: dict = None,
    ) -> Sequence[dict]:
        """
        Fetch all documents matching a provided query. For the first order by field
        the None values are sorted in the end regardless of the sorting order.
        If the first order field is a user defined parameter (either from execution.parameters,
        or from last_metrics) then the collation is set that sorts strings in numeric order where possible.
        This is a company-less version for internal uses. We assume the caller has either added any necessary
        constraints to the query or that no constraints are required.

        NOTE: BE VERY CAREFUL WITH THIS CALL, as it allows returning data across companies.

        :param query: Query object (mongoengine.Q)
        :param parameters: Parameters dict from which paging ordering and searching parameters are extracted.
        :param override_projection: A list of projection fields overriding any projection specified in the `param_dict`
            argument
        """
        if not query:
            raise ValueError("query or call_data must be provided")

        parameters = parameters or {}
        search_text = parameters.get(cls._search_text_key)
        order_by = cls.validate_order_by(parameters=parameters, search_text=search_text)
        page, page_size = cls.validate_paging(parameters=parameters)
        include, exclude = cls.split_projection(
            cls.get_projection(parameters, override_projection)
        )

        query_sets = [cls.objects(query)]
        if order_by:
            order_field = first(
                field for field in order_by if not field.startswith("$")
            )
            res = cls._get_queries_for_order_field(query, order_field)
            if res:
                query_sets = [cls.objects(q) for q in res]
            query_sets = [qs.order_by(*order_by) for qs in query_sets]
            if order_field and not override_collation:
                override_collation = cls._get_collation_override(order_field)

        if override_collation:
            query_sets = [
                qs.collation(collation=override_collation) for qs in query_sets
            ]

        if search_text:
            query_sets = [qs.search_text(search_text) for qs in query_sets]

        if include:
            # add projection
            query_sets = [qs.only(*include) for qs in query_sets]

        if exclude:
            query_sets = [qs.exclude(*exclude) for qs in query_sets]

        if page is None or not page_size:
            return [obj.to_proper_dict(only=include) for qs in query_sets for obj in qs]

        # add paging
        ret = []
        start = page * page_size
        for qs in query_sets:
            qs_size = qs.count()
            if qs_size < start:
                start -= qs_size
                continue
            ret.extend(
                obj.to_proper_dict(only=include)
                for obj in qs.skip(start).limit(page_size)
            )
            if len(ret) >= page_size:
                break
            start = 0
            page_size -= len(ret)

        return ret

    @classmethod
    def get_for_writing(
        cls, *args, _only: Collection[str] = None, **kwargs
    ) -> "GetMixin":
        if _only and "company" not in _only:
            _only = list(set(_only) | {"company"})
        result = cls.get(*args, _only=_only, include_public=True, **kwargs)
        if result and not result.company:
            object_name = cls.__name__.lower()
            raise errors.forbidden.NoWritePermission(
                f"cannot modify public {object_name}(s), ids={(result.id,)}"
            )
        return result

    @classmethod
    def get_many_for_writing(cls, company, *args, **kwargs):
        result = cls.get_many(
            company=company,
            *args,
            **dict(return_dicts=False, **kwargs),
            allow_public=True,
        )
        forbidden_objects = {obj.id for obj in result if not obj.company}
        if forbidden_objects:
            object_name = cls.__name__.lower()
            raise errors.forbidden.NoWritePermission(
                f"cannot modify public {object_name}(s), ids={tuple(forbidden_objects)}"
            )
        return result


class UpdateMixin(object):
    __user_set_allowed_fields = None
    __locked_when_published_fields = None

    @classmethod
    def user_set_allowed(cls):
        if cls.__user_set_allowed_fields is None:
            cls.__user_set_allowed_fields = dict(
                get_fields_choices(cls, "user_set_allowed")
            )
        return cls.__user_set_allowed_fields

    @classmethod
    def locked_when_published(cls):
        if cls.__locked_when_published_fields is None:
            cls.__locked_when_published_fields = dict(
                get_fields_choices(cls, "locked_when_published")
            )
        return cls.__locked_when_published_fields

    @classmethod
    def get_safe_update_dict(cls, fields):
        if not fields:
            return {}
        valid_fields = cls.user_set_allowed()
        fields = [(k, v, fields[k]) for k, v in valid_fields.items() if k in fields]
        update_dict = {
            field: value
            for field, allowed, value in fields
            if allowed is None
            or (
                (value in allowed)
                if not isinstance(value, list)
                else all(v in allowed for v in value)
            )
        }
        return update_dict

    @classmethod
    def safe_update(
        cls: Union["UpdateMixin", Document],
        company_id,
        id,
        partial_update_dict,
        injected_update=None,
    ):
        update_dict = cls.get_safe_update_dict(partial_update_dict)
        if not update_dict:
            return 0, {}
        if injected_update:
            update_dict.update(injected_update)
        update_count = cls.objects(id=id, company=company_id).update(
            upsert=False, **update_dict
        )
        return update_count, update_dict


class DbModelMixin(GetMixin, ProperDictMixin, UpdateMixin):
    """ Provide convenience methods for a subclass of mongoengine.Document """

    @classmethod
    def aggregate(
        cls: Union["DbModelMixin", Document],
        pipeline: Sequence[dict],
        allow_disk_use=None,
        **kwargs,
    ) -> CommandCursor:
        """
        Aggregate objects of this document class according to the provided pipeline.
        :param pipeline: a list of dictionaries describing the pipeline stages
        :param allow_disk_use: if True, allow the server to use disk space if aggregation query cannot fit in memory.
        If None, default behavior will be used (see apiserver.conf/mongo/aggregate/allow_disk_use)
        :param kwargs: additional keyword arguments passed to mongoengine
        :return:
        """
        kwargs.update(
            allowDiskUse=allow_disk_use
            if allow_disk_use is not None
            else config.get("apiserver.mongo.aggregate.allow_disk_use", True)
        )
        return cls.objects.aggregate(pipeline, **kwargs)

    @classmethod
    def set_public(
        cls: Type[Document],
        company_id: str,
        ids: Sequence[str],
        invalid_cls: Type[BaseError],
        enabled: bool = True,
    ):
        if enabled:
            items = list(cls.objects(id__in=ids, company=company_id).only("id"))
            update = dict(set__company_origin=company_id, set__company="")
        else:
            items = list(
                cls.objects(
                    id__in=ids, company__in=(None, ""), company_origin=company_id
                ).only("id")
            )
            update = dict(set__company=company_id, unset__company_origin=1)

        if len(items) < len(ids):
            missing = tuple(set(ids).difference(i.id for i in items))
            raise invalid_cls(ids=missing)

        return {"updated": cls.objects(id__in=ids).update(**update)}


def validate_id(cls, company, **kwargs):
    """
    Validate existence of objects with certain IDs. within company.
    :param cls: Model class to search in
    :param company: Company to search in
    :param kwargs: Mapping of field name to object ID. If any ID does not have a corresponding object,
                    it will be reported along with the name it was assigned to.
    :return:
    """
    ids = set(kwargs.values())
    objs = list(cls.objects(company=company, id__in=ids).only("id"))
    missing = ids - set(x.id for x in objs)
    if not missing:
        return
    id_to_name = {}
    for name, obj_id in kwargs.items():
        id_to_name.setdefault(obj_id, []).append(name)
    raise errors.bad_request.ValidationError(
        "Invalid {} ids".format(cls.__name__.lower()),
        **{name: obj_id for obj_id in missing for name in id_to_name[obj_id]},
    )
