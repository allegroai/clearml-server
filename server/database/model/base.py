import re
from collections import namedtuple
from functools import reduce
from typing import Collection, Sequence, Union

from boltons.iterutils import first
from dateutil.parser import parse as parse_datetime
from mongoengine import Q, Document, ListField, StringField
from pymongo.command_cursor import CommandCursor

from apierrors import errors
from config import config
from database.errors import MakeGetAllQueryError
from database.projection import project_dict, ProjectionHelper
from database.props import PropsMixin
from database.query import RegexQ, RegexWrapper
from database.utils import (
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
    def to_proper_dict(self, strip_private=True, only=None, extra_dict=None) -> dict:
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

    _ordering_key = "order_by"
    _search_text_key = "search_text"

    _multi_field_param_sep = "__"
    _multi_field_param_prefix = {
        ("_any_", "_or_"): lambda a, b: a | b,
        ("_all_", "_and_"): lambda a, b: a & b,
    }
    MultiFieldParameters = namedtuple("MultiFieldParameters", "pattern fields")

    class QueryParameterOptions(object):
        def __init__(
            self,
            pattern_fields=("name",),
            list_fields=("tags", "system_tags", "id"),
            datetime_fields=None,
            fields=None,
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
            self.pattern_fields = pattern_fields

    get_all_query_options = QueryParameterOptions()

    @classmethod
    def get(
        cls, company, id, *, _only=None, include_public=False, **kwargs
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
            parameters = parameters.copy()
            opts = parameters_options
            for field in opts.pattern_fields:
                pattern = parameters.pop(field, None)
                if pattern:
                    dict_query[field] = RegexWrapper(pattern)

            for field in tuple(opts.list_fields or ()):
                data = parameters.pop(field, None)
                if data:
                    if not isinstance(data, (list, tuple)):
                        raise MakeGetAllQueryError("expected list", field)
                    exclude = [t for t in data if t.startswith("-")]
                    include = list(set(data).difference(exclude))
                    mongoengine_field = field.replace(".", "__")
                    if include:
                        dict_query[f"{mongoengine_field}__in"] = include
                    if exclude:
                        dict_query[f"{mongoengine_field}__nin"] = [
                            t[1:] for t in exclude
                        ]

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
                    regex = RegexWrapper(data.pattern, flags=re.IGNORECASE)
                    sep_fields = [f.replace(".", "__") for f in data.fields]
                    q = reduce(
                        lambda a, x: func(a, RegexQ(**{x: regex})), sep_fields, RegexQ()
                    )
                    query = query & q

        return query & RegexQ(**dict_query)

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
        return parameters.get("projection") or parameters.get("only_fields", [])

    @classmethod
    def set_default_ordering(cls, parameters, value):
        parameters[cls._ordering_key] = parameters.get(cls._ordering_key) or value

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
            )

        return cls._get_many_no_company(
            query=_query, parameters=parameters, override_projection=override_projection
        )

    @classmethod
    def _get_many_no_company(cls, query, parameters=None, override_projection=None):
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
        page, page_size = cls.validate_paging(parameters=parameters)
        only = cls.get_projection(parameters, override_projection)

        qs = cls.objects(query)
        if search_text:
            qs = qs.search_text(search_text)
        if order_by:
            # add ordering
            qs = qs.order_by(*order_by)
        if only:
            # add projection
            qs = qs.only(*only)
        else:
            exclude = set(cls.get_exclude_fields()).difference(only)
            if exclude:
                qs = qs.exclude(*exclude)
        if page is not None and page_size:
            # add paging
            qs = qs.skip(page * page_size).limit(page_size)

        return qs

    @classmethod
    def _get_many_override_none_ordering(
        cls: Union[Document, "GetMixin"],
        query: Q = None,
        parameters: dict = None,
        override_projection: Collection[str] = None,
    ) -> Sequence[dict]:
        """
        Fetch all documents matching a provided query. For the first order by field
        the None values are sorted in the end regardless of the sorting order.
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
        only = cls.get_projection(parameters, override_projection)

        query_sets = [cls.objects(query)]
        if order_by:
            order_field = first(
                field for field in order_by if not field.startswith("$")
            )
            if (
                order_field
                and not order_field.startswith("-")
                and "[" not in order_field
            ):
                params = {}
                mongo_field = order_field.replace(".", "__")
                if mongo_field in cls.get_field_names_for_type(of_type=ListField):
                    params["is_list"] = True
                elif mongo_field in cls.get_field_names_for_type(of_type=StringField):
                    params["empty_value"] = ""
                non_empty = query & field_exists(mongo_field, **params)
                empty = query & field_does_not_exist(mongo_field, **params)
                query_sets = [cls.objects(non_empty), cls.objects(empty)]

            query_sets = [qs.order_by(*order_by) for qs in query_sets]

        if search_text:
            query_sets = [qs.search_text(search_text) for qs in query_sets]

        if only:
            # add projection
            query_sets = [qs.only(*only) for qs in query_sets]
        else:
            exclude = set(cls.get_exclude_fields())
            if exclude:
                query_sets = [qs.exclude(*exclude) for qs in query_sets]

        if page is None or not page_size:
            return [obj.to_proper_dict(only=only) for qs in query_sets for obj in qs]

        # add paging
        ret = []
        start = page * page_size
        for qs in query_sets:
            qs_size = qs.count()
            if qs_size < start:
                start -= qs_size
                continue
            ret.extend(
                obj.to_proper_dict(only=only) for obj in qs.skip(start).limit(page_size)
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
    @classmethod
    def user_set_allowed(cls):
        res = getattr(cls, "__user_set_allowed_fields", None)
        if res is None:
            res = cls.__user_set_allowed_fields = get_fields_choices(
                cls, "user_set_allowed"
            )
        return res

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
    def safe_update(cls, company_id, id, partial_update_dict, injected_update=None):
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
        cls: Document, *pipeline: dict, allow_disk_use=None, **kwargs
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
        return cls.objects.aggregate(*pipeline, **kwargs)


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
        **{name: obj_id for obj_id in missing for name in id_to_name[obj_id]}
    )
