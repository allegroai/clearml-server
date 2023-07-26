import threading
from concurrent.futures import ThreadPoolExecutor
from itertools import groupby, chain
from typing import Sequence, Dict, Callable

from boltons import iterutils

from apiserver.apierrors import errors
from apiserver.config_repo import config
from apiserver.database.props import PropsMixin

SEP = "."
max_items_per_fetch = config.get("services._mongo.max_page_size", 500)


class _ReferenceProxy(dict):
    def __init__(self, id):
        super(_ReferenceProxy, self).__init__(**({"id": id} if id else {}))


class _ProxyManager:
    lock = threading.Lock()

    def __init__(self):
        self._proxies: Dict[str, _ReferenceProxy] = {}

    def add(self, id):
        with self.lock:
            proxy = self._proxies.get(id)
            if proxy is None:
                proxy = self._proxies[id] = _ReferenceProxy(id)
            return proxy

    def update(self, result):
        proxy = self._proxies.get(result.get("id"))
        if proxy is not None:
            proxy.update(result)


class ProjectionHelper(object):
    pool = ThreadPoolExecutor()
    exclusion_prefix = "-"

    @property
    def doc_projection(self):
        return self._doc_projection

    def __init__(self, doc_cls, projection, expand_reference_ids=False):
        super(ProjectionHelper, self).__init__()
        self._should_expand_reference_ids = expand_reference_ids
        self._doc_cls = doc_cls
        self._doc_projection = None
        self._ref_projection = None
        self._proxy_manager = _ProxyManager()

        self._parse_projection(projection)

    def _collect_projection_fields(self, doc_cls, projection):
        """
        Collect projection for the given document into immediate document projection and reference documents projection
        :param doc_cls: Document class
        :param projection: List of projection fields
        :return: A tuple of document projection and reference fields information
        """
        doc_projection = (
            set()
        )  # Projection fields for this class (used in the main query)
        ref_projection_info = (
            []
        )  # Projection information for reference fields (used in join queries)
        for field in projection:
            field_ = field.lstrip(self.exclusion_prefix)
            for ref_field, ref_field_cls in doc_cls.get_reference_fields().items():
                if not field_.startswith(ref_field):
                    # Doesn't start with a reference field
                    continue
                if field_ == ref_field:
                    # Field is exactly a reference field. In this case we won't perform any inner projection (for that,
                    # use '<reference field name>.*')
                    continue
                subfield = field_[len(ref_field) :]
                if not subfield.startswith(SEP):
                    # Starts with something that looks like a reference field, but isn't
                    continue

                ref_projection_info.append(
                    (
                        ref_field,
                        ref_field_cls,
                        ("" if field_[0] == field[0] else self.exclusion_prefix)
                        + subfield[1:],
                    )
                )
                break
            else:
                # Not a reference field, just add to the top-level projection
                # We strip any trailing '*' since it means nothing for simple fields and for embedded documents
                orig_field = field
                if field.endswith(".*"):
                    field = field[:-2]
                if not field.lstrip(self.exclusion_prefix):
                    raise errors.bad_request.InvalidFields(
                        field=orig_field, object=doc_cls.__name__
                    )
                doc_projection.add(field)
        return doc_projection, ref_projection_info

    def _parse_projection(self, projection):
        """
        Prepare the projection data structures for get_many_with_join().
        :param projection: A list of field names that should be returned by the query. Sub-fields can be specified
            using '.' (i.e. "parent.name"). A field terminated by '.*' indicated that all of the field's sub-fields
            should be returned (only relevant for fields that represent sub-documents or referenced documents)
        :type projection: list of strings
        :returns A tuple of (class fields projection, reference fields projection)
        """
        doc_cls = self._doc_cls
        assert issubclass(doc_cls, PropsMixin)
        if not projection:
            return [], {}

        doc_projection, ref_projection_info = self._collect_projection_fields(
            doc_cls, projection
        )

        def normalize_cls_projection(cls_, fields):
            """ Normalize projection for this class and group (expand *, for once) """
            if "*" in fields:
                return list(fields.difference("*").union(cls_.get_fields()))
            return list(fields)

        def compute_ref_cls_projection(cls_, group):
            """ Compute inner projection for this class and group """
            subfields = set([x[2] for x in group if x[2]])
            return normalize_cls_projection(cls_, subfields)

        def sort_key(proj_info):
            return proj_info[:2]

        # Aggregate by reference field. We'll leave out '*' from the projected items since
        ref_projection = {
            ref_field: dict(cls=ref_cls, only=compute_ref_cls_projection(ref_cls, g))
            for (ref_field, ref_cls), g in groupby(
                sorted(ref_projection_info, key=sort_key), sort_key
            )
        }

        # Make sure this doesn't contain any reference field we'll join anyway
        # (i.e. in case only_fields=[project, project.name])
        doc_projection = normalize_cls_projection(
            doc_cls, doc_projection.difference(ref_projection)
        )

        # Make sure that in case one or more field is a subfield of another field, we only use the the top-level field.
        # This is done since in such a case, MongoDB will only use the most restrictive field (most nested field) and
        # won't return some of the data we need.
        # This way, we make sure to use the most inclusive field that contains all requested subfields.
        projection_set = set(doc_projection)
        doc_projection = [
            field
            for field in doc_projection
            if not any(
                field.startswith(f"{other_field}.")
                for other_field in projection_set - {field}
            )
        ]

        # Make sure we didn't get any invalid projection fields for this class
        invalid_fields = [
            f
            for f in doc_projection
            if f.partition(SEP)[0].lstrip(self.exclusion_prefix)
            not in doc_cls.get_fields()
        ]
        if invalid_fields:
            raise errors.bad_request.InvalidFields(
                fields=invalid_fields, object=doc_cls.__name__
            )

        if ref_projection:
            # Join mode - use both normal projection fields and top-level reference fields
            doc_projection = set(doc_projection)
            for field in set(ref_projection).difference(doc_projection):
                if any(f for f in doc_projection if field.startswith(f)):
                    continue
                doc_projection.add(field)
            doc_projection = list(doc_projection)

        # If there are include fields (not only exclude) then add an id field
        if (
            not all(p.startswith(self.exclusion_prefix) for p in doc_projection)
            and "id" not in doc_projection
        ):
            doc_projection.append("id")

        self._doc_projection = doc_projection
        self._ref_projection = ref_projection

    def _search(
        self,
        doc_cls: PropsMixin,
        obj: dict,
        path: str,
        factory: Callable[[str], dict] = None,
    ) -> Sequence[str]:
        """
        Search for a path in the given object, return the list of values found for the
        given path (multiple values may exist if the path is a glob expression)
        :param doc_cls: The document class represented by the object
        :param obj: Data object
        :param path: Path to a leaf in the data object ("." separated, may contain "*")
         (in case the path contains "*", there may be multiple values)
        :param factory: If provided, replace each value found with an instance provided by the factory.
        """
        norm_path = doc_cls.get_dpath_translated_path(path)
        globlist = norm_path.strip(SEP).split(SEP)

        def _search_and_replace(target: dict, p: Sequence[str]) -> Sequence[str]:
            parent = None
            for idx, part in enumerate(p):
                if isinstance(target, dict) and part in target:
                    parent = target
                    target = target[part]
                elif isinstance(target, list) and part == "*":
                    return list(
                        chain.from_iterable(
                            _search_and_replace(t, p[idx + 1 :]) for t in target
                        )
                    )
                else:
                    return []

            if parent and factory:
                parent[p[-1]] = factory(target)
            return [target]

        return _search_and_replace(obj, globlist)

    def project(self, results, projection_func):
        """
        Perform projection on query results, using the provided projection func.
        :param results: A list of results dictionaries on which projection should be performed
        :param projection_func: A callable that receives a document type, list of ids and projection and returns query
            results. This callable is used in order to perform sub-queries during projection
        :return: Modified results (in-place)
        """
        cls = self._doc_cls
        ref_projection = self._ref_projection

        if ref_projection:
            # Join mode - get results for each reference fields projection required (this is the join step)
            # Note: this is a recursive step, so nested reference fields are supported

            def collect_ids(ref_field_name):
                """
                Collect unique IDs for the given reference path from all result documents.
                All collected IDs are replaced in the result dictionaries with a reference proxy generated by the
                proxies manager to allow rapid update later on when projection results are obtained.
                """
                all_ids = (
                    self._search(
                        cls, res, ref_field_name, factory=self._proxy_manager.add
                    )
                    for res in results
                )
                return list(filter(None, set(chain.from_iterable(all_ids))))

            items = [
                tup
                for tup in (
                    (*item, collect_ids(item[0])) for item in ref_projection.items()
                )
                if tup[2]
            ]

            if items:

                def do_projection(item):
                    ref_field_name, data, ids = item

                    doc_type = data["cls"]
                    doc_only = list(filter(None, data["only"]))
                    doc_only = list({"id"} | set(doc_only)) if doc_only else None

                    for ids_chunk in iterutils.chunked_iter(ids, max_items_per_fetch):
                        for res in projection_func(
                            doc_type=doc_type, projection=doc_only, ids=ids_chunk
                        ):
                            self._proxy_manager.update(res)

                if len(ref_projection) == 1:
                    do_projection(items[0])
                else:
                    for _ in self.pool.map(do_projection, items):
                        # From ThreadPoolExecutor.map() documentation: If a call raises an exception then that exception
                        #  will be raised when its value is retrieved from the map() iterator
                        pass

        def do_expand_reference_ids(result, skip_fields=None):
            ref_fields = cls.get_reference_fields()
            if skip_fields:
                ref_fields = set(ref_fields) - set(skip_fields)
            self._expand_reference_fields(cls, result, ref_fields)

        # any reference field not projected should be expanded
        if self._should_expand_reference_ids:
            for result in results:
                do_expand_reference_ids(
                    result, skip_fields=list(ref_projection) if ref_projection else None
                )

        return results

    def _expand_reference_fields(self, doc_cls, result, fields):
        for ref_field_name in fields:
            self._search(doc_cls, result, ref_field_name, factory=_ReferenceProxy)

    def expand_reference_ids(self, doc_cls, result):
        self._expand_reference_fields(doc_cls, result, doc_cls.get_reference_fields())
