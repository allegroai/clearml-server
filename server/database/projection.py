from concurrent.futures import ThreadPoolExecutor
from itertools import groupby, chain

import dpath

from apierrors import errors
from database.props import PropsMixin


def project_dict(data, projection, separator='.'):
    """
    Project partial data from a dictionary into a new dictionary
    :param data: Input dictionary
    :param projection: List of dictionary paths (each a string with field names separated using a separator)
    :param separator: Separator (default is '.')
    :return: A new dictionary containing only the projected parts from the original dictionary
    """
    assert isinstance(data, dict)
    result = {}

    def copy_path(path_parts, source, destination):
        src, dst = source, destination
        try:
            for depth, path_part in enumerate(path_parts[:-1]):
                src_part = src[path_part]
                if isinstance(src_part, dict):
                    src = src_part
                    dst = dst.setdefault(path_part, {})
                elif isinstance(src_part, (list, tuple)):
                    if path_part not in dst:
                        dst[path_part] = [{} for _ in range(len(src_part))]
                    elif not isinstance(dst[path_part], (list, tuple)):
                        raise TypeError('Incompatible destination type %s for %s (list expected)'
                                        % (type(dst), separator.join(path_parts[:depth + 1])))
                    elif not len(dst[path_part]) == len(src_part):
                        raise ValueError('Destination list length differs from source length for %s'
                                         % separator.join(path_parts[:depth + 1]))

                    dst[path_part] = [copy_path(path_parts[depth + 1:], s, d)
                                      for s, d in zip(src_part, dst[path_part])]

                    return destination
                else:
                    raise TypeError('Unsupported projection type %s for %s'
                                    % (type(src), separator.join(path_parts[:depth + 1])))

            last_part = path_parts[-1]
            dst[last_part] = src[last_part]
        except KeyError:
            # Projection field not in source, no biggie.
            pass
        return destination

    for projection_path in sorted(projection):
        copy_path(
            path_parts=projection_path.split(separator),
            source=data,
            destination=result)
    return result


class ProjectionHelper(object):
    pool = ThreadPoolExecutor()

    @property
    def doc_projection(self):
        return self._doc_projection

    def __init__(self, doc_cls, projection, expand_reference_ids=False):
        super(ProjectionHelper, self).__init__()
        self._should_expand_reference_ids = expand_reference_ids
        self._doc_cls = doc_cls
        self._doc_projection = None
        self._ref_projection = None
        self._parse_projection(projection)

    def _collect_projection_fields(self, doc_cls, projection):
        """
        Collect projection for the given document into immediate document projection and reference documents projection
        :param doc_cls: Document class
        :param projection: List of projection fields
        :return: A tuple of document projection and reference fields information
        """
        doc_projection = set()  # Projection fields for this class (used in the main query)
        ref_projection_info = []  # Projection information for reference fields (used in join queries)
        for field in projection:
            for ref_field, ref_field_cls in doc_cls.get_reference_fields().items():
                if not field.startswith(ref_field):
                    # Doesn't start with a reference field
                    continue
                if field == ref_field:
                    # Field is exactly a reference field. In this case we won't perform any inner projection (for that,
                    # use '<reference field name>.*')
                    continue
                subfield = field[len(ref_field):]
                if not subfield.startswith('.'):
                    # Starts with something that looks like a reference field, but isn't
                    continue

                ref_projection_info.append((ref_field, ref_field_cls, subfield[1:]))
                break
            else:
                # Not a reference field, just add to the top-level projection
                # We strip any trailing '*' since it means nothing for simple fields and for embedded documents
                orig_field = field
                if field.endswith('.*'):
                    field = field[:-2]
                if not field:
                    raise errors.bad_request.InvalidFields(field=orig_field, object=doc_cls.__name__)
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

        doc_projection, ref_projection_info = self._collect_projection_fields(doc_cls, projection)

        def normalize_cls_projection(cls_, fields):
            """ Normalize projection for this class and group (expand *, for once) """
            if '*' in fields:
                return list(fields.difference('*').union(cls_.get_fields()))
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
            for (ref_field, ref_cls), g in groupby(sorted(ref_projection_info, key=sort_key), sort_key)
        }

        # Make sure this doesn't contain any reference field we'll join anyway
        # (i.e. in case only_fields=[project, project.name])
        doc_projection = normalize_cls_projection(doc_cls, doc_projection.difference(ref_projection).union({'id'}))

        # Make sure that in case one or more field is a subfield of another field, we only use the the top-level field.
        # This is done since in such a case, MongoDB will only use the most restrictive field (most nested field) and
        # won't return some of the data we need.
        # This way, we make sure to use the most inclusive field that contains all requested subfields.
        projection_set = set(doc_projection)
        doc_projection = [
            field
            for field in doc_projection
            if not any(field.startswith(f"{other_field}.") for other_field in projection_set - {field})
        ]

        # Make sure we didn't get any invalid projection fields for this class
        invalid_fields = [f for f in doc_projection if f.split('.')[0] not in doc_cls.get_fields()]
        if invalid_fields:
            raise errors.bad_request.InvalidFields(fields=invalid_fields, object=doc_cls.__name__)

        if ref_projection:
            # Join mode - use both normal projection fields and top-level reference fields
            doc_projection = set(doc_projection)
            for field in set(ref_projection).difference(doc_projection):
                if any(f for f in doc_projection if field.startswith(f)):
                    continue
                doc_projection.add(field)
            doc_projection = list(doc_projection)

        self._doc_projection = doc_projection
        self._ref_projection = ref_projection

    @staticmethod
    def _search(doc_cls, obj, path, only_values=True):
        """ Call dpath.search with yielded=True, collect result values """
        norm_path = doc_cls.get_dpath_translated_path(path)
        return [v if only_values else (k, v) for k, v in dpath.search(obj, norm_path, separator='.', yielded=True)]

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
            # Note: this is a recursive step, so we support nested reference fields

            def do_projection(item):
                ref_field_name, data = item
                res = {}
                ids = list(filter(None, set(chain.from_iterable(self._search(cls, res, ref_field_name)
                                                                for res in results))))
                if ids:
                    doc_type = data['cls']
                    doc_only = list(filter(None, data['only']))
                    doc_only = list({'id'} | set(doc_only)) if doc_only else None
                    res = {r['id']: r for r in projection_func(doc_type=doc_type, projection=doc_only, ids=ids)}
                data['res'] = res

            items = list(ref_projection.items())
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

        def merge_projection_result(result):
            for ref_field_name, data in ref_projection.items():
                res = data.get('res')
                if not res:
                    self._expand_reference_fields(cls, result, [ref_field_name])
                    continue
                ref_ids = self._search(cls, result, ref_field_name, only_values=False)
                if not ref_ids:
                    continue
                for path, value in ref_ids:
                    obj = res.get(value) or {'id': value}
                    dpath.new(result, path, obj, separator='.')

            # any reference field not projected should be expanded
            do_expand_reference_ids(result, skip_fields=list(ref_projection))

        update_func = merge_projection_result if ref_projection else \
            do_expand_reference_ids if self._should_expand_reference_ids else None

        if update_func:
            for result in results:
                update_func(result)

        return results

    @classmethod
    def _expand_reference_fields(cls, doc_cls, result, fields):
        for ref_field_name in fields:
            ref_ids = cls._search(doc_cls, result, ref_field_name, only_values=False)
            if not ref_ids:
                continue
            for path, value in ref_ids:
                dpath.set(
                    result,
                    path,
                    {'id': value} if value else {},
                    separator='.')

    @classmethod
    def expand_reference_ids(cls, doc_cls, result):
        cls._expand_reference_fields(doc_cls, result, doc_cls.get_reference_fields())
