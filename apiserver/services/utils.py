from datetime import datetime
from typing import Union, Sequence, Tuple

from apiserver.apierrors import errors
from apiserver.apimodels.organization import Filter
from apiserver.bll.project import project_ids_with_children
from apiserver.database.model.base import GetMixin
from apiserver.database.model.task.task import TaskModelTypes, TaskModelNames
from apiserver.database.utils import partition_tags
from apiserver.service_repo import APICall
from apiserver.utilities.dicts import nested_set, nested_get, nested_delete
from apiserver.utilities.parameter_key_escaper import ParameterKeyEscaper
from apiserver.utilities.partial_version import PartialVersion


def process_include_subprojects(call_data: dict):
    include_subprojects = call_data.pop("include_subprojects", False)
    project_ids = call_data.get("project")
    if not project_ids or not include_subprojects:
        return

    if not isinstance(project_ids, list):
        project_ids = [project_ids]
    call_data["project"] = project_ids_with_children(project_ids)


def get_tags_filter_dictionary(input_: Filter) -> dict:
    if not input_:
        return {}

    return {
        field: vals
        for field, vals in (("tags", input_.tags), ("system_tags", input_.system_tags))
        if vals
    }


def sort_tags_response(ret: dict) -> dict:
    return {field: sorted(vals) for field, vals in ret.items()}


def conform_output_tags(call: APICall, documents: Union[dict, Sequence[dict]]):
    """
    Make sure that tags are always returned sorted
    For old clients both tags and system tags are returned in 'tags' field
    """
    if isinstance(documents, dict):
        documents = [documents]

    merge_tags = call.requested_endpoint_version < PartialVersion("2.3")
    for doc in documents:
        if merge_tags:
            system_tags = doc.get("system_tags")
            if system_tags:
                doc["tags"] = list(set(doc.get("tags", [])) | set(system_tags))

        for field in ("system_tags", "tags"):
            tags = doc.get(field)
            if tags:
                doc[field] = sorted(tags)


def conform_tag_fields(call: APICall, document: dict, validate=False):
    """
    Upgrade old client tags in place
    """
    if "tags" in document:
        tags, system_tags = conform_tags(
            call, document["tags"], document.get("system_tags"), validate
        )
        if tags != document.get("tags"):
            document["tags"] = tags
        if system_tags != document.get("system_tags"):
            document["system_tags"] = system_tags


def conform_tags(
    call: APICall, tags: Sequence, system_tags: Sequence, validate=False
) -> Tuple[Sequence, Sequence]:
    """
    Make sure that 'tags' from the old SDK clients
    are correctly split into 'tags' and 'system_tags'
    Make sure that there are no duplicate tags
    """
    if validate:
        validate_tags(tags, system_tags)
    if call.requested_endpoint_version < PartialVersion("2.3"):
        tags, system_tags = _upgrade_tags(call, tags, system_tags)
    return tags, system_tags


def _upgrade_tags(call: APICall, tags: Sequence, system_tags: Sequence):
    if tags is not None and not system_tags:
        service_name = call.endpoint_name.partition(".")[0]
        entity = service_name[:-1] if service_name.endswith("s") else service_name
        return partition_tags(entity, tags)

    return tags, system_tags


def validate_tags(tags: Sequence[str], system_tags: Sequence[str]):
    for values in filter(None, (tags, system_tags)):
        unsupported = [
            t for t in values if t.startswith(GetMixin.NewListFieldBucketHelper.op_prefix)
        ]
        if unsupported:
            raise errors.bad_request.FieldsValueError(
                "unsupported tag prefix", values=unsupported
            )


def escape_dict(data: dict) -> dict:
    if not data:
        return data

    return {ParameterKeyEscaper.escape(k): v for k, v in data.items()}


def unescape_dict(data: dict) -> dict:
    if not data:
        return data

    return {ParameterKeyEscaper.unescape(k): v for k, v in data.items()}


def escape_dict_field(fields: dict, path: Union[str, Sequence[str]]):
    if isinstance(path, str):
        path = (path,)

    data = nested_get(fields, path)
    if not data or not isinstance(data, dict):
        return

    nested_set(fields, path, escape_dict(data))


def unescape_dict_field(fields: dict, path: Union[str, Sequence[str]]):
    if isinstance(path, str):
        path = (path,)

    data = nested_get(fields, path)
    if not data or not isinstance(data, dict):
        return

    nested_set(fields, path, unescape_dict(data))


class ModelsBackwardsCompatibility:
    max_version = PartialVersion("2.13")
    mode_to_fields = {
        TaskModelTypes.input: ("execution", "model"),
        TaskModelTypes.output: ("output", "model"),
    }
    models_field = "models"

    @classmethod
    def prepare_for_save(cls, call: APICall, fields: dict):
        if call.requested_endpoint_version >= cls.max_version:
            return

        for mode, field in cls.mode_to_fields.items():
            value = nested_get(fields, field)
            if value is None:
                continue
            val = [
                dict(
                    name=TaskModelNames[mode],
                    model=value,
                    updated=datetime.utcnow(),
                )
            ] if value else []
            nested_set(fields, (cls.models_field, mode), value=val)

            nested_delete(fields, field)

    @classmethod
    def unprepare_from_saved(
        cls, call: APICall, tasks_data: Union[Sequence[dict], dict]
    ):
        if call.requested_endpoint_version >= cls.max_version:
            return

        if isinstance(tasks_data, dict):
            tasks_data = [tasks_data]

        for task in tasks_data:
            for mode, field in cls.mode_to_fields.items():
                models = nested_get(task, (cls.models_field, mode))
                if not models:
                    continue

                model = models[0] if mode == TaskModelTypes.input else models[-1]
                if model:
                    nested_set(task, field, model.get("model"))


class DockerCmdBackwardsCompatibility:
    max_version = PartialVersion("2.13")
    field = ("execution", "docker_cmd")

    @classmethod
    def prepare_for_save(cls, call: APICall, fields: dict):
        if call.requested_endpoint_version >= cls.max_version:
            return

        docker_cmd = nested_get(fields, cls.field)
        if docker_cmd is not None:
            image, _, arguments = docker_cmd.partition(" ")
            nested_set(fields, ("container", "image"), value=image)
            nested_set(fields, ("container", "arguments"), value=arguments)

        nested_delete(fields, cls.field)

    @classmethod
    def unprepare_from_saved(
        cls, call: APICall, tasks_data: Union[Sequence[dict], dict]
    ):
        if call.requested_endpoint_version >= cls.max_version:
            return

        if isinstance(tasks_data, dict):
            tasks_data = [tasks_data]

        for task in tasks_data:
            container = task.get("container")
            if not container or not container.get("image"):
                continue

            docker_cmd = " ".join(
                filter(None, map(container.get, ("image", "arguments")))
            )
            if docker_cmd:
                nested_set(task, cls.field, docker_cmd)


def escape_metadata(document: dict):
    """
    Escape special characters in metadata keys
    """
    metadata = document.get("metadata")
    if not metadata:
        return

    document["metadata"] = {
        ParameterKeyEscaper.escape(k): v
        for k, v in metadata.items()
    }


def unescape_metadata(call: APICall, documents: Union[dict, Sequence[dict]]):
    """
    Unescape special characters in metadata keys
    """
    if isinstance(documents, dict):
        documents = [documents]

    old_client = call.requested_endpoint_version <= PartialVersion("2.16")
    for doc in documents:
        if old_client and "metadata" in doc:
            doc["metadata"] = []
            continue

        metadata = doc.get("metadata")
        if not metadata:
            continue

        doc["metadata"] = {
            ParameterKeyEscaper.unescape(k): v
            for k, v in metadata.items()
        }
