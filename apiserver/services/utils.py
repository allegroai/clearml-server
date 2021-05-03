from datetime import datetime
from typing import Union, Sequence, Tuple

from apiserver.apierrors import errors
from apiserver.apimodels.organization import Filter
from apiserver.database.model.base import GetMixin
from apiserver.database.utils import partition_tags
from apiserver.service_repo import APICall
from apiserver.utilities.dicts import nested_set, nested_get, nested_delete
from apiserver.utilities.partial_version import PartialVersion


def get_tags_filter_dictionary(input_: Filter) -> dict:
    if not input_:
        return {}

    return {
        field: vals
        for field, vals in (("tags", input_.tags), ("system_tags", input_.system_tags))
        if vals
    }


def get_tags_response(ret: dict) -> dict:
    return {field: sorted(vals) for field, vals in ret.items()}


def conform_output_tags(call: APICall, documents: Union[dict, Sequence[dict]]):
    """
    For old clients both tags and system tags are returned in 'tags' field
    """
    if call.requested_endpoint_version >= PartialVersion("2.3"):
        return
    if isinstance(documents, dict):
        documents = [documents]
    for doc in documents:
        system_tags = doc.get("system_tags")
        if system_tags:
            doc["tags"] = list(set(doc.get("tags", [])) | set(system_tags))


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
            t for t in values if t.startswith(GetMixin.ListFieldBucketHelper.op_prefix)
        ]
        if unsupported:
            raise errors.bad_request.FieldsValueError(
                "unsupported tag prefix", values=unsupported
            )


class ModelsBackwardsCompatibility:
    max_version = PartialVersion("2.13")
    mode_to_fields = {"input": ("execution", "model"), "output": ("output", "model")}
    models_field = "models"

    @classmethod
    def prepare_for_save(cls, call: APICall, fields: dict):
        if call.requested_endpoint_version > cls.max_version:
            return

        for mode, field in cls.mode_to_fields.items():
            value = nested_get(fields, field)
            if not value:
                continue

            nested_delete(fields, field)

            nested_set(
                fields,
                (cls.models_field, mode),
                value=[dict(name=mode, model=value, updated=datetime.utcnow())],
            )

    @classmethod
    def unprepare_from_saved(
        cls, call: APICall, tasks_data: Union[Sequence[dict], dict]
    ):
        if call.requested_endpoint_version > cls.max_version:
            return

        if isinstance(tasks_data, dict):
            tasks_data = [tasks_data]

        for task in tasks_data:
            for mode, field in cls.mode_to_fields.items():
                models = nested_get(task, (cls.models_field, mode))
                if not models:
                    continue

                model = models[0] if mode == "input" else models[-1]
                if model:
                    nested_set(task, field, model.get("model"))
