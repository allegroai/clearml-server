from typing import Union, Sequence, Tuple

from database.utils import partition_tags
from service_repo import APICall
from service_repo.base import PartialVersion


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


def conform_tag_fields(call: APICall, document: dict):
    """
    Upgrade old client tags in place
    """
    if "tags" in document:
        tags, system_tags = conform_tags(
            call, document["tags"], document.get("system_tags")
        )
        if tags != document.get("tags"):
            document["tags"] = tags
        if system_tags != document.get("system_tags"):
            document["system_tags"] = system_tags


def conform_tags(
    call: APICall, tags: Sequence, system_tags: Sequence
) -> Tuple[Sequence, Sequence]:
    """
    Make sure that 'tags' from the old SDK clients
    are correctly split into 'tags' and 'system_tags'
    Make sure that there are no duplicate tags
    """
    if call.requested_endpoint_version < PartialVersion("2.3"):
        tags, system_tags = _upgrade_tags(call, tags, system_tags)
    return _get_unique_values(tags), _get_unique_values(system_tags)


def _upgrade_tags(call: APICall, tags: Sequence, system_tags: Sequence):
    if tags is not None and not system_tags:
        service_name = call.endpoint_name.partition(".")[0]
        entity = service_name[:-1] if service_name.endswith("s") else service_name
        return partition_tags(entity, tags)

    return tags, system_tags


def _get_unique_values(values: Sequence) -> Sequence:
    """Get unique values from the given sequence"""
    if not values:
        return values

    return list(set(values))
