from typing import Union, Sequence

from database.utils import partition_tags
from service_repo import APICall
from service_repo.base import PartialVersion


def conform_output_tags(call: APICall, documents: Union[dict, Sequence[dict]]):
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
    Make sure that 'tags' from the old SDK clients
    are correctly split into 'tags' and 'system_tags'
    Make sure that there are no duplicate tags
    """
    if call.requested_endpoint_version < PartialVersion("2.3"):
        service_name = call.endpoint_name.partition(".")[0]
        upgrade_tags(
            service_name[:-1] if service_name.endswith("s") else service_name, document
        )
    remove_duplicate_tags(document)


def upgrade_tags(entity: str, document: dict):
    """
    If only 'tags' is present in the fields then extract
    the system tags from it to a separate field 'system_tags'
    """
    tags = document.get("tags")
    if tags is not None and not document.get("system_tags"):
        user_tags, system_tags = partition_tags(entity, tags)
        document["tags"] = user_tags
        document["system_tags"] = system_tags


def remove_duplicate_tags(document: dict):
    """
    Remove duplicates from 'tags' and 'system_tags' fields
    """
    for name in ("tags", "system_tags"):
        values = document.get(name)
        if values:
            document[name] = list(set(values))
