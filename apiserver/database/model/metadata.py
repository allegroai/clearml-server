from typing import Sequence, Type

from mongoengine import EmbeddedDocument, StringField, Document
from pymongo import UpdateOne
from pymongo.collection import Collection

from apiserver.database.model.base import ProperDictMixin


class MetadataItem(EmbeddedDocument, ProperDictMixin):
    key = StringField(required=True)
    type = StringField(required=True)
    value = StringField(required=True)


def metadata_add_or_update(cls: Type[Document], _id: str, items: Sequence[dict]) -> int:
    collection: Collection = cls._get_collection()
    res = collection.update_one(
        filter={"_id": _id},
        update={
            "$set": {f"metadata.$[elem{idx}]": item for idx, item in enumerate(items)}
        },
        array_filters=[
            {f"elem{idx}.key": item["key"]} for idx, item in enumerate(items)
        ],
        upsert=False,
    )
    if len(items) == 1 and res.modified_count == 1:
        return res.modified_count

    requests = [
        UpdateOne(
            filter={"_id": _id, "metadata.key": {"$ne": item["key"]}},
            update={"$push": {"metadata": item}},
        )
        for item in items
    ]
    res = collection.bulk_write(requests)

    return 1 if res.modified_count else 0


def metadata_delete(cls: Type[Document], _id: str, keys: Sequence[str]) -> int:
    return cls.objects(id=_id).update_one(pull__metadata__key__in=keys)
