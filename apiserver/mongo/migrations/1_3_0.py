from pymongo.collection import Collection
from pymongo.database import Database

from apiserver.utilities.parameter_key_escaper import ParameterKeyEscaper
from .utils import _drop_all_indices_from_collections


def _convert_metadata(db: Database, name):
    collection: Collection = db[name]

    metadata_field = "metadata"
    query = {metadata_field: {"$exists": True, "$type": 4}}
    for doc in collection.find(filter=query, projection=(metadata_field,)):
        metadata = {
            ParameterKeyEscaper.escape(item["key"]): item
            for item in doc.get(metadata_field, [])
            if isinstance(item, dict) and "key" in item
        }
        collection.update_one(
            {"_id": doc["_id"]}, {"$set": {"metadata": metadata}},
        )


def migrate_backend(db: Database):
    collections = ["model", "queue"]
    for name in collections:
        _convert_metadata(db, name)

    _drop_all_indices_from_collections(db, collections)
