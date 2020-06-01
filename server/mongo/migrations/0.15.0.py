from collections import Collection
from typing import Sequence

from pymongo.database import Database, Collection


def _drop_all_indices_from_collections(db: Database, names: Sequence[str]):
    for collection_name in db.list_collection_names():
        if collection_name not in names:
            continue
        collection: Collection = db[collection_name]
        collection.drop_indexes()


def migrate_auth(db: Database):
    """
    Remove the old indices from the collections since
    they may come out of sync with the latest changes
    in the code and mongo libraries update
    """
    _drop_all_indices_from_collections(db, ["user"])


def migrate_backend(db: Database):
    """
    1. Sort tags and system tags
    2. Remove the old indices from the collections since
        they may come out of sync with the latest changes
        in the code and mongo libraries update
    """

    fields = ("tags", "system_tags")
    query = {"$or": [{field: {"$exists": True, "$ne": []}} for field in fields]}
    for collection_name in ("task", "model", "project", "queue"):
        collection = db[collection_name]
        for doc in collection.find(filter=query, projection=fields):
            update = {
                field: sorted(doc[field])
                for field in fields
                if doc.get(field)
            }
            if update:
                collection.update_one({"_id": doc["_id"]}, {"$set": update})

    _drop_all_indices_from_collections(
        db,
        [
            "company",
            "model",
            "project",
            "queue",
            "settings",
            "task",
            "task__trash",
            "user",
            "versions",
        ],
    )
