from pymongo.database import Database, Collection

from apiserver.database.utils import partition_tags


def migrate_backend(db: Database):
    for name in ("project", "task", "model"):
        collection: Collection = db[name]
        for doc in collection.find(projection=["tags", "system_tags"]):
            tags = doc.get("tags")
            if tags is not None:
                user_tags, system_tags = partition_tags(
                    name, tags, doc.get("system_tags", [])
                )
                collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"system_tags": system_tags, "tags": user_tags}}
                )
