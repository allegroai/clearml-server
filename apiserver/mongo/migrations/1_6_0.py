from pymongo.collection import Collection
from pymongo.database import Database


def migrate_backend(db: Database):
    projects: Collection = db["project"]
    for doc in projects.find({"basename": None}):
        name: str = doc["name"]
        _, _, basename = name.rpartition("/")
        projects.update_one(
            {"_id": doc["_id"]}, {"$set": {"basename": basename}},
        )
