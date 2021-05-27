from pymongo.collection import Collection
from pymongo.database import Database


def _migrate_project_description(db: Database):
    projects: Collection = db["project"]
    filter = {
        "$or": [
            {
                "$expr": {"$lt": [{"$strLenCP": "$description"}, 100]},
                "description": {"$regex": "^Auto-generated at ", "$options": "i"},
            },
            {"description": {"$regex": "^Auto-generated during move$", "$options": "i"}},
            {"description": {"$regex": "^Auto-generated while cloning$", "$options": "i"}},
        ]
    }
    for doc in projects.find(filter=filter):
        projects.update_one({"_id": doc["_id"]}, {"$unset": {"description": 1}})


def migrate_backend(db: Database):
    _migrate_project_description(db)
