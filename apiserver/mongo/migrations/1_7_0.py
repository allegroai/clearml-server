from pymongo.collection import Collection
from pymongo.database import Database


def migrate_backend(db: Database, auth_db: Database):
    users: Collection = db["user"]
    auth_users: Collection = auth_db["user"]
    created_field = "created"
    for doc in users.find({created_field: {"$exists": False}}):
        auth_user = auth_users.find_one({"_id": doc["_id"]}, projection=[created_field])
        if not auth_user or created_field not in auth_user:
            continue
        users.update_one(
            {"_id": doc["_id"]}, {"$set": {created_field: auth_user[created_field]}}
        )
