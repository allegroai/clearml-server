from pymongo.database import Database


def _add_active_duration(db: Database):
    active_duration = "active_duration"
    query = {active_duration: {"$eq": None}}
    collection = db["task"]
    for doc in collection.find(
        filter=query, projection=[active_duration, "started", "last_update"]
    ):
        started = doc.get("started")
        last_update = doc.get("last_update")
        if started and last_update and doc.get(active_duration) is None:
            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {active_duration: (last_update - started).total_seconds()}},
            )


def migrate_backend(db: Database):
    """
    Add active_duration field to tasks
    """
    _add_active_duration(db)
