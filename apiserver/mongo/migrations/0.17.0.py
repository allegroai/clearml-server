from datetime import datetime
from typing import Optional

from pymongo.database import Database, Collection

from apiserver.bll.task.artifacts import get_artifact_id
from apiserver.utilities.dicts import nested_get


def _add_active_duration(db: Database):
    active_duration_key = "active_duration"
    query = {"$or": [{active_duration_key: {"$eq": None}}, {active_duration_key: {"$eq": 0}}]}
    collection = db["task"]
    for doc in collection.find(
        filter=query, projection=[active_duration_key, "status", "started", "completed"]
    ):
        started = doc.get("started")
        completed = doc.get("completed")
        running = doc.get("status") == "running"
        active_duration_value = doc.get(active_duration_key)
        if active_duration_value == 0:
            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {active_duration_key: None}},
            )
        elif started and active_duration_value is None:
            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {active_duration_key: _get_active_duration(completed, running, started)}},
            )


def _get_active_duration(
    completed: datetime, running: bool, started: datetime
) -> Optional[float]:
    if running:
        return (datetime.utcnow() - started).total_seconds()
    elif completed:
        return (completed - started).total_seconds()
    else:
        return None


def migrate_backend(db: Database):
    collection: Collection = db["task"]
    artifacts_field = "execution.artifacts"
    query = {artifacts_field: {"$type": 4}}
    for doc in collection.find(filter=query, projection=(artifacts_field,)):
        artifacts = nested_get(doc, artifacts_field.split("."))
        if not isinstance(artifacts, list):
            continue

        new_artifacts = {get_artifact_id(a): a for a in artifacts}
        collection.update_one(
            {"_id": doc["_id"]}, {"$set": {artifacts_field: new_artifacts}}
        )

    _add_active_duration(db)


def migrate_auth(db: Database):
    """
    Remove the old indices from the user collections
    to enable building of the updated user email index
    """
    collection: Collection = db["user"]
    collection.drop_indexes()
