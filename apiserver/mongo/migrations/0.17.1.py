from datetime import datetime
from typing import Optional

from pymongo.database import Database


def _add_active_duration(db: Database):
    active_duration = "active_duration"
    query = {active_duration: {"$eq": None}}
    collection = db["task"]
    for doc in collection.find(
        filter=query, projection=[active_duration, "status", "started", "completed"]
    ):
        started = doc.get("started")
        completed = doc.get("completed")
        running = doc.get("status") == "running"
        if started and doc.get(active_duration) is None:
            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {active_duration: _get_active_duration(completed, running, started)}},
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
    """
    Add active_duration field to tasks
    """
    _add_active_duration(db)
