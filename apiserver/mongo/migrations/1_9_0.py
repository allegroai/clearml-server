import logging as log

from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import OperationFailure


def migrate_backend(db: Database):
    """
    Drop task text index so that the new one including reports field is created
    """
    tasks: Collection = db["task"]
    try:
        tasks.drop_index("backend-db.task.main_text_index")
    except OperationFailure as ex:
        log.warning(f"Could not delete task text index due to: {str(ex)}")
        pass
