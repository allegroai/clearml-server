from pymongo.database import Database, Collection

from apiserver.bll.task.artifacts import get_artifact_id
from apiserver.utilities.dicts import nested_get


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
