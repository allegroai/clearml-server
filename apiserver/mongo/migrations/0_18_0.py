from datetime import datetime

from pymongo.collection import Collection
from pymongo.database import Database

from apiserver.utilities.dicts import nested_get
from .utils import _drop_all_indices_from_collections


def migrate_backend(db: Database):
    """
    Collect the task output models from the models collections
    Move the execution and output models to new models.input and output lists
    Drop the task indices to accommodate the change in schema
    """
    tasks: Collection = db["task"]
    models: Collection = db["model"]

    models_field = "models"
    input = "input"
    output = "output"
    now = datetime.utcnow()

    pipeline = [
        {"$match": {"task": {"$exists": True}}},
        {"$project": {"name": 1, "task": 1}},
        {"$group": {"_id": "$task", "models": {"$push": "$$ROOT"}}},
    ]
    output_models = f"{models_field}.{output}"
    for group in models.aggregate(pipeline=pipeline, allowDiskUse=True):
        task_id = group.get("_id")
        task_models = group.get("models")
        if task_id and models:
            task_models = [
                {"model": m["_id"], "name": m.get("name", m["_id"]), "updated": now}
                for m in task_models
            ]
            tasks.update_one(
                {"_id": task_id, output_models: {"$in": [None, []]}},
                {"$set": {output_models: task_models}},
                upsert=False,
            )

    fields = {input: "execution.model", output: "output.model"}
    query = {
        "$or": [
            {field: {"$exists": True, "$nin": [None, ""]}} for field in fields.values()
        ]
    }
    for doc in tasks.find(filter=query, projection=[*fields.values(), models_field]):
        set_commands = {}
        for mode, field in fields.items():
            value = nested_get(doc, field.split("."))
            if not value:
                continue

            model_doc = models.find_one(filter={"_id": value}, projection=["name"])
            name = model_doc.get("name", mode) if model_doc else mode
            model_item = {"model": value, "name": name, "updated": now}
            existing_models = nested_get(doc, (models_field, mode), default=[])
            existing_models = (
                m
                for m in existing_models
                if m.get("name") != name and m.get("model") != value
            )
            if mode == input:
                updated_models = [model_item, *existing_models]
            else:
                updated_models = [*existing_models, model_item]
            set_commands[f"{models_field}.{mode}"] = updated_models

        tasks.update_one(
            {"_id": doc["_id"]},
            {
                "$unset": {field: 1 for field in fields.values()},
                **({"$set": set_commands} if set_commands else {}),
            },
        )

    _drop_all_indices_from_collections(db, ["task*"])
