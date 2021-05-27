import os
import re
from datetime import datetime

from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from apiserver.database.model.task.task import TaskModelTypes, TaskModelNames
from apiserver.services.utils import escape_dict
from apiserver.utilities.dicts import nested_get
from .utils import _drop_all_indices_from_collections


def _migrate_task_models(db: Database):
    """
    Move the execution and output models to new models.input and output lists
    """
    tasks: Collection = db["task"]

    models_field = "models"
    now = datetime.utcnow()

    fields = {
        TaskModelTypes.input: "execution.model",
        TaskModelTypes.output: "output.model",
    }
    query = {"$or": [{field: {"$exists": True}} for field in fields.values()]}
    for doc in tasks.find(filter=query, projection=[*fields.values(), models_field]):
        set_commands = {}
        for mode, field in fields.items():
            value = nested_get(doc, field.split("."))
            if value:
                name = TaskModelNames[mode]
                model_item = {"model": value, "name": name, "updated": now}
                existing_models = nested_get(doc, (models_field, mode), default=[])
                existing_models = (
                    m
                    for m in existing_models
                    if m.get("name") != name and m.get("model") != value
                )
                if mode == TaskModelTypes.input:
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


def _migrate_docker_cmd(db: Database):
    tasks: Collection = db["task"]

    docker_cmd_field = "execution.docker_cmd"
    query = {docker_cmd_field: {"$exists": True}}

    for doc in tasks.find(filter=query, projection=(docker_cmd_field,)):
        set_commands = {}
        docker_cmd = nested_get(doc, docker_cmd_field.split("."))
        if docker_cmd:
            image, _, arguments = docker_cmd.partition(" ")
            set_commands["container"] = {"image": image, "arguments": arguments}

        tasks.update_one(
            {"_id": doc["_id"]},
            {
                "$unset": {docker_cmd_field: 1},
                **({"$set": set_commands} if set_commands else {}),
            },
        )


def _migrate_model_labels(db: Database):
    tasks: Collection = db["task"]

    fields = ("execution.model_labels", "container")
    query = {"$or": [{field: {"$nin": [None, {}]}} for field in fields]}

    for doc in tasks.find(filter=query, projection=fields):
        set_commands = {}
        for field in fields:
            data = nested_get(doc, field.split("."))
            if not data:
                continue
            escaped = escape_dict(data)
            if data == escaped:
                continue
            set_commands[field] = escaped

        if set_commands:
            tasks.update_one({"_id": doc["_id"]}, {"$set": set_commands})


def _migrate_project_names(db: Database):
    projects: Collection = db["project"]

    regx = re.compile("/", re.IGNORECASE)
    for doc in projects.find(filter={"name": regx, "path": {"$in": [None, []]}}):
        name = doc.get("name")
        if not name:
            continue

        max_tries = int(os.getenv("CLEARML_MIGRATION_PROJECT_RENAME_MAX_TRIES", 10))
        iteration = 0
        for iteration in range(max_tries):
            new_name = name.replace("/", "_" * (iteration + 1))
            try:
                projects.update_one({"_id": doc["_id"]}, {"$set": {"name": new_name}})
                break
            except DuplicateKeyError:
                pass

        if iteration >= max_tries - 1:
            print(f"Could not upgrade the name {name} of the project {doc.get('_id')}")


def migrate_backend(db: Database):
    _migrate_task_models(db)
    _migrate_docker_cmd(db)
    _migrate_model_labels(db)
    _migrate_project_names(db)
    _drop_all_indices_from_collections(db, ["task*"])
