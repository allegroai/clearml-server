from pymongo.database import Database, Collection

from apiserver.bll.task.param_utils import (
    hyperparams_legacy_type,
    hyperparams_default_section,
    split_param_name,
)
from apiserver.tools import safe_get


def migrate_backend(db: Database):
    hyperparam_fields = ("execution.parameters", "hyperparams")
    configuration_fields = ("execution.model_desc", "configuration")
    collection: Collection = db["task"]
    for doc in collection.find(projection=hyperparam_fields + configuration_fields):
        set_commands = {}
        for (old_field, new_field), default_section in zip(
            (hyperparam_fields, configuration_fields),
            (hyperparams_default_section, None),
        ):
            legacy = safe_get(doc, old_field, separator=".")
            if not legacy:
                continue
            for full_name, value in legacy.items():
                section, name = split_param_name(full_name, default_section)
                new_path = list(filter(None, (new_field, section, name)))
                # if safe_get(doc, new_path) is not None:
                #    continue
                new_value = dict(
                    name=name, type=hyperparams_legacy_type, value=str(value)
                )
                if section is not None:
                    new_value["section"] = section
                set_commands[".".join(new_path)] = new_value
        if set_commands:
            collection.update_one({"_id": doc["_id"]}, {"$set": set_commands})
