from typing import Sequence

from boltons.iterutils import partition
from pymongo.database import Database, Collection


def _drop_all_indices_from_collections(db: Database, names: Sequence[str]):
    """
    Drop all indices for the existing collections from the specified list
    """
    prefixes, names = partition(names, key=lambda x: x.endswith("*"))
    prefixes = {p.rstrip("*") for p in prefixes}
    for collection_name in db.list_collection_names():
        if not (
            collection_name in names
            or any(p for p in prefixes if collection_name.startswith(p))
        ):
            continue
        collection: Collection = db[collection_name]
        collection.drop_indexes()
