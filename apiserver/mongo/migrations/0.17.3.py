from collections import Collection

from pymongo.database import Database, Collection


def migrate_auth(db: Database):
    """
    Remove the old indices from the user collections
    to enable building of the updated user email index
    """
    collection: Collection = db["user"]
    collection.drop_indexes()
