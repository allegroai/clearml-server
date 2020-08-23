from pymongo.database import Database, Collection


def migrate_backend(db: Database):
    collection: Collection = db["project"]
    featured = "featured"
    query = {featured: {"$exists": False}}
    for doc in collection.find(filter=query, projection=()):
        collection.update_one(
            {"_id": doc["_id"]}, {"$set": {featured: 9999}},
        )
