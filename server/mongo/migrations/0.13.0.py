from pymongo.database import Database, Collection


def migrate_auth(db: Database):
    collection: Collection = db["user"]
    collection.drop_index("name_1_company_1")
