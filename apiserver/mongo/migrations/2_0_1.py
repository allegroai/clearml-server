import json

from pymongo.database import Database, Collection



def migrate_backend(db: Database):
    db.create_collection(name="pipelinestep")