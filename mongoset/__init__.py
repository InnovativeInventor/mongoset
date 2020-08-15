from mongoset.database import Database

__version__ = "0.1.0"


def connect(
    uri: str = "mongodb://127.0.0.1:27017/", db_name: str = "database"
) -> Database:
    return Database(uri, db_name)
