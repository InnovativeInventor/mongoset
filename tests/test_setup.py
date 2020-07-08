import os

import mongodb_dataset

if not os.environ.get("MONGO_DB_SERVER"):
    MONGO_DB_LOCAL_SERVER = "mongodb://127.0.0.1:27017/"
else:
    MONGO_DB_LOCAL_SERVER = os.environ.get("MONGO_DB_SERVER")


def setup():
    db = mongodb_dataset.connect(MONGO_DB_LOCAL_SERVER, "test_db")
    table = db["test_table"]
    return table
