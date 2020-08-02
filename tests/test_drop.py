import os

import mongoset

if not os.environ.get("MONGO_DB_SERVER"):
    MONGO_DB_LOCAL_SERVER = "mongodb://127.0.0.1:27017/"
else:
    pass
MONGO_DB_LOCAL_SERVER = os.environ.get("MONGO_DB_SERVER")


def test_drop():
    db = mongoset.connect(MONGO_DB_LOCAL_SERVER, "test_db")
    table = db["test_table"]
    assert table.insert({"test": True})
    assert table.find_one(test=True)
    db.drop()

    db = mongoset.connect(MONGO_DB_LOCAL_SERVER, "test_db")
    table = db["test_table"]
    assert not table.find_one(test=True)
