from mongodb_dataset import __version__, connect
from mongodb_dataset.database import Database, Table
import os

if not os.environ.get("MONGO_DB_SERVER"):
    MONGO_DB_LOCAL_SERVER = "mongodb://127.0.0.1:27017/"
else:
    MONGO_DB_LOCAL_SERVER = os.environ.get("MONGO_DB_SERVER")


def test_version():
    assert __version__ == "0.1.0"


def test_connect():
    # Usually fails because mongodb local server isn't running
    db = connect(MONGO_DB_LOCAL_SERVER, "test_db")
    db.client.server_info()
    assert isinstance(db, Database)
    assert db.db_name == "test_db"


def test_table():
    db = connect(MONGO_DB_LOCAL_SERVER, "test_db")
    table = db["test_table"]
    assert isinstance(table, Table)


def test_insert():
    db = connect(MONGO_DB_LOCAL_SERVER, "test_db")
    table = db["test_table"]

    len_before_insert = len(table)

    table.insert({"a": "b"})

    len_after_insert = len(table)

    assert len_before_insert + 1 == len_after_insert


def test_clear():
    db = connect(MONGO_DB_LOCAL_SERVER, "test_db")
    table = db["test_table"]

    table.insert({"a": "b"})
    table.insert({"a": "b"})
    table.insert({"a": "b"})

    table.clear()

    assert len(table) == 0


def test_delete():
    db = connect(MONGO_DB_LOCAL_SERVER, "test_db")
    table = db["test_table"]
    table.clear()

    table.insert({"i": 1, "j": 1})
    table.insert({"i": 2, "j": 1})
    table.insert({"i": 1, "j": 2})
    table.insert({"i": 2, "j": 2})

    table.delete(i=1)

    assert len(table) == 2


def test_upsert():
    db = connect(MONGO_DB_LOCAL_SERVER, "test_db")
    table = db["test_table"]
    table.clear()

    table.insert({"i": 1, "j": 1})

    row = table.find_one(i=1, j=1)
    row["i"] = 3
    row["j"] = 2

    table.upsert(row)

    row2 = table.find_one(i=3, j=2)

    assert row2["i"] == 3
    assert row2["j"] == 2
    assert len(table) == 1


def test_find():
    db = connect(MONGO_DB_LOCAL_SERVER, "test_db")
    table = db["test_table"]
    table.clear()

    table.insert({"i": 1, "j": 1})
    table.insert({"i": 2, "j": 1})
    table.insert({"i": 1, "j": 2})
    table.insert({"i": 2, "j": 2})

    rows = table.find(i=1)
    assert len(rows) == 2
    assert rows[0]["i"] == 1
    assert rows[1]["i"] == 1

    rows = table.find(i=1, j=1)
    assert len(rows) == 1
    assert rows[0]["i"] == 1
    assert rows[0]["j"] == 1


def test_all():
    db = connect(MONGO_DB_LOCAL_SERVER, "test_db")
    table = db["test_table"]
    table.clear()

    table.insert({"j": 1, "k": 1})
    table.insert({"j": 2, "k": 1})
    table.insert({"j": 1, "k": 2})
    table.insert({"j": 2, "k": 2})

    elements = table.all()

    assert len(elements) == 4

    for element in elements:
        assert element["j"]
        assert element["k"]
