import os

import pytest
from mongodb_dataset import __version__, connect
from mongodb_dataset.database import Database, Table
from mongodb_dataset.expression import gt, gte, in_list, lt, lte, not_in_list

if not os.environ.get("MONGO_DB_SERVER"):
    MONGO_DB_LOCAL_SERVER = "mongodb://127.0.0.1:27017/"
else:
    MONGO_DB_LOCAL_SERVER = os.environ.get("MONGO_DB_SERVER")


def setup():
    db = connect(MONGO_DB_LOCAL_SERVER, "test_db")
    table = db["test_table"]
    table.clear()
    return table


def test_upsert():
    table = setup()
    assert table.upsert({"i": 1, "j": 1})

    row = table.find_one(i=1, j=1)
    assert row
    assert row["i"] == 1
    assert row["j"] == 1


def test_upsert_modify_check():
    table = setup()
    assert table.upsert({"i": 1, "j": 1})

    row = table.find_one(i=1, j=1)
    assert row

    row["i"] = 3
    row["j"] = 2

    assert table.upsert(row)
    assert not table.upsert(row)
    assert table.upsert(row) == 0
    assert len(table) == 2

    print(table.all())

    assert table.find_one(i=3, j=2)
    assert not table.find_one(i=1, j=1)

    row2 = table.find_one(i=3, j=2)
    assert row
    assert row2["i"] == 3
    assert row2["j"] == 2


def test_upsert_non_id():
    table = setup()
    assert table.upsert({"i": 1, "j": 1})

    row = table.find_one(i=1, j=1)
    row["k"] = 4
    del row["_id"]
    assert table.upsert(row, ["i"]) == 1
    assert len(table) == 2

    row2 = table.find_one(i=1, j=1)

    assert row2["i"] == 1
    assert row2["j"] == 1
    assert row2["k"] == 4
    assert len(table) == 2
