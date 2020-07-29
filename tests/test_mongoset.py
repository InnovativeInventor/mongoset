import os

import pytest
from mongoset import __version__, connect
from mongoset.database import Database, Table
from mongoset.expression import gt, gte, in_list, lt, lte, not_in_list
from tests.test_setup import setup

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
    table = setup()
    assert isinstance(table, Table)


def test_insert():
    table = setup()

    len_before_insert = len(table)

    table.insert({"a": "b"})

    len_after_insert = len(table)

    assert len_before_insert + 1 == len_after_insert


def test_clear():
    table = setup()

    table.insert({"a": "b"})
    table.insert({"a": "b"})
    table.insert({"a": "b"})

    table.clear()

    assert len(table) == 0


def test_delete():
    table = setup()
    table.clear()

    table.insert({"i": 1, "j": 1})
    table.insert({"i": 2, "j": 1})
    table.insert({"i": 1, "j": 2})
    table.insert({"i": 2, "j": 2})

    table.delete(i=1)

    assert len(table) == 2


def test_find():
    table = setup()
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
    table = setup()
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


def test_find_expr():
    table = setup()
    table.clear()

    table.insert({"i": 1, "j": 1})
    table.insert({"i": 2, "j": 2})
    table.insert({"i": 1, "j": 3})
    table.insert({"i": 2, "j": 4})

    rows = table.find(j=gt(2))
    assert len(rows) == 2

    rows = table.find(j=gte(2))
    assert len(rows) == 3

    rows = table.find(i=lt(2))
    assert len(rows) == 2

    rows = table.find(i=lte(2))
    assert len(rows) == 4

    rows = table.find(j=(gt(1), lt(3)))
    assert len(rows) == 1

    rows = table.find(j=(gt(1), lte(3)))
    assert len(rows) == 2

    rows = table.find(j=(gt(1), lte(3)), i=lt(2))
    assert len(rows) == 1

    rows = table.find(j=in_list(2, 4))
    assert len(rows) == 2

    rows = table.find(j=not_in_list(1, 2, 4))
    assert len(rows) == 1


def test_find_none():
    table = setup()
    table.clear()

    rows = table.find(j=gt(2))
    assert not rows

    rows = table.find()
    assert not rows

    rows = table.find_one(j=gt(2))
    assert not rows

    rows = table.find_one()
    assert not rows


def test_raise_value_error():
    with pytest.raises(Exception) as e_info:
        db = connect(MONGO_DB_LOCAL_SERVER, "test_db")
        table = db["test_table"]
        table.delete()
