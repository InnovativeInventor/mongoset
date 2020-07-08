import os
import time

import pytest
from mongodb_dataset import __version__, connect
from mongodb_dataset.database import Database, Table
from mongodb_dataset.expression import gt, gte, in_list, lt, lte, not_in_list
from tests.test_setup import setup


def test_upsert():
    table = setup()
    table.clear()
    assert not table.upsert({"i": 1, "j": 1}, ["i"])

    row = table.find_one(i=1, j=1)
    assert row
    assert row["i"] == 1
    assert row["j"] == 1

    assert not table.upsert({"test": 1}, ["test"])


def test_upsert_modify_check():
    table = setup()
    table.clear()
    assert not table.upsert({"i": 1, "j": 1}, ["i"])
    print(table.all())

    row = table.find_one(i=1, j=1)
    assert row
    print("test id", row["_id"])

    row["i"] = 3
    row["j"] = 2

    assert table.upsert(row)
    print(table.all())
    assert len(table) == 1
    assert not table.upsert(row)
    assert table.upsert(row) == 0
    assert len(table) == 1

    print(table.all())

    assert table.find_one(i=3, j=2)
    assert not table.find_one(i=1, j=1)

    row2 = table.find_one(i=3, j=2)
    assert row
    assert row2["i"] == 3
    assert row2["j"] == 2


def test_upsert_non_id():
    table = setup()
    table.clear()
    assert not table.upsert({"i": 1, "j": 1}, key=["i"])

    row = table.find_one(i=1, j=1)
    row["k"] = 4
    del row["_id"]
    assert table.upsert(row, ["i"]) == 1
    assert len(table) == 1

    row2 = table.find_one(i=1, j=1)

    assert row2["i"] == 1
    assert row2["j"] == 1
    print(table.all())
    assert row2["k"] == 4
    assert len(table) == 1
