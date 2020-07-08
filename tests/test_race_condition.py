import time

import pytest
from tests.test_setup import setup

row = {}


def test_update_race(start_race):
    table_main = setup()
    table_main.clear()
    assert table_main.insert({"i": 1, "locked": False})

    def test_update():
        table = setup()
        assert table.update({"i": 1, "locked": True}, ["i"])

    start_race(threads_num=1, target=test_update)


def test_update_race_fail(start_race):
    table_main = setup()
    table_main.clear()
    assert table_main.insert({"i": 1, "locked": False})

    def test_update():
        table = setup()
        assert table.update({"i": 1, "locked": True}, ["i"])

    with pytest.raises(Exception) as e:
        start_race(threads_num=2, target=test_update)

    assert "Table.update" in str(e)
    assert "AssertionError" in str(e)
    assert "where 0 =" in str(e)


def test_upsert_race(start_race):
    table_main = setup()
    table_main.clear()
    assert table_main.insert({"i": 1, "locked": False})

    def test_upsert():
        table = setup()
        assert table.upsert({"i": 1, "locked": True}, ["i"])

    start_race(threads_num=1, target=test_upsert)


def test_upsert_race_fail(start_race):
    table_main = setup()
    table_main.clear()
    assert table_main.insert({"i": 1, "locked": False})

    def test_upsert():
        table = setup()
        assert table.upsert({"i": 1, "locked": True}, ["i"])

    with pytest.raises(Exception) as e:
        start_race(threads_num=2, target=test_upsert)

    assert "Table.upsert" in str(e)
    assert "AssertionError" in str(e)
    assert "where 0 =" in str(e)
