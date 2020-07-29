import pytest
from mongoset import __version__, connect
from mongoset.database import Database, Table
from tests.test_setup import setup


# def test_index():
# table = setup()

# table.clear()
# table.insert({"x": 1})
# table.index("x")
# table.deindex("x")
# table.insert({"x": 1, "y": 2})


# def test_unique_index():
# table = setup()
# table.clear()
# table.insert({"xa": 1})
