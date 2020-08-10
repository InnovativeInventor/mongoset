import pytest

from threading import Thread

from tests.model.test_model import TestObject, TestObjectTable

import mongoset
from mongoset.model import DocumentModel
from mongoset.model.immutable_arguments import Immutable
from mongoset.model.model_table import ModelTable
from tests.test_setup import setup


class TestObject(DocumentModel):
    first_name: str = Immutable()
    age: int


class TestObjectTable(ModelTable[TestObject]):
    member_class = TestObject

"""
Race condition tests using pytest-race
"""


def test_lock():
    table = TestObjectTable(setup())
    table.clear()

    test_object = TestObject(first_name="Max", age=10)

    assert table.create(test_object)
    assert table.lock(test_object)
    assert not table.lock(test_object)

    assert table.release(test_object)
    assert not table.release(test_object)

def test_lock_race_success(start_race):
    table = TestObjectTable(setup())
    table.clear()

    test_object = TestObject(first_name="Max", age=10)
    assert table.create(test_object)

    def test_lock():
        assert table.lock(test_object)

    with pytest.raises(Exception) as e:
        start_race(threads_num=1, target=test_update)

def test_lock_race_fail(start_race):
    for i in range(5):
        table = TestObjectTable(setup())
        table.clear()

        test_object = TestObject(first_name="Max", age=10)
        assert table.create(test_object)

        def test_lock():
            assert table.lock(test_object)

        with pytest.raises(Exception) as e:
            start_race(threads_num=2, target=test_update)

