import os

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

def setup_model():
    table = TestObjectTable(setup())
    table.clear()
    return table


def test_model():
    table = TestObjectTable(setup())
    table.clear()

    obj0 = TestObject(first_name="ethan", age=15)
    table.create(obj0)

    try:
        obj0.first_name = "test"
    except ValueError:
        pass
    else:
        raise ValueError("Immutable field was set")

    obj0.age = 10
    table.update(obj0)

    obj1 = table.get_by_id(obj0.id)

    assert obj0 == obj1
