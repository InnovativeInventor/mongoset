from tests.test_setup import setup


def test_delete():
    table = setup()
    table.clear()
    table.insert({"test": True})

    assert table.find(test=True)
    assert table.delete(test=True)
    assert not table.find(test=True)
