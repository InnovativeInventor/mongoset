from __future__ import annotations

from typing import List

from mongoset.database import Table
from mongoset.model.document_model_objects import DocumentModel


class _BaseOperations:
    @staticmethod
    def create(table: Table, db_object: DocumentModel) -> str:
        object_id = table.insert(db_object.serialize())
        db_object.force_set("id", object_id)
        return db_object.id

    @staticmethod
    def delete(table: Table, db_object: DocumentModel) -> bool:
        return table.delete(_id=db_object.id) == 1

    @staticmethod
    def delete_matching(table: Table, filter_expr: dict) -> int:
        return table.delete(**filter_expr)

    @staticmethod
    def update(table: Table, data: dict) -> bool:
        if data.get('id') is not None:
            data['_id'] = data['id']
            del data['id']
            return table.update(data)

    @staticmethod
    def get_by_id(table: Table, _id: str) -> dict:
        data = table.find_one(_id=_id)
        data['id'] = data['_id']
        del data['_id']
        return data

    @staticmethod
    def all(table: Table) -> List[dict]:
        data = list(table.all())
        for i in data:
            i['id'] = i['_id']
            del i['_id']
        return data

    @staticmethod
    def filter(table: Table, filter_expr: dict) -> List[dict]:
        data = list(table.find(**filter_expr))
        for i in data:
            i['id'] = i['_id']
            del i['_id']
        return data

    @staticmethod
    def count(table: Table, filter_expr: dict) -> int:
        return table.count(**filter_expr)
