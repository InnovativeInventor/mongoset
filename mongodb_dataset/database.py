import pymongo
import pymongo.results
from typing import List

from bson import ObjectId


class Database:
    def __init__(self, uri="", db_name="database"):
        self.client = pymongo.MongoClient(uri)
        self.db_name = db_name

    def __getitem__(self, table):
        return Table(self.client, db_name=self.db_name, table_name=table)


class Table:
    def __init__(
        self,
        client: pymongo.MongoClient,
        db_name: str = "database",
        table_name: str = "table",
    ):
        self.client = client
        self.db_name = db_name
        self.table_name = table_name
        self.table = self.client[self.db_name][self.table_name]

    def insert(self, row: dict) -> pymongo.results.InsertOneResult:
        """
        Inserts row
        """
        return self.table.insert_one(row)

    def upsert(self, row: dict, key: List[str] = None) -> bool:
        """
        Upserts row. Returns true if something was updated
        """
        row = self._convert_id_to_obj(row)

        if key is None:
            key = ["_id"]

        f = {a: b for a, b in [(i, row[i]) for i in key]}
        update_response = self.table.update_one(f, {"$set": row}, True)
        if update_response.modified_count:
            return True
        else:
            return False

    def find_one(self, projection=None, **filter_expr) -> dict:
        """
        Returns the first match
        """
        filter_expr = self._convert_id_to_obj(self._eval_filter_expr(filter_expr))
        response = self.table.find_one(filter_expr, projection)
        if response:
            return self._convert_id_to_str(
                dict(self.table.find_one(filter_expr, projection))
            )
        else:
            return {}

    def find(self, projection=None, **filter_expr) -> List[dict]:
        """
        Searches. Does not support comparison operators yet.
        """
        filter_expr = self._convert_id_to_obj(self._eval_filter_expr(filter_expr))
        return [
            self._convert_id_to_str(dict(i))
            for i in self.table.find(filter_expr, projection)
        ]

    def all(self) -> List[dict]:
        """
        Returns everything in the table
        """
        return [dict(self._convert_id_to_str(i)) for i in self.table.find()]

    def delete(self, **filter_expr) -> pymongo.results.DeleteResult:
        """
        Deletes everything that matches
        """
        if not filter_expr:
            raise ValueError(
                "Error! Empty filter expression! Call db.clear() if you want to delete everything"
            )

        return self.table.delete_many(self._eval_filter_expr(filter_expr))

    def clear(self) -> pymongo.results.DeleteResult:
        """
        Clears the entire table
        """
        return self.table.delete_many({})

    def count(self, **filter_expr) -> int:
        """
        Counts the number of items that match the filter expression
        """
        return int(self.table.count_documents(self._eval_filter_expr(filter_expr)))

    __len__ = count

    @staticmethod
    def _eval_filter_expr(filer_expr: dict) -> dict:
        for key, val in filer_expr.items():
            if isinstance(val, Expression):
                val = val.to_dict()
                filer_expr[key] = val

            if isinstance(val, tuple):
                new_val = dict()

                for expr in val:
                    assert isinstance(expr, Expression)
                    new_val.update(expr.to_dict())

                filer_expr[key] = new_val

        return filer_expr

    @staticmethod
    def _convert_id_to_str(data: dict) -> dict:
        if "_id" in data:
            data["_id"] = str(data["_id"])
        return data

    @staticmethod
    def _convert_id_to_obj(data: dict) -> dict:
        if "_id" in data:
            data["_id"] = ObjectId(data["_id"])
        return data


class Expression:
    def __init__(self, key, val):
        self.key: str = key
        self.val: str = val

    def to_dict(self) -> dict:
        return {self.key: self.val}
