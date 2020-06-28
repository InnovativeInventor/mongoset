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

    def upsert(self, row: dict, key=None) -> pymongo.results.UpdateResult:
        """
        Upserts row
        """
        row = self._convert_id_to_obj(row)

        if key is None:
            key = ["_id"]

        f = {a: b for a, b in [(i, row[i]) for i in key]}
        return self.table.update_one(f, {"$set": row}, True)

    def find_one(self, projection=None, **filter_expr) -> dict:
        """
        Returns the first match
        """
        filter_expr = self._convert_id_to_obj(filter_expr)
        return self._convert_id_to_str(
            dict(self.table.find_one(filter_expr, projection))
        )

    def find(self, projection=None, **filter_expr) -> List[dict]:
        """
        Searches. Does not support comparison operators yet.
        """
        filter_expr = self._convert_id_to_obj(filter_expr)
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
        return self.table.delete_many(filter_expr)

    def clear(self) -> pymongo.results.DeleteResult:
        """
        Clears the entire table
        """
        return self.delete()

    def count(self, **filter_expr) -> int:
        """
        Counts the number of items that match the filter expression
        """
        return int(self.table.count_documents(filter_expr))

    __len__ = count

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
