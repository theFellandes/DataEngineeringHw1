from typing import Optional
from pymongo import MongoClient
from .database_client import DatabaseClient


class MongoDBClient(DatabaseClient):
    client: Optional[MongoClient] = None
    db: Optional[object] = None

    class Config:
        arbitrary_types_allowed = True

    def connect(self):
        self.client = MongoClient(self.connection_string)
        self.db = self.client.get_database("data-hw1")

    def insert_data(self, collection_name: str, data: list[dict]):
        if not self.client:
            self.connect()
        collection = self.db[collection_name]
        collection.insert_many(data)

    def test_connection(self) -> bool:
        try:
            self.connect()
            self.client.admin.command('ping')
            return True
        except Exception as e:
            print(f"MongoDB connection test failed: {e}")
            return False
