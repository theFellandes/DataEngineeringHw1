from typing import Optional
from neo4j import GraphDatabase, basic_auth
from db.database_client import DatabaseClient
from bson import ObjectId


class Neo4jClient(DatabaseClient):
    driver: Optional[object] = None

    def connect(self):
        self.driver = GraphDatabase.driver(self.connection_string, auth=basic_auth("neo4j", "your_password"))

    @staticmethod
    def sanitize_record(record: dict) -> dict:
        """Converts unsupported types (e.g., ObjectId) to strings."""
        new_record = {}
        for key, value in record.items():
            if isinstance(value, ObjectId):
                new_record[key] = str(value)
            else:
                new_record[key] = value
        return new_record

    def insert_data(self, label: str, data: list[dict]):
        if not self.driver:
            self.connect()
        with self.driver.session() as session:
            for record in data:
                clean_record = Neo4jClient.sanitize_record(record)
                cypher_query = f"CREATE (n:{label} $props)"
                session.run(cypher_query, props=clean_record)

    def test_connection(self) -> bool:
        try:
            self.connect()
            with self.driver.session() as session:
                session.run("RETURN 1")
            return True
        except Exception as e:
            print(f"Neo4j connection test failed: {e}")
            return False
