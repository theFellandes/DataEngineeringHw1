from typing import Optional
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker

from .database_client import DatabaseClient


class PostgreSQLClient(DatabaseClient):
    engine: Optional[object] = None
    Session: Optional[object] = None

    def connect(self):
        self.engine = create_engine(self.connection_string)
        self.Session = sessionmaker(bind=self.engine)

    def insert_data(self, table_name: str, data: list[dict]):
        if not self.engine:
            self.connect()
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=self.engine)
        # Use engine.begin() to start a transaction that commits upon exit
        with self.engine.begin() as connection:
            connection.execute(table.insert(), data)

    def test_connection(self) -> bool:
        try:
            self.connect()
            with self.engine.connect() as connection:
                connection.execute("SELECT 1")
            return True
        except Exception as e:
            print(f"PostgreSQL connection test failed: {e}")
            return False
