from typing import Optional
from clickhouse_driver import Client as ClickHouseDriver
from .database_client import DatabaseClient


def transform_record(record):
    # Only process if record is a dict.
    if isinstance(record, dict):
        # Example: Rename key 'book_id' to 'goodreads_book_id' if needed
        if 'book_id' in record:
            record['goodreads_book_id'] = record.pop('book_id')
    else:
        # Log or handle the unexpected type
        print(f"Unexpected record type: {type(record)}")
    return record


class ClickhouseClient(DatabaseClient):
    client: Optional[ClickHouseDriver] = None

    def connect(self):
        self.client = ClickHouseDriver.from_url(self.connection_string)

    def insert_data(self, table_name: str, data: list[dict]):
        if not self.client:
            self.connect()
        if not data:
            return
        # Prepare query: extract columns and create a tuple list of values
        keys = list(data[0].keys())
        columns = ", ".join(keys)
        values = [tuple(d[k] for k in keys) for d in data]
        query = f"INSERT INTO {table_name} ({transform_record(columns)}) VALUES"
        self.client.execute(query, values)

    def test_connection(self) -> bool:
        try:
            self.connect()
            self.client.execute("SELECT 1")
            return True
        except Exception as e:
            print(f"ClickHouse connection test failed: {e}")
            return False
