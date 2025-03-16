# data_pipeline.py
import os
import glob
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from kaggle.api.kaggle_api_extended import KaggleApi

from db.postgre_sql_client import PostgreSQLClient
from db.mongo_db_client import MongoDBClient
from db.neo4j_client import Neo4jClient
from db.clickhouse_client import ClickhouseClient
from db.mssql_client import MSSQLClient


def get_neo4j_label(table_name: str) -> str:
    """Convert table name to a singular, capitalized label for Neo4j."""
    if table_name.endswith('s'):
        return table_name[:-1].capitalize()
    return table_name.capitalize()


class DataPipeline:
    def __init__(self, download_dir: str = "data", chunk_size: int = 1000):
        self.download_dir = download_dir
        self.chunk_size = chunk_size

        # Initialize each client with the connection string based on docker-compose settings
        self.pg_client = PostgreSQLClient(connection_string="postgresql://myuser:mypassword@localhost:5432/data-hw1")
        self.mongo_client = MongoDBClient(connection_string="mongodb://root:example@localhost:27017/?authSource=admin")
        self.neo4j_client = Neo4jClient(connection_string="bolt://localhost:7687")
        self.clickhouse_client = ClickhouseClient(connection_string="clickhouse://default:your_password@localhost:9000/default")
        self.mssql_client = MSSQLClient(
            connection_string="mssql+pyodbc://sa:Your_password123@localhost:1433/master?driver=ODBC+Driver+17+for+SQL+Server"
        )

        # Establish connections (or allow lazy connection in each client)
        self.pg_client.connect()
        self.mongo_client.connect()
        self.neo4j_client.connect()
        self.clickhouse_client.connect()
        self.mssql_client.connect()

    def download_dataset(self):
        """Download and unzip the Kaggle dataset if CSV files are not already present."""
        csv_files = glob.glob(os.path.join(self.download_dir, "*.csv"))
        if not csv_files:
            print("Downloading dataset from Kaggle...")
            os.makedirs(self.download_dir, exist_ok=True)
            api = KaggleApi()
            api.authenticate()
            # Downloads and unzips all files from the dataset into the download directory
            api.dataset_download_files("alexanderfrosati/goodbooks-10k-updated", path=self.download_dir, unzip=True)
            print("Download complete.")
        else:
            print("Dataset already downloaded.")

    def process_chunk(self, table_name: str, chunk: pd.DataFrame):
        """Process a single chunk of data by inserting into all databases concurrently."""
        data = chunk.to_dict(orient='records')
        neo4j_label = get_neo4j_label(table_name)
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(self.pg_client.insert_data, table_name, data): "PostgreSQL",
                executor.submit(self.mongo_client.insert_data, table_name, data): "MongoDB",
                executor.submit(self.neo4j_client.insert_data, neo4j_label, data): "Neo4j",
                executor.submit(self.clickhouse_client.insert_data, table_name, data): "ClickHouse",
                executor.submit(self.mssql_client.insert_data, table_name, data): "MSSQL",
            }
            for future in as_completed(futures):
                db_name = futures[future]
                try:
                    future.result()
                    print(f"Inserted chunk into {db_name} for table '{table_name}'")
                except Exception as e:
                    print(f"Error inserting into {db_name} for table '{table_name}': {e}")

    def process_file(self, file_path: str):
        """Stream-read a CSV file in chunks and process each chunk."""
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        print(f"Processing file: {file_path} into table: '{table_name}'")
        with open(file_path, 'r') as f:
            for chunk in pd.read_csv(f, chunksize=self.chunk_size):
                self.process_chunk(table_name, chunk)

    def run(self):
        """Download dataset and process every CSV file in the download directory."""
        self.download_dataset()
        csv_files = glob.glob(os.path.join(self.download_dir, "*.csv"))
        if not csv_files:
            print("No CSV files found in the download directory.")
            return
        for file_path in csv_files:
            self.process_file(file_path)


if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run()
