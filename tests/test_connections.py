import unittest

from db.postgre_sql_client import PostgreSQLClient
from db.mongo_db_client import MongoDBClient
from db.neo4j_client import Neo4jClient
from db.clickhouse_client import ClickhouseClient
from db.mssql_client import MSSQLClient


class TestDatabaseConnections(unittest.TestCase):
    def setUp(self):
        self.pg_client = PostgreSQLClient(connection_string="postgresql://myuser:mypassword@localhost:5432/data-hw1")
        self.mongo_client = MongoDBClient(connection_string="mongodb://root:example@localhost:27017/?authSource=admin")
        self.neo4j_client = Neo4jClient(connection_string="bolt://localhost:7687")
        self.clickhouse_client = ClickhouseClient(connection_string="clickhouse://default:@localhost:9000/default")
        self.mssql_client = MSSQLClient(connection_string="mssql+pyodbc://sa:Your_password123@localhost:1433/?driver=ODBC+Driver+17+for+SQL+Server")

    def test_postgresql_connection(self):
        self.assertTrue(self.pg_client.test_connection())

    def test_mongo_connection(self):
        self.assertTrue(self.mongo_client.test_connection())

    def test_neo4j_connection(self):
        self.assertTrue(self.neo4j_client.test_connection())

    def test_clickhouse_connection(self):
        self.assertTrue(self.clickhouse_client.test_connection())

    def test_mssql_connection(self):
        self.assertTrue(self.mssql_client.test_connection())


if __name__ == '__main__':
    unittest.main()
