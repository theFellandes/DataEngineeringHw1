import unittest
from unittest.mock import patch, MagicMock

from db.clickhouse_client import ClickHouseClient
from db.neo4j_client import Neo4jClient
from db.postgre_sql_client import PostgreSQLClient


class TestDatabaseClients(unittest.TestCase):

    @patch('sqlalchemy.create_engine')
    def test_postgresql_client(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        client = PostgreSQLClient(connection_string='postgresql://user:password@localhost/dbname')
        client.connect()
        self.assertTrue(client.test_connection())
        mock_engine.connect.assert_called_once()

    @patch('clickhouse_connect.get_client')
    def test_clickhouse_client(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        client = ClickHouseClient(connection_string='localhost')
        client.connect()
        self.assertTrue(client.test_connection())
        mock_client.command.assert_called_once_with("SELECT 1")

    @patch('neo4j.GraphDatabase.driver')
    def test_neo4j_client(self, mock_driver):
        mock_session = MagicMock()
        mock_driver.return_value.session.return_value = mock_session
        client = Neo4jClient(connection_string='bolt://localhost:7687', username='neo4j', password='password')
        client.connect()
        self.assertTrue(client.test_connection())
        mock_session.run.assert_called_once_with("RETURN 1")


if __name__ == '__main__':
    unittest.main()
