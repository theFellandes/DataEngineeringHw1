from abc import ABC, abstractmethod
from pydantic import BaseModel


class DatabaseClient(BaseModel, ABC):
    connection_string: str

    class Config:
        arbitrary_types_allowed = True

    @abstractmethod
    def connect(self):
        """Establish a connection to the database."""
        raise NotImplementedError

    @abstractmethod
    def insert_data(self, data: dict):
        """Insert data into the database."""
        raise NotImplementedError

    @abstractmethod
    def test_connection(self) -> bool:
        """Test the database connection."""
        raise NotImplementedError
