"""
Database connection helper for PostgreSQL running in Docker.
Provides easy-to-use functions for connecting to and querying the database.
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from typing import Optional, Dict, Any
import psycopg2
from contextlib import contextmanager

class DatabaseConnection:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "postgres",
        user: str = "postgres",
        password: str = "mysecretpassword"
    ):
        """
        Initialize database connection parameters.
        
        Args:
            host (str): Database host
            port (int): Database port
            database (str): Database name
            user (str): Database user
            password (str): Database password
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self._engine = None

    @property
    def connection_string(self) -> str:
        """Get the SQLAlchemy connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def engine(self):
        """Get or create SQLAlchemy engine."""
        if self._engine is None:
            self._engine = create_engine(self.connection_string)
        return self._engine

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        try:
            yield conn
        finally:
            conn.close()

    def test_connection(self) -> bool:
        """
        Test the database connection.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    return True
        except Exception as e:
            print(f"Connection failed: {str(e)}")
            return False

    def query_to_df(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Execute a query and return results as a pandas DataFrame.
        
        Args:
            query (str): SQL query to execute
            params (dict, optional): Query parameters
            
        Returns:
            pd.DataFrame: Query results as a DataFrame
        """
        try:
            return pd.read_sql_query(query, self.engine, params=params)
        except Exception as e:
            print(f"Query failed: {str(e)}")
            return pd.DataFrame()

# Create a default connection instance
default_connection = DatabaseConnection()

def get_connection() -> DatabaseConnection:
    """Get the default database connection instance."""
    return default_connection

def query_to_df(query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Convenience function to execute a query using the default connection.
    
    Args:
        query (str): SQL query to execute
        params (dict, optional): Query parameters
        
    Returns:
        pd.DataFrame: Query results as a DataFrame
    """
    return default_connection.query_to_df(query, params) 