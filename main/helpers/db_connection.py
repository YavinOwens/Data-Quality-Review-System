"""
Database connection helper for Oracle 19c.
Provides easy-to-use functions for connecting to and querying the database.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional, Dict, Any
import oracledb
from contextlib import contextmanager
import sys

# Add the project root to Python path if not already there
def add_project_root_to_path():
    """Add the project root directory to Python path."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    if project_root not in sys.path:
        sys.path.append(project_root)

# Ensure project root is in path
add_project_root_to_path()

class DatabaseConnection:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 1521,
        service_name: str = "ORCLCDB",
        user: str = "system",
        password: str = "Oracle123"
    ):
        """
        Initialize database connection parameters.
        
        Args:
            host (str): Database host
            port (int): Database port (default: 1521 for Oracle)
            service_name (str): Oracle service name
            user (str): Database user
            password (str): Database password
        """
        self.host = host
        self.port = port
        self.service_name = service_name
        self.user = user
        self.password = password
        self._engine = None
        self._connection = None

    @property
    def connection_string(self) -> str:
        """Get the SQLAlchemy connection string."""
        return f"oracle+oracledb://{self.user}:{self.password}@{self.host}:{self.port}/?service_name={self.service_name}"

    @property
    def engine(self):
        """Get or create SQLAlchemy engine."""
        if self._engine is None:
            self._engine = create_engine(self.connection_string)
        return self._engine

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        dsn = f"{self.host}:{self.port}/{self.service_name}"
        conn = oracledb.connect(
            user=self.user,
            password=self.password,
            dsn=dsn
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
                    cur.execute("SELECT 1 FROM DUAL")
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

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> None:
        """
        Execute a SQL query without returning results.
        
        Args:
            query (str): SQL query to execute
            params (dict, optional): Query parameters
        """
        try:
            with self.engine.connect() as connection:
                connection.execute(text(query), params or {})
                connection.commit()
        except Exception as e:
            print(f"Query execution failed: {str(e)}")
            raise

    def close(self) -> None:
        """Close the database connection."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
        if self._connection:
            self._connection.close()
            self._connection = None

    def connect(self) -> bool:
        """Establish connection to the database."""
        try:
            dsn = f"{self.host}:{self.port}/{self.service_name}"
            self._connection = oracledb.connect(
                user=self.user,
                password=self.password,
                dsn=dsn
            )
            return True
        except Exception as e:
            print(f"Error connecting to database: {str(e)}")
            return False

    def disconnect(self):
        """Close the database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def insert_dataframe(self, df: pd.DataFrame, table_name: str, batch_size: int = 1000):
        """Insert a pandas DataFrame into an Oracle table."""
        try:
            if not self.connect():
                raise Exception("Failed to connect to database")
            
            cursor = self._connection.cursor()
            
            # Prepare the insert statement
            columns = ', '.join(df.columns)
            placeholders = ', '.join([':' + str(i+1) for i in range(len(df.columns))])
            insert_stmt = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # Insert data in batches
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                data = [tuple(x) for x in batch.values]
                cursor.executemany(insert_stmt, data)
                self._connection.commit()
            
            cursor.close()
            
        except Exception as e:
            print(f"Error inserting data: {str(e)}")
            if self._connection:
                self._connection.rollback()
        finally:
            self.disconnect()

    def execute_script(self, script_path: str) -> None:
        """
        Execute a SQL script file.
        
        Args:
            script_path (str): Path to the SQL script file
        """
        try:
            with open(script_path, 'r') as f:
                script = f.read()
            
            # Split script into individual statements, preserving PL/SQL blocks
            current_statement = []
            statements = []
            
            for line in script.split('\n'):
                line = line.strip()
                if not line or line.startswith('--'):
                    continue
                
                current_statement.append(line)
                
                # Check if this is the end of a statement
                if line.endswith(';'):
                    statements.append('\n'.join(current_statement))
                    current_statement = []
                elif line == '/':
                    # End of PL/SQL block
                    statements.append('\n'.join(current_statement[:-1]))  # Exclude the '/'
                    current_statement = []
            
            # Add any remaining statement
            if current_statement:
                statements.append('\n'.join(current_statement))
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                for statement in statements:
                    statement = statement.strip()
                    if statement:
                        try:
                            cursor.execute(statement)
                            print(f"Successfully executed: {statement[:100]}...")
                        except Exception as e:
                            print(f"Error executing statement: {statement}")
                            print(f"Error: {str(e)}")
                            raise
                conn.commit()
                print("Script executed successfully")
        except Exception as e:
            print(f"Error executing script: {str(e)}")
            raise

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