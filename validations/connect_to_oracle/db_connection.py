"""
Database connection module for Oracle 19c.
Provides a DatabaseConnection class for managing Oracle database connections.
"""

import os
from typing import Optional, Union, Dict, List

import cx_Oracle
import oracledb
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

class DatabaseConnection:
    """Class to manage Oracle database connections."""
    
    def __init__(
        self,
        host: str,
        port: int,
        service_name: str,
        user: str,
        password: str,
        thick_mode: bool = False,
        lib_dir: Optional[str] = None
    ):
        """
        Initialize database connection parameters.
        
        Args:
            host: Database host
            port: Database port
            service_name: Oracle service name
            user: Database username
            password: Database password
            thick_mode: Whether to use thick mode (cx_Oracle) instead of thin mode (oracledb)
            lib_dir: Path to Oracle Client libraries (required for thick mode)
        """
        self.host = host
        self.port = port
        self.service_name = service_name
        self.user = user
        self.password = password
        self.thick_mode = thick_mode
        self.lib_dir = lib_dir
        
        # Initialize connection objects as None
        self._connection = None
        self._engine = None
        
        # Set up connection
        self._setup_connection()
    
    def _setup_connection(self) -> None:
        """Set up database connection based on mode."""
        try:
            if self.thick_mode:
                if self.lib_dir:
                    cx_Oracle.init_oracle_client(lib_dir=self.lib_dir)
                self._connection = cx_Oracle.connect(
                    user=self.user,
                    password=self.password,
                    dsn=f"{self.host}:{self.port}/{self.service_name}"
                )
            else:
                self._connection = oracledb.connect(
                    user=self.user,
                    password=self.password,
                    dsn=f"{self.host}:{self.port}/{self.service_name}"
                )
            
            # Create SQLAlchemy engine
            self._create_engine()
            
        except (cx_Oracle.Error, oracledb.Error) as e:
            raise Exception(f"Failed to connect to database: {str(e)}")
    
    def _create_engine(self) -> None:
        """Create SQLAlchemy engine for database operations."""
        try:
            connection_string = (
                f"oracle+{'cx_oracle' if self.thick_mode else 'oracledb'}://"
                f"{self.user}:{self.password}@{self.host}:{self.port}/"
                f"{self.service_name}"
            )
            self._engine = create_engine(connection_string)
        except Exception as e:
            raise Exception(f"Failed to create SQLAlchemy engine: {str(e)}")
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self._engine.connect() as conn:
                result = conn.execute(text("SELECT 1 FROM DUAL"))
                return result.scalar() == 1
        except SQLAlchemyError as e:
            print(f"Connection test failed: {str(e)}")
            return False
    
    def query_to_df(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute SQL query and return results as pandas DataFrame.
        
        Args:
            query: SQL query string
            params: Optional parameters for parameterized queries
            
        Returns:
            pandas DataFrame containing query results
        """
        try:
            return pd.read_sql(text(query), self._engine, params=params)
        except Exception as e:
            raise Exception(f"Query execution failed: {str(e)}")
    
    def execute_query(
        self, 
        query: str, 
        params: Optional[Dict] = None,
        commit: bool = True
    ) -> None:
        """
        Execute SQL query without returning results.
        
        Args:
            query: SQL query string
            params: Optional parameters for parameterized queries
            commit: Whether to commit the transaction
        """
        try:
            with self._engine.connect() as conn:
                conn.execute(text(query), params)
                if commit:
                    conn.commit()
        except Exception as e:
            raise Exception(f"Query execution failed: {str(e)}")
    
    def bulk_insert(
        self,
        table_name: str,
        data: Union[pd.DataFrame, List[Dict]],
        batch_size: int = 1000,
        commit: bool = True
    ) -> None:
        """
        Perform bulk insert of data into specified table.
        
        Args:
            table_name: Target table name
            data: Data to insert (DataFrame or list of dictionaries)
            batch_size: Number of records per batch
            commit: Whether to commit the transaction
        """
        try:
            if isinstance(data, pd.DataFrame):
                data.to_sql(
                    table_name,
                    self._engine,
                    if_exists='append',
                    index=False,
                    chunksize=batch_size
                )
            else:
                # Convert list of dicts to DataFrame
                df = pd.DataFrame(data)
                df.to_sql(
                    table_name,
                    self._engine,
                    if_exists='append',
                    index=False,
                    chunksize=batch_size
                )
        except Exception as e:
            raise Exception(f"Bulk insert failed: {str(e)}")
    
    def close(self) -> None:
        """Close database connection."""
        try:
            if self._connection:
                self._connection.close()
            if self._engine:
                self._engine.dispose()
        except Exception as e:
            raise Exception(f"Failed to close connection: {str(e)}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

# Example Usage Documentation
"""
Example Usage:
-------------
1. Basic Connection and Query:
   ```python
   from db_connection import DatabaseConnection
   
   # Create connection instance
   db = DatabaseConnection(
       host="localhost",
       port=1521,
       service_name="ORCLCDB",
       user="system",
       password="your_password"
   )
   
   # Execute query and get results as DataFrame
   df = db.query_to_df("SELECT * FROM employees WHERE department_id = :dept_id", 
                      params={"dept_id": 10})
   
   # Close connection
   db.close()
   ```

2. Using Context Manager:
   ```python
   with DatabaseConnection(
       host="localhost",
       port=1521,
       service_name="ORCLCDB",
       user="system",
       password="your_password"
   ) as db:
       # Execute multiple queries
       df1 = db.query_to_df("SELECT * FROM departments")
       db.execute_query("UPDATE employees SET salary = :new_salary WHERE department_id = :dept_id",
                      params={"new_salary": 5000, "dept_id": 20})
   ```

3. Bulk Insert with DataFrame:
   ```python
   import pandas as pd
   
   # Create sample DataFrame
   data = pd.DataFrame({
       'employee_id': [101, 102, 103],
       'first_name': ['John', 'Jane', 'Bob'],
       'last_name': ['Doe', 'Smith', 'Johnson'],
       'department_id': [10, 20, 30]
   })
   
   with DatabaseConnection(
       host="localhost",
       port=1521,
       service_name="ORCLCDB",
       user="system",
       password="your_password"
   ) as db:
       # Bulk insert DataFrame
       db.bulk_insert('employees', data, batch_size=100)
   ```

4. Using Thick Mode with Oracle Client:
   ```python
   # Windows
   db = DatabaseConnection(
       host="localhost",
       port=1521,
       service_name="ORCLCDB",
       user="system",
       password="your_password",
       thick_mode=True,
       lib_dir="C:/oracle/instantclient_19_20"
   )
   
   # macOS/Linux
   db = DatabaseConnection(
       host="localhost",
       port=1521,
       service_name="ORCLCDB",
       user="system",
       password="your_password",
       thick_mode=True,
       lib_dir="/opt/oracle/instantclient_19_20"
   )
   ```

5. Complex Query with Multiple Parameters:
   ```python
   with DatabaseConnection(
       host="localhost",
       port=1521,
       service_name="ORCLCDB",
       user="system",
       password="your_password"
   ) as db:
       # Complex query with multiple parameters
       query = '''
       SELECT e.employee_id,
              e.first_name,
              e.last_name,
              d.department_name
       FROM employees e
       JOIN departments d ON e.department_id = d.department_id
       WHERE e.salary > :min_salary
       AND d.location_id = :location_id
       ORDER BY e.salary DESC
       '''
       
       params = {
           "min_salary": 5000,
           "location_id": 1700
       }
       
       df = db.query_to_df(query, params=params)
   ```

Note: Always use parameterized queries to prevent SQL injection and handle special characters properly.
""" 