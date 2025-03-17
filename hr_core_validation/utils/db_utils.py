"""
Database utility functions for Oracle EBS connection and operations
"""

import os
import logging
import time
import re
from typing import Dict, List, Any, Optional, Union, Tuple
from contextlib import contextmanager
import cx_Oracle
from ..config.oracle_config import ORACLE_CONFIG

# Configure logging
logger = logging.getLogger(__name__)

# SQL injection prevention patterns
SQL_KEYWORDS = {
    'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'TRUNCATE', 'ALTER',
    'CREATE', 'GRANT', 'REVOKE', 'COMMIT', 'ROLLBACK', 'MERGE'
}

class DatabaseError(Exception):
    """Base exception for database-related errors"""
    pass

class ConnectionError(DatabaseError):
    """Exception raised for connection-related errors"""
    pass

class QueryError(DatabaseError):
    """Exception raised for query-related errors"""
    pass

class SecurityError(DatabaseError):
    """Exception raised for security-related errors"""
    pass

class OracleConnection:
    def __init__(
        self,
        config: Optional[Dict[str, str]] = None,
        pool_size: int = 5,
        timeout: int = 30,
        retry_attempts: int = 3,
        retry_delay: int = 1,
        max_query_size: int = 1000000  # 1MB
    ):
        """
        Initialize Oracle connection with configuration
        
        Args:
            config: Optional dictionary containing connection parameters
                   If None, uses default config from oracle_config.py
            pool_size: Number of connections in the connection pool
            timeout: Connection timeout in seconds
            retry_attempts: Number of retry attempts for failed operations
            retry_delay: Delay between retry attempts in seconds
            max_query_size: Maximum allowed query size in bytes
        """
        self.config = config or ORACLE_CONFIG
        self.pool_size = pool_size
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.max_query_size = max_query_size
        self.pool = None
        self._setup_oracle_client()
        self._setup_logging()
        self._pool_stats = {
            'total_connections': 0,
            'active_connections': 0,
            'waiting_connections': 0,
            'last_health_check': None
        }
    
    def _setup_oracle_client(self) -> None:
        """Setup Oracle client environment"""
        try:
            oracle_client_path = os.getenv('ORACLE_CLIENT_PATH')
            if oracle_client_path:
                cx_Oracle.init_oracle_client(lib_dir=oracle_client_path)
                logger.info(f"Oracle client initialized from {oracle_client_path}")
        except Exception as e:
            logger.error(f"Failed to initialize Oracle client: {str(e)}")
            raise ConnectionError(f"Oracle client initialization failed: {str(e)}")
    
    def _setup_logging(self) -> None:
        """Setup logging configuration"""
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
    
    def _create_dsn(self) -> str:
        """Create DSN string for Oracle connection"""
        try:
            return cx_Oracle.makedsn(
                self.config['host'],
                self.config['port'],
                service_name=self.config['service_name']
            )
        except Exception as e:
            logger.error(f"Failed to create DSN: {str(e)}")
            raise ConnectionError(f"DSN creation failed: {str(e)}")
    
    def _create_pool(self) -> None:
        """Create connection pool"""
        try:
            dsn = self._create_dsn()
            self.pool = cx_Oracle.SessionPool(
                user=self.config['user'],
                password=self.config['password'],
                dsn=dsn,
                min=2,
                max=self.pool_size,
                increment=1,
                timeout=self.timeout
            )
            logger.info(f"Connection pool created with size {self.pool_size}")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {str(e)}")
            raise ConnectionError(f"Connection pool creation failed: {str(e)}")
    
    @contextmanager
    def get_connection(self) -> cx_Oracle.Connection:
        """
        Context manager for getting a connection from the pool
        
        Yields:
            cx_Oracle.Connection: Database connection
            
        Raises:
            ConnectionError: If connection cannot be established
            cx_Oracle.Error: If database operation fails
        """
        if not self.pool:
            self._create_pool()
            
        connection = None
        try:
            connection = self.pool.acquire()
            yield connection
        except cx_Oracle.Error:
            raise
        except Exception as e:
            logger.error(f"Failed to acquire connection from pool: {str(e)}")
            raise ConnectionError(f"Connection acquisition failed: {str(e)}")
        finally:
            if connection:
                try:
                    self.pool.release(connection)
                except Exception as e:
                    logger.error(f"Failed to release connection: {str(e)}")
    
    def _execute_with_retry(
        self,
        operation: callable,
        *args,
        **kwargs
    ) -> Any:
        """
        Execute an operation with retry logic
        
        Args:
            operation: Function to execute
            *args: Positional arguments for the operation
            **kwargs: Keyword arguments for the operation
            
        Returns:
            Result of the operation
            
        Raises:
            cx_Oracle.Error: If database operation fails
            QueryError: If operation fails after all retry attempts
        """
        last_error = None
        for attempt in range(self.retry_attempts):
            try:
                return operation(*args, **kwargs)
            except cx_Oracle.Error as e:
                # Don't retry on certain errors
                if any(msg in str(e).lower() for msg in [
                    "invalid username/password",
                    "connection failed",
                    "not connected",
                    "invalid object name",
                    "table or view does not exist",
                    "insufficient privileges",
                    "batch failed",
                    "database error",
                    "query failed"
                ]):
                    raise

                last_error = e
                logger.warning(
                    f"Attempt {attempt + 1}/{self.retry_attempts} failed: {str(e)}"
                )
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay)
        
        logger.error(f"Operation failed after {self.retry_attempts} attempts")
        if isinstance(last_error, cx_Oracle.Error):
            raise last_error
        raise QueryError(f"Operation failed: {str(last_error)}")
    
    def _validate_query(self, query: str) -> None:
        """
        Validate query for security and size
        
        Args:
            query: SQL query string
            
        Raises:
            SecurityError: If query is potentially unsafe
            QueryError: If query exceeds size limit
        """
        # Check query size
        if len(query.encode('utf-8')) > self.max_query_size:
            raise QueryError(f"Query exceeds maximum size of {self.max_query_size} bytes")
        
        # Check for multiple statements
        if ';' in query:
            raise SecurityError("Multiple SQL statements not allowed")
        
        # Check for comments
        if '--' in query or '/*' in query:
            raise SecurityError("SQL comments not allowed")
        
        # Check for dangerous patterns
        dangerous_patterns = [
            'EXECUTE IMMEDIATE',
            'DBMS_SQL',
            'UTL_FILE',
            'UTL_HTTP',
            'UTL_SMTP',
            'UTL_TCP',
            'UTL_MAIL',
            'DBMS_JAVA',
            'DBMS_LDAP',
            'CREATE ',
            'DROP ',
            'ALTER ',
            'TRUNCATE ',
            'GRANT ',
            'REVOKE '
        ]
        
        query_upper = query.upper()
        for pattern in dangerous_patterns:
            if pattern in query_upper:
                raise SecurityError(f"Potentially unsafe SQL pattern '{pattern}' found in query")

    def _validate_params(self, params: Optional[Dict[str, Any]]) -> None:
        """
        Validate query parameters
        
        Args:
            params: Dictionary of query parameters
            
        Raises:
            SecurityError: If parameters are potentially unsafe
        """
        if not params:
            return
            
        for key, value in params.items():
            if isinstance(value, str):
                # Check for SQL injection in string parameters
                if any(keyword in value.upper() for keyword in SQL_KEYWORDS):
                    raise SecurityError(f"Potentially unsafe value in parameter '{key}'")
            
            # Check for large objects
            if isinstance(value, (str, bytes)) and len(str(value)) > 4000:
                raise QueryError(f"Parameter '{key}' exceeds maximum size of 4000 characters")

    def get_pool_stats(self) -> Dict[str, Any]:
        """
        Get current connection pool statistics
        
        Returns:
            Dictionary containing pool statistics
        """
        if not self.pool:
            return self._pool_stats
            
        try:
            self._pool_stats.update({
                'total_connections': self.pool.opened,
                'active_connections': self.pool.busy,
                'waiting_connections': self.pool.waiting,
                'last_health_check': time.time()
            })
        except Exception as e:
            logger.warning(f"Failed to get pool statistics: {str(e)}")
            
        return self._pool_stats

    @contextmanager
    def transaction(self) -> None:
        """
        Context manager for database transactions
        
        Yields:
            None
            
        Raises:
            QueryError: If transaction operations fail
        """
        connection = None
        try:
            connection = self.pool.acquire()
            yield
            connection.commit()
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Transaction failed: {str(e)}")
            raise QueryError(f"Transaction failed: {str(e)}")
        finally:
            if connection:
                self.pool.release(connection)

    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results
        
        Args:
            query: SQL query string
            params: Optional dictionary of query parameters
            
        Returns:
            List of dictionaries containing query results
            
        Raises:
            QueryError: If query execution fails
            SecurityError: If query is potentially unsafe
            cx_Oracle.Error: If database operation fails
        """
        # Validate query and parameters
        self._validate_query(query)
        self._validate_params(params)
        
        def _execute():
            with self.get_connection() as connection:
                cursor = None
                try:
                    cursor = connection.cursor()
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    
                    # Get column names
                    columns = [col[0] for col in cursor.description]
                    
                    # Fetch results
                    rows = cursor.fetchall()
                    return [dict(zip(columns, row)) for row in rows]
                finally:
                    if cursor:
                        cursor.close()
        
        try:
            logger.debug(f"Executing query: {query}")
            start_time = time.time()
            results = self._execute_with_retry(_execute)
            execution_time = time.time() - start_time
            logger.info(f"Query executed successfully in {execution_time:.2f} seconds")
            return results
        except cx_Oracle.Error as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise QueryError(f"Query execution failed: {str(e)}")
    
    def execute_many(
        self,
        query: str,
        params_list: List[Dict[str, Any]],
        batch_size: int = 1000
    ) -> None:
        """
        Execute a SQL query multiple times with different parameters
        
        Args:
            query: SQL query string
            params_list: List of parameter dictionaries
            batch_size: Number of records to process in each batch
            
        Raises:
            QueryError: If batch execution fails
            SecurityError: If query is potentially unsafe
            cx_Oracle.Error: If database operation fails
        """
        # Validate query and parameters
        self._validate_query(query)
        for params in params_list:
            self._validate_params(params)
        
        def _execute_batch(batch):
            with self.get_connection() as connection:
                cursor = None
                try:
                    cursor = connection.cursor()
                    cursor.executemany(query, batch)
                    connection.commit()
                except Exception as e:
                    connection.rollback()
                    raise
                finally:
                    if cursor:
                        cursor.close()
        
        try:
            logger.debug(f"Executing batch query: {query}")
            start_time = time.time()
            
            # Process in batches
            for i in range(0, len(params_list), batch_size):
                batch = params_list[i:i + batch_size]
                self._execute_with_retry(_execute_batch, batch)
            
            execution_time = time.time() - start_time
            logger.info(
                f"Batch query executed successfully in {execution_time:.2f} seconds"
            )
        except cx_Oracle.Error as e:
            logger.error(f"Batch query execution failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Batch query execution failed: {str(e)}")
            raise QueryError(f"Batch query execution failed: {str(e)}")
    
    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """
        Get metadata for a specific table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing table metadata
            
        Raises:
            QueryError: If metadata retrieval fails
        """
        query = """
        SELECT column_name, data_type, data_length, nullable, column_id
        FROM all_tab_columns
        WHERE table_name = :table_name
        ORDER BY column_id
        """
        
        try:
            logger.debug(f"Retrieving metadata for table: {table_name}")
            results = self.execute_query(query, {'table_name': table_name})
            return {row['COLUMN_NAME']: row for row in results}
        except Exception as e:
            logger.error(f"Failed to retrieve table metadata: {str(e)}")
            raise QueryError(f"Table metadata retrieval failed: {str(e)}")
    
    def check_connection_health(self) -> bool:
        """
        Check if the database connection is healthy
        
        Returns:
            bool: True if connection is healthy, False otherwise
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                cursor.fetchone()
                cursor.close()
                return True
        except Exception as e:
            logger.error(f"Connection health check failed: {str(e)}")
            return False
    
    def close(self) -> None:
        """Close the connection pool"""
        if self.pool:
            try:
                self.pool.close()
                logger.info("Connection pool closed")
            except Exception as e:
                logger.error(f"Error closing connection pool: {str(e)}")
                raise ConnectionError(f"Failed to close connection pool: {str(e)}")
    
    def __enter__(self) -> 'OracleConnection':
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit"""
        self.close()

    def connect(self) -> None:
        """Establish connection to Oracle database"""
        try:
            dsn = cx_Oracle.makedsn(
                self.config['host'],
                self.config['port'],
                service_name=self.config['service_name']
            )
            
            self.connection = cx_Oracle.connect(
                user=self.config['user'],
                password=self.config['password'],
                dsn=dsn
            )
            print(f"Successfully connected to Oracle database at {self.config['host']}")
            
        except cx_Oracle.Error as error:
            print(f"Error connecting to Oracle database: {error}")
            raise
    
    def disconnect(self) -> None:
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("Database connection closed")
    
    def get_active_workers(self, effective_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get list of active workers as of a specific date
        
        Args:
            effective_date: Optional date string in YYYY-MM-DD format
                          If None, uses current date
                          
        Returns:
            List of dictionaries containing worker information
        """
        if not effective_date:
            effective_date = "TRUNC(SYSDATE)"
        else:
            effective_date = f"TO_DATE('{effective_date}', 'YYYY-MM-DD')"
            
        query = f"""
        SELECT 
            p.PERSON_ID,
            p.EMPLOYEE_NUMBER,
            p.FIRST_NAME,
            p.LAST_NAME,
            p.SEX,
            p.DATE_OF_BIRTH,
            p.NATIONAL_IDENTIFIER,
            p.CURRENT_EMPLOYEE_FLAG,
            a.ASSIGNMENT_ID,
            a.ASSIGNMENT_TYPE,
            a.ASSIGNMENT_STATUS_TYPE_ID,
            a.BUSINESS_GROUP_ID,
            a.ORGANIZATION_ID,
            a.JOB_ID,
            a.POSITION_ID,
            a.GRADE_ID
        FROM PER_ALL_PEOPLE_F p
        JOIN PER_ALL_ASSIGNMENTS_F a
            ON p.PERSON_ID = a.PERSON_ID
        WHERE {effective_date} BETWEEN p.EFFECTIVE_START_DATE AND p.EFFECTIVE_END_DATE
        AND {effective_date} BETWEEN a.EFFECTIVE_START_DATE AND a.EFFECTIVE_END_DATE
        AND p.CURRENT_EMPLOYEE_FLAG = 'Y'
        AND a.ASSIGNMENT_STATUS_TYPE_ID = 1  -- Active assignment status
        """
        
        return self.execute_query(query)
    
    def get_worker_addresses(self, person_ids: List[int], effective_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get addresses for specified workers
        
        Args:
            person_ids: List of PERSON_ID values
            effective_date: Optional date string in YYYY-MM-DD format
                          If None, uses current date
                          
        Returns:
            List of dictionaries containing address information
        """
        if not effective_date:
            effective_date = "TRUNC(SYSDATE)"
        else:
            effective_date = f"TO_DATE('{effective_date}', 'YYYY-MM-DD')"
            
        query = f"""
        SELECT 
            a.ADDRESS_ID,
            a.PERSON_ID,
            a.ADDRESS_TYPE,
            a.ADDRESS_LINE1,
            a.CITY,
            a.POSTAL_CODE,
            a.COUNTRY
        FROM PER_ADDRESSES_F a
        WHERE a.PERSON_ID IN ({','.join(map(str, person_ids))})
        AND {effective_date} BETWEEN a.EFFECTIVE_START_DATE AND a.EFFECTIVE_END_DATE
        """
        
        return self.execute_query(query)
    
    def get_worker_communications(self, person_ids: List[int], effective_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get contact information for specified workers
        
        Args:
            person_ids: List of PERSON_ID values
            effective_date: Optional date string in YYYY-MM-DD format
                          If None, uses current date
                          
        Returns:
            List of dictionaries containing contact information
        """
        if not effective_date:
            effective_date = "TRUNC(SYSDATE)"
        else:
            effective_date = f"TO_DATE('{effective_date}', 'YYYY-MM-DD')"
            
        query = f"""
        SELECT 
            c.CONTACT_ID,
            c.PERSON_ID,
            c.CONTACT_TYPE,
            c.CONTACT_VALUE,
            c.PRIMARY_FLAG
        FROM PER_CONTACTS_F c
        WHERE c.PERSON_ID IN ({','.join(map(str, person_ids))})
        AND {effective_date} BETWEEN c.EFFECTIVE_START_DATE AND c.EFFECTIVE_END_DATE
        """
        
        return self.execute_query(query) 