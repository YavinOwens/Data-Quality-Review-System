"""
Unit tests for database utility functions.
"""
import pytest
import logging
from unittest.mock import Mock, patch, MagicMock
import cx_Oracle
from hr_core_validation.utils.db_utils import (
    OracleConnection,
    DatabaseError,
    ConnectionError,
    QueryError,
    SecurityError
)

@pytest.fixture
def mock_db_pool():
    """Mock database connection pool for testing."""
    with patch('cx_Oracle.SessionPool') as mock_pool:
        # Create a mock pool
        mock_pool_instance = Mock()
        mock_pool.return_value = mock_pool_instance
        
        # Create a mock connection
        mock_conn = Mock()
        mock_pool_instance.acquire.return_value = mock_conn
        
        # Create a mock cursor
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        
        yield mock_pool_instance, mock_conn, mock_cursor

@pytest.fixture
def test_config():
    """Test database configuration."""
    return {
        'host': 'test_host',
        'port': 1521,
        'service_name': 'test_service',
        'user': 'test_user',
        'password': 'test_pass'
    }

def test_connection_pool_creation(test_config, mock_db_pool):
    """Test connection pool creation."""
    mock_pool_instance, _, _ = mock_db_pool
    
    conn = OracleConnection(test_config, pool_size=3)
    conn._create_pool()
    
    # Verify pool was created with correct parameters
    cx_Oracle.SessionPool.assert_called_once()
    call_args = cx_Oracle.SessionPool.call_args[1]
    assert call_args['user'] == 'test_user'
    assert call_args['password'] == 'test_pass'
    assert call_args['min'] == 2
    assert call_args['max'] == 3
    assert call_args['increment'] == 1
    assert call_args['timeout'] == 30

def test_connection_pool_error(test_config):
    """Test connection pool creation error handling."""
    with patch('cx_Oracle.SessionPool') as mock_pool:
        mock_pool.side_effect = cx_Oracle.Error("Pool creation failed")
        
        conn = OracleConnection(test_config)
        with pytest.raises(ConnectionError) as exc_info:
            conn._create_pool()
        
        assert "Pool creation failed" in str(exc_info.value)

def test_get_connection_context_manager(test_config, mock_db_pool):
    """Test connection context manager."""
    mock_pool_instance, mock_conn, _ = mock_db_pool
    
    conn = OracleConnection(test_config)
    conn.pool = mock_pool_instance
    
    with conn.get_connection() as connection:
        assert connection == mock_conn
        mock_pool_instance.acquire.assert_called_once()
    
    mock_pool_instance.release.assert_called_once_with(mock_conn)

def test_get_connection_error(test_config, mock_db_pool):
    """Test connection acquisition error handling."""
    mock_pool_instance, _, _ = mock_db_pool
    mock_pool_instance.acquire.side_effect = cx_Oracle.Error("Connection failed")
    
    conn = OracleConnection(test_config)
    conn.pool = mock_pool_instance
    
    with pytest.raises(cx_Oracle.Error) as exc_info:
        with conn.get_connection():
            pass
    
    assert "Connection failed" in str(exc_info.value)

def test_execute_query_with_retry(test_config, mock_db_pool):
    """Test query execution with retry logic."""
    mock_pool_instance, mock_conn, mock_cursor = mock_db_pool
    
    # Setup mock cursor description and results
    mock_cursor.description = [('COLUMN1',), ('COLUMN2',)]
    mock_cursor.fetchall.return_value = [
        ('value1', 'value2'),
        ('value3', 'value4')
    ]
    
    conn = OracleConnection(test_config, retry_attempts=2)
    conn.pool = mock_pool_instance
    
    results = conn.execute_query("SELECT * FROM test_table")
    
    # Verify results
    assert len(results) == 2
    assert results[0] == {'COLUMN1': 'value1', 'COLUMN2': 'value2'}
    assert results[1] == {'COLUMN1': 'value3', 'COLUMN2': 'value4'}
    
    # Verify cursor was used correctly
    mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table")
    mock_cursor.close.assert_called_once()

def test_execute_query_retry_failure(test_config, mock_db_pool):
    """Test query execution retry failure."""
    mock_pool_instance, mock_conn, mock_cursor = mock_db_pool
    mock_cursor.execute.side_effect = cx_Oracle.Error("Query failed")
    
    conn = OracleConnection(test_config, retry_attempts=2)
    conn.pool = mock_pool_instance
    
    with pytest.raises(cx_Oracle.Error) as exc_info:
        conn.execute_query("SELECT * FROM test_table")
    
    assert str(exc_info.value) == "Query failed"
    mock_cursor.close.assert_called()

def test_execute_many_with_batching(test_config, mock_db_pool):
    """Test batch query execution with batching."""
    mock_pool_instance, mock_conn, mock_cursor = mock_db_pool
    
    params_list = [
        {'id': i, 'name': f'name{i}'} for i in range(5)
    ]
    
    conn = OracleConnection(test_config)
    conn.pool = mock_pool_instance
    
    conn.execute_many(
        "INSERT INTO users (id, name) VALUES (:id, :name)",
        params_list,
        batch_size=2
    )
    
    # Verify batches were processed
    assert mock_cursor.executemany.call_count == 3  # 2 batches of 2 + 1 batch of 1
    mock_conn.commit.assert_called()

def test_execute_many_rollback(test_config, mock_db_pool):
    """Test batch query execution rollback on error."""
    mock_pool_instance, mock_conn, mock_cursor = mock_db_pool
    mock_cursor.executemany.side_effect = cx_Oracle.Error("Batch failed")
    
    conn = OracleConnection(test_config)
    conn.pool = mock_pool_instance
    
    with pytest.raises(cx_Oracle.Error):
        conn.execute_many(
            "INSERT INTO users (id, name) VALUES (:id, :name)",
            [{'id': 1, 'name': 'test'}]
        )
    
    mock_conn.rollback.assert_called()
    mock_cursor.close.assert_called()

def test_connection_health_check(test_config, mock_db_pool):
    """Test connection health check."""
    mock_pool_instance, mock_conn, mock_cursor = mock_db_pool
    
    conn = OracleConnection(test_config)
    conn.pool = mock_pool_instance
    
    assert conn.check_connection_health() is True
    mock_cursor.execute.assert_called_once_with("SELECT 1 FROM DUAL")

def test_connection_health_check_failure(test_config, mock_db_pool):
    """Test connection health check failure."""
    mock_pool_instance, mock_conn, mock_cursor = mock_db_pool
    mock_cursor.execute.side_effect = cx_Oracle.Error("Health check failed")
    
    conn = OracleConnection(test_config)
    conn.pool = mock_pool_instance
    
    assert conn.check_connection_health() is False

def test_context_manager(test_config, mock_db_pool):
    """Test context manager functionality."""
    mock_pool_instance, _, _ = mock_db_pool
    
    with OracleConnection(test_config) as conn:
        conn.pool = mock_pool_instance
        # Do some operations here
    
    mock_pool_instance.close.assert_called_once()

def test_logging_setup(test_config):
    """Test logging configuration."""
    conn = OracleConnection(test_config)
    
    # Verify logger was configured
    logger = logging.getLogger('hr_core_validation.utils.db_utils')
    assert logger.level == logging.INFO
    assert len(logger.handlers) == 1

def test_connect_success():
    """Test successful database connection."""
    with patch('cx_Oracle.makedsn') as mock_makedsn, \
         patch('cx_Oracle.connect') as mock_connect:
        
        # Setup mock connection
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        # Create test config
        test_config = {
            'host': 'test_host',
            'port': 1521,
            'service_name': 'test_service',
            'user': 'test_user',
            'password': 'test_pass'
        }
        
        # Create connection
        conn = OracleConnection(test_config)
        conn.connect()
        
        # Verify DSN was created correctly
        mock_makedsn.assert_called_once_with(
            'test_host',
            1521,
            service_name='test_service'
        )
        
        # Verify connection was established
        mock_connect.assert_called_once()
        assert conn.connection is not None

def test_connect_failure():
    """Test database connection failure handling."""
    with patch('cx_Oracle.makedsn'), \
         patch('cx_Oracle.connect') as mock_connect:
        
        # Setup mock connection to raise error
        mock_connect.side_effect = cx_Oracle.Error("Connection failed")
        
        # Create test config
        test_config = {
            'host': 'test_host',
            'port': 1521,
            'service_name': 'test_service',
            'user': 'test_user',
            'password': 'test_pass'
        }
        
        # Attempt connection
        conn = OracleConnection(test_config)
        with pytest.raises(cx_Oracle.Error) as exc_info:
            conn.connect()
        
        assert str(exc_info.value) == "Connection failed"

def test_execute_query_success(mock_db_pool):
    """Test successful query execution."""
    mock_pool_instance, mock_conn, mock_cursor = mock_db_pool
    
    # Setup mock cursor description and results
    mock_cursor.description = [('COLUMN1',), ('COLUMN2',)]
    mock_cursor.fetchall.return_value = [
        ('value1', 'value2'),
        ('value3', 'value4')
    ]
    
    # Create connection and execute query
    conn = OracleConnection()
    conn.pool = mock_pool_instance
    
    results = conn.execute_query("SELECT * FROM test_table")
    
    # Verify results
    assert len(results) == 2
    assert results[0] == {'COLUMN1': 'value1', 'COLUMN2': 'value2'}
    assert results[1] == {'COLUMN1': 'value3', 'COLUMN2': 'value4'}
    
    # Verify cursor was used correctly
    mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table")
    mock_cursor.close.assert_called_once()

def test_execute_query_with_params(mock_db_pool):
    """Test query execution with parameters."""
    mock_pool_instance, mock_conn, mock_cursor = mock_db_pool
    
    # Setup mock cursor description and results
    mock_cursor.description = [('NAME',), ('AGE',)]
    mock_cursor.fetchall.return_value = [('John', 30)]
    
    # Create connection and execute query
    conn = OracleConnection()
    conn.pool = mock_pool_instance
    
    params = {'name': 'John', 'age': 30}
    results = conn.execute_query(
        "SELECT * FROM users WHERE name = :name AND age = :age",
        params
    )
    
    # Verify results
    assert len(results) == 1
    assert results[0] == {'NAME': 'John', 'AGE': 30}
    
    # Verify cursor was used correctly with parameters
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM users WHERE name = :name AND age = :age",
        params
    )

def test_execute_query_failure(mock_db_pool):
    """Test query execution failure handling."""
    mock_pool_instance, mock_conn, mock_cursor = mock_db_pool
    
    # Setup mock cursor to raise an error
    mock_cursor.execute.side_effect = cx_Oracle.Error("Database error")
    
    # Create connection and attempt query
    conn = OracleConnection()
    conn.pool = mock_pool_instance
    
    # Verify that the error is raised
    with pytest.raises(cx_Oracle.Error) as exc_info:
        conn.execute_query("SELECT * FROM test_table")
    
    assert str(exc_info.value) == "Database error"
    
    # Verify cursor was closed even after error
    mock_cursor.close.assert_called_once()

def test_execute_query_no_connection(mock_db_pool):
    """Test query execution when no connection exists."""
    mock_pool_instance, mock_conn, mock_cursor = mock_db_pool
    
    # Setup mock cursor description and results
    mock_cursor.description = [('RESULT',)]
    mock_cursor.fetchall.return_value = [('success',)]
    
    # Create connection without setting connection
    conn = OracleConnection()
    conn.pool = None
    
    # Execute query should connect first
    results = conn.execute_query("SELECT 'success' FROM dual")
    
    # Verify results
    assert len(results) == 1
    assert results[0] == {'RESULT': 'success'}
    
    # Verify connection was established
    assert conn.pool is not None
    mock_cursor.execute.assert_called_once_with("SELECT 'success' FROM dual")

def test_execute_many_success(mock_db_pool):
    """Test successful batch query execution."""
    mock_pool_instance, mock_conn, mock_cursor = mock_db_pool
    
    # Create connection and execute batch query
    conn = OracleConnection()
    conn.pool = mock_pool_instance
    
    params_list = [
        {'id': 1, 'name': 'John'},
        {'id': 2, 'name': 'Jane'}
    ]
    
    conn.execute_many(
        "INSERT INTO users (id, name) VALUES (:id, :name)",
        params_list
    )
    
    # Verify cursor was used correctly
    mock_cursor.executemany.assert_called_once_with(
        "INSERT INTO users (id, name) VALUES (:id, :name)",
        params_list
    )
    mock_cursor.close.assert_called_once()
    mock_conn.commit.assert_called_once()

def test_execute_many_failure(mock_db_pool):
    """Test batch query execution failure handling."""
    mock_pool_instance, mock_conn, mock_cursor = mock_db_pool
    
    # Setup mock cursor to raise an error
    mock_cursor.executemany.side_effect = cx_Oracle.Error("Database error")
    
    # Create connection and attempt batch query
    conn = OracleConnection()
    conn.pool = mock_pool_instance
    
    params_list = [
        {'id': 1, 'name': 'John'},
        {'id': 2, 'name': 'Jane'}
    ]
    
    # Verify that the error is raised
    with pytest.raises(cx_Oracle.Error) as exc_info:
        conn.execute_many(
            "INSERT INTO users (id, name) VALUES (:id, :name)",
            params_list
        )
    
    assert str(exc_info.value) == "Database error"
    
    # Verify cursor was closed and transaction was rolled back
    mock_cursor.close.assert_called_once()
    mock_conn.rollback.assert_called_once()

def test_get_table_metadata(mock_db_pool):
    """Test retrieving table metadata."""
    mock_pool_instance, mock_conn, mock_cursor = mock_db_pool
    
    # Setup mock cursor description and results
    mock_cursor.description = [
        ('COLUMN_NAME',),
        ('DATA_TYPE',),
        ('DATA_LENGTH',),
        ('NULLABLE',),
        ('COLUMN_ID',)
    ]
    mock_cursor.fetchall.return_value = [
        ('ID', 'NUMBER', 22, 'N', 1),
        ('NAME', 'VARCHAR2', 100, 'Y', 2)
    ]
    
    # Create connection and get metadata
    conn = OracleConnection()
    conn.pool = mock_pool_instance
    
    metadata = conn.get_table_metadata('TEST_TABLE')
    
    # Verify results
    assert len(metadata) == 2
    assert metadata['ID']['DATA_TYPE'] == 'NUMBER'
    assert metadata['NAME']['DATA_LENGTH'] == 100
    
    # Verify query was executed correctly
    mock_cursor.execute.assert_called_once()
    assert 'TEST_TABLE' in str(mock_cursor.execute.call_args)

def test_query_validation(test_config, mock_db_pool):
    """Test SQL query validation."""
    mock_pool_instance, _, _ = mock_db_pool
    
    conn = OracleConnection(test_config)
    conn.pool = mock_pool_instance
    
    # Test valid query
    valid_query = "SELECT * FROM users WHERE id = :id"
    conn._validate_query(valid_query)
    
    # Test query with SQL injection
    with pytest.raises(SecurityError) as exc_info:
        conn._validate_query("SELECT * FROM users; DROP TABLE users")
    assert "Multiple SQL statements not allowed" in str(exc_info.value)
    
    # Test query with comments
    with pytest.raises(SecurityError) as exc_info:
        conn._validate_query("SELECT * FROM users -- malicious comment")
    assert "SQL comments not allowed" in str(exc_info.value)
    
    # Test query exceeding size limit
    large_query = "SELECT * FROM " + "x" * (conn.max_query_size + 1)
    with pytest.raises(QueryError) as exc_info:
        conn._validate_query(large_query)
    assert "exceeds maximum size" in str(exc_info.value)

def test_parameter_validation(test_config, mock_db_pool):
    """Test query parameter validation."""
    mock_pool_instance, _, _ = mock_db_pool
    
    conn = OracleConnection(test_config)
    conn.pool = mock_pool_instance
    
    # Test valid parameters
    valid_params = {'id': 1, 'name': 'John'}
    conn._validate_params(valid_params)
    
    # Test parameters with SQL injection
    with pytest.raises(SecurityError) as exc_info:
        conn._validate_params({'id': 1, 'name': "'; DROP TABLE users; --"})
    assert "Potentially unsafe value" in str(exc_info.value)
    
    # Test parameters exceeding size limit
    with pytest.raises(QueryError) as exc_info:
        conn._validate_params({'id': 1, 'name': 'x' * 4001})
    assert "exceeds maximum size" in str(exc_info.value)

def test_pool_stats(test_config, mock_db_pool):
    """Test connection pool statistics."""
    mock_pool_instance, _, _ = mock_db_pool
    
    # Setup mock pool attributes
    mock_pool_instance.opened = 10
    mock_pool_instance.busy = 3
    mock_pool_instance.waiting = 2
    
    conn = OracleConnection(test_config)
    conn.pool = mock_pool_instance
    
    stats = conn.get_pool_stats()
    
    assert stats['total_connections'] == 10
    assert stats['active_connections'] == 3
    assert stats['waiting_connections'] == 2
    assert stats['last_health_check'] is not None

def test_pool_stats_no_pool(test_config):
    """Test connection pool statistics when no pool exists."""
    conn = OracleConnection(test_config)
    
    stats = conn.get_pool_stats()
    
    assert stats['total_connections'] == 0
    assert stats['active_connections'] == 0
    assert stats['waiting_connections'] == 0
    assert stats['last_health_check'] is None

def test_transaction_success(test_config, mock_db_pool):
    """Test successful transaction."""
    mock_pool_instance, mock_conn, _ = mock_db_pool
    
    conn = OracleConnection(test_config)
    conn.pool = mock_pool_instance
    
    with conn.transaction():
        # Do some operations here
        pass
    
    mock_conn.commit.assert_called_once()
    mock_pool_instance.release.assert_called_once_with(mock_conn)

def test_transaction_failure(test_config, mock_db_pool):
    """Test transaction failure and rollback."""
    mock_pool_instance, mock_conn, _ = mock_db_pool
    
    # Setup mock to raise an error
    mock_conn.commit.side_effect = cx_Oracle.Error("Transaction failed")
    
    conn = OracleConnection(test_config)
    conn.pool = mock_pool_instance
    
    with pytest.raises(QueryError) as exc_info:
        with conn.transaction():
            # Do some operations here
            pass
    
    assert "Transaction failed" in str(exc_info.value)
    mock_conn.rollback.assert_called_once()
    mock_pool_instance.release.assert_called_once_with(mock_conn)

def test_execute_query_with_security_validation(test_config, mock_db_pool):
    """Test query execution with security validation."""
    mock_pool_instance, mock_conn, mock_cursor = mock_db_pool

    # Setup mock cursor description and results
    mock_cursor.description = [('ID',), ('NAME',)]
    mock_cursor.fetchall.return_value = [(1, 'John')]

    conn = OracleConnection(test_config)
    conn.pool = mock_pool_instance

    # Test valid query
    valid_query = "SELECT * FROM users WHERE id = :id"
    valid_params = {'id': 1}
    conn.execute_query(valid_query, valid_params)

    # Test query with SQL injection
    with pytest.raises(SecurityError):
        conn.execute_query("SELECT * FROM users; DROP TABLE users")

    # Test query with unsafe parameters
    with pytest.raises(SecurityError):
        conn.execute_query(
            "SELECT * FROM users WHERE name = :name",
            {'name': "'; DROP TABLE users; --"}
        )

def test_execute_many_with_security_validation(test_config, mock_db_pool):
    """Test batch query execution with security validation."""
    mock_pool_instance, mock_conn, mock_cursor = mock_db_pool
    
    conn = OracleConnection(test_config)
    conn.pool = mock_pool_instance
    
    # Test valid batch query
    valid_query = "INSERT INTO users (id, name) VALUES (:id, :name)"
    valid_params = [
        {'id': 1, 'name': 'John'},
        {'id': 2, 'name': 'Jane'}
    ]
    conn.execute_many(valid_query, valid_params)
    
    # Test batch query with SQL injection
    with pytest.raises(SecurityError):
        conn.execute_many(
            "INSERT INTO users (id, name) VALUES (:id, :name); DROP TABLE users",
            valid_params
        )
    
    # Test batch query with unsafe parameters
    unsafe_params = [
        {'id': 1, 'name': "'; DROP TABLE users; --"},
        {'id': 2, 'name': 'Jane'}
    ]
    with pytest.raises(SecurityError):
        conn.execute_many(valid_query, unsafe_params) 