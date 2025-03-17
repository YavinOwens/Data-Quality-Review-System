"""
Common test fixtures for HR Core Validation tests.
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime

@pytest.fixture
def sample_worker_data():
    """Fixture providing sample worker data for testing."""
    return pd.DataFrame({
        'WORKER_ID': ['W001', 'W002', 'W003'],
        'FIRST_NAME': ['John', 'Jane', 'Bob'],
        'LAST_NAME': ['Doe', 'Smith', 'Johnson'],
        'HIRE_DATE': [
            datetime(2020, 1, 1),
            datetime(2021, 2, 15),
            datetime(2019, 6, 30)
        ],
        'STATUS': ['ACTIVE', 'ACTIVE', 'TERMINATED']
    })

@pytest.fixture
def sample_assignment_data():
    """Fixture providing sample assignment data for testing."""
    return pd.DataFrame({
        'ASSIGNMENT_ID': ['A001', 'A002', 'A003'],
        'WORKER_ID': ['W001', 'W002', 'W003'],
        'POSITION': ['Engineer', 'Manager', 'Analyst'],
        'DEPARTMENT': ['IT', 'HR', 'Finance'],
        'START_DATE': [
            datetime(2020, 1, 1),
            datetime(2021, 2, 15),
            datetime(2019, 6, 30)
        ],
        'END_DATE': [None, None, datetime(2023, 12, 31)]
    })

@pytest.fixture
def sample_address_data():
    """Fixture providing sample address data for testing."""
    return pd.DataFrame({
        'ADDRESS_ID': ['ADR001', 'ADR002', 'ADR003'],
        'WORKER_ID': ['W001', 'W002', 'W003'],
        'ADDRESS_TYPE': ['HOME', 'WORK', 'HOME'],
        'STREET': ['123 Main St', '456 Office Blvd', '789 Home Ave'],
        'CITY': ['Springfield', 'Metropolis', 'Smallville'],
        'STATE': ['IL', 'NY', 'KS'],
        'POSTAL_CODE': ['62701', '10001', '66002']
    })

@pytest.fixture
def sample_communication_data():
    """Fixture providing sample communication data for testing."""
    return pd.DataFrame({
        'COMMUNICATION_ID': ['C001', 'C002', 'C003'],
        'WORKER_ID': ['W001', 'W002', 'W003'],
        'COMM_TYPE': ['EMAIL', 'PHONE', 'EMAIL'],
        'VALUE': ['john.doe@email.com', '555-0123', 'bob.j@email.com'],
        'IS_PRIMARY': [True, True, False]
    })

@pytest.fixture
def invalid_data_samples():
    """Fixture providing invalid data samples for testing validation rules."""
    return {
        'invalid_worker': pd.DataFrame({
            'WORKER_ID': ['W001', 'W001', None],  # Duplicate and null IDs
            'FIRST_NAME': ['John', None, 'Bob'],  # Null name
            'LAST_NAME': ['Doe', 'Smith', ''],  # Empty string
            'HIRE_DATE': ['2020-01-01', '2021-13-15', '2019-06-30'],  # Invalid date
            'STATUS': ['ACTIVE', 'INVALID', None]  # Invalid status
        }),
        'invalid_dates': pd.DataFrame({
            'START_DATE': ['2020-01-01', '2021-02-15', '2022-01-01'],
            'END_DATE': ['2019-12-31', None, '2021-12-31']  # End date before start date
        })
    }

@pytest.fixture
def db_config():
    """Fixture providing test database configuration."""
    return {
        'host': 'test_host',
        'port': 5432,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    } 