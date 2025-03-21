"""
Helper functions for database operations and SQL queries.
This package provides utilities for database connections and SQL query management.
"""

import os
import sys

# Add the parent directory to sys.path to make the package importable
package_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if package_dir not in sys.path:
    sys.path.append(package_dir)

# Import the main components
from helpers.db_connection import DatabaseConnection, create_connection
from helpers.sql_queries import (
    get_table_names,
    get_table_schema,
    get_sample_data,
    execute_query
)
from helpers.schema_visualizer import generate_erd_dot, generate_erd_image

# Define what should be available when using 'from helpers import *'
__all__ = [
    'DatabaseConnection',
    'create_connection',
    'get_table_names',
    'get_table_schema',
    'get_sample_data',
    'execute_query',
    'generate_erd_dot',
    'generate_erd_image'
]


