"""
Helper functions for database operations and SQL queries.
This package provides utilities for connecting to and querying PostgreSQL databases.
"""

import os
import sys

# Get the absolute path of the main directory (parent of helpers)
main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the main directory to Python path if it's not already there
if main_dir not in sys.path:
    sys.path.append(main_dir)

