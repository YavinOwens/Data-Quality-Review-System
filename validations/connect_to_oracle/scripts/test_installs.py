#!/usr/bin/env python3
"""
Test script to verify all installed libraries are working correctly.
Uses Jupyter-style code blocks with #%% magic command for interactive testing.
"""

#%%
# Test core Python packages
import sys
print(f"Python version: {sys.version}")

#%%
# Test base packages
import setuptools
import wheel
import pip
print(f"setuptools version: {setuptools.__version__}")
print(f"wheel version: {wheel.__version__}")
print(f"pip version: {pip.__version__}")

#%%
# Test Oracle connectivity
import oracledb
print(f"oracledb version: {oracledb.__version__}")

#%%
# Test SQLAlchemy
import sqlalchemy
from sqlalchemy import create_engine
print(f"SQLAlchemy version: {sqlalchemy.__version__}")

#%%
# Test data manipulation
import pandas as pd
import numpy as np
print(f"pandas version: {pd.__version__}")
print(f"numpy version: {np.__version__}")

# Create a test DataFrame
df = pd.DataFrame({
    'A': np.random.rand(5),
    'B': np.random.rand(5)
})
print("\nTest DataFrame:")
print(df)

#%%
# Test date/time handling
import datetime
from dateutil import parser
import pytz
import tzdata
# print(f"python-dateutil version: {parser.__version__}")
print(f"pytz version: {pytz.__version__}")

# Test date parsing
date_str = "2024-03-25 10:00:00"
parsed_date = parser.parse(date_str)
print(f"\nParsed date: {parsed_date}")

#%%
# Test Excel handling
import openpyxl
import xlrd
import xlwt
import xlsxwriter
print(f"openpyxl version: {openpyxl.__version__}")
print(f"xlrd version: {xlrd.__version__}")
# print(f"xlwt version: {xlwt.__version__}")
print(f"XlsxWriter version: {xlsxwriter.__version__}")

#%%
# Test PyTables
import tables
print(f"PyTables version: {tables.__version__}")

#%%
# Test PyArrow
import pyarrow as pa
print(f"PyArrow version: {pa.__version__}")

#%%
# Test performance monitoring
import psutil
import cpuinfo
print(f"psutil version: {psutil.__version__}")
# print(f"py-cpuinfo version: {cpuinfo.__version__}")

# Get system info
print("\nSystem Information:")
print(f"CPU Usage: {psutil.cpu_percent()}%")
print(f"Memory Usage: {psutil.virtual_memory().percent}%")

#%%
# Test cryptography
import cryptography
import cffi
print(f"cryptography version: {cryptography.__version__}")
print(f"cffi version: {cffi.__version__}")

#%%
# Test environment variables
from dotenv import load_dotenv
import os
# print(f"python-dotenv version: {load_dotenv.__version__}")

# Test loading environment variables
load_dotenv()
print("\nEnvironment Variables:")
print(f"ORACLE_HOST: {os.getenv('ORACLE_HOST', 'Not set')}")
print(f"ORACLE_PORT: {os.getenv('ORACLE_PORT', 'Not set')}")
print(f"ORACLE_SERVICE: {os.getenv('ORACLE_SERVICE', 'Not set')}")

#%%
# Test database connection
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from db_connection import DatabaseConnection

try:
    with DatabaseConnection() as db:
        if db.test_connection():
            print("Database connection successful!")
            # Test a simple query
            result = db.query_to_df("SELECT SYSDATE FROM DUAL")
            print("\nDatabase time:")
            print(result)
        else:
            print("Database connection failed!")
except Exception as e:
    print(f"Database connection error: {str(e)}")

#%%
# Test bulk insert
try:
    with DatabaseConnection() as db:
        # Create test data
        test_data = pd.DataFrame({
            'ID': range(1, 4),
            'NAME': ['Test1', 'Test2', 'Test3']
        })
        # Attempt bulk insert
        db.bulk_insert('TEST_TABLE', test_data)
        print("Bulk insert successful!")
except Exception as e:
    print(f"Bulk insert error: {str(e)}")

#%%
print("\nAll library tests completed!") 