#!/usr/bin/env python3
"""
Script to test Oracle database connection.
Uses the DatabaseConnection class from db_connection.py
"""

import os
import sys
from pathlib import Path

# Add parent directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from db_connection import DatabaseConnection

def test_connection(host, port, service_name, user, password):
    """Test connection to Oracle database."""
    try:
        # Create connection instance
        db = DatabaseConnection(
            host=host,
            port=port,
            service_name=service_name,
            user=user,
            password=password
        )
        
        # Test connection
        if db.test_connection():
            print("Successfully connected to Oracle database!")
            
            # Test a simple query
            result = db.query_to_df("SELECT SYSDATE FROM DUAL")
            print("\nCurrent database time:", result.iloc[0, 0])
            
            # Close connection
            db.close()
            print("\nConnection closed successfully.")
            return True
    except Exception as e:
        print(f"\nError connecting to database: {str(e)}")
        return False

def main():
    # Get connection details from environment variables or use defaults
    host = os.getenv("ORACLE_HOST", "localhost")
    port = int(os.getenv("ORACLE_PORT", "1521"))
    service_name = os.getenv("ORACLE_SERVICE", "ORCLCDB")
    user = os.getenv("ORACLE_USER", "system")
    password = os.getenv("ORACLE_PASSWORD", "Oracle123")
    
    print("\nTesting Oracle connection with following parameters:")
    print(f"Host: {host}")
    print(f"Port: {port}")
    print(f"Service Name: {service_name}")
    print(f"User: {user}")
    
    # Test connection
    success = test_connection(
        host=host,
        port=port,
        service_name=service_name,
        user=user,
        password=password
    )
    
    if not success:
        print("\nPlease check your Oracle configuration and try again.")
        sys.exit(1)

if __name__ == "__main__":
    main()

"""
Example Usage:
-------------
1. Using environment variables (Windows Command Prompt):
   C:\> set ORACLE_HOST=localhost
   C:\> set ORACLE_PORT=1521
   C:\> set ORACLE_SERVICE=ORCLCDB
   C:\> set ORACLE_USER=system
   C:\> set ORACLE_PASSWORD=your_password
   C:\> python scripts\test_connection.py

2. Using environment variables (Windows PowerShell):
   PS> $env:ORACLE_HOST="localhost"
   PS> $env:ORACLE_PORT="1521"
   PS> $env:ORACLE_SERVICE="ORCLCDB"
   PS> $env:ORACLE_USER="system"
   PS> $env:ORACLE_PASSWORD="your_password"
   PS> python scripts\test_connection.py

3. Using environment variables (macOS/Linux):
   $ export ORACLE_HOST="localhost"
   $ export ORACLE_PORT="1521"
   $ export ORACLE_SERVICE="ORCLCDB"
   $ export ORACLE_USER="system"
   $ export ORACLE_PASSWORD="your_password"
   $ python scripts/test_connection.py

Expected Output:
---------------
Testing Oracle connection with following parameters:
Host: localhost
Port: 1521
Service Name: ORCLCDB
User: system

Successfully connected to Oracle database!

Current database time: 2024-03-14 15:30:45

Connection closed successfully.

Note: If connection fails, check:
1. Oracle Client libraries are properly installed
2. Environment variables are set correctly
3. Database is running and accessible
4. Firewall settings allow the connection
5. User credentials are correct
""" 