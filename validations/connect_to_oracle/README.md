# Oracle 19c Offline Connectivity Package

This package provides functionality for connecting to Oracle 19c databases in an offline environment using Python 3.10.

## System Requirements

- Python 3.10
- Oracle Client libraries (for thick mode)
  - Windows: Oracle Instant Client 19c
  - macOS: Oracle Instant Client 19c
  - Linux: Oracle Instant Client 19c

## Directory Structure

```
connect_to_oracle/
├── wheels/                 # Directory for downloaded wheel files
├── scripts/
│   ├── download_wheels.py  # Script to download required wheels
│   ├── install_wheels.py   # Script to install wheels offline
│   └── test_connection.py  # Script to test database connection
├── requirements/
│   └── requirements.txt    # Package dependencies
├── db_connection.py        # Main database connection module
└── README.md              # This file
```

## Setup Instructions

### 1. On a Machine with Internet Access

1. Ensure Python 3.10 is installed
2. Navigate to the `connect_to_oracle` directory
3. Run the download script:
   ```bash
   # Windows
   python scripts\download_wheels.py
   
   # macOS/Linux
   python scripts/download_wheels.py
   ```
4. Copy the entire `connect_to_oracle` directory to the offline machine

### 2. On the Offline Machine

1. Ensure Python 3.10 is installed
2. Install Oracle Client libraries if using thick mode:
   - Windows:
     1. Download Oracle Instant Client 19c from Oracle's website
     2. Extract to a directory (e.g., `C:\oracle\instantclient_19_20`)
     3. Add the directory to PATH environment variable
   - macOS:
     1. Download Oracle Instant Client 19c from Oracle's website
     2. Extract to a directory (e.g., `/opt/oracle/instantclient_19_20`)
     3. Set `ORACLE_HOME` environment variable
   - Linux:
     1. Download Oracle Instant Client 19c from Oracle's website
     2. Extract to a directory (e.g., `/opt/oracle/instantclient_19_20`)
     3. Set `ORACLE_HOME` environment variable
3. Navigate to the `connect_to_oracle` directory
4. Install the downloaded wheels:
   ```bash
   # Windows
   python scripts\install_wheels.py
   
   # macOS/Linux
   python scripts/install_wheels.py
   ```

## Configuration

Set the following environment variables for database connection:

```bash
# Windows (Command Prompt)
set ORACLE_HOST=your_host
set ORACLE_PORT=1521
set ORACLE_SERVICE=your_service_name
set ORACLE_USER=your_username
set ORACLE_PASSWORD=your_password

# Windows (PowerShell)
$env:ORACLE_HOST="your_host"
$env:ORACLE_PORT="1521"
$env:ORACLE_SERVICE="your_service_name"
$env:ORACLE_USER="your_username"
$env:ORACLE_PASSWORD="your_password"

# macOS/Linux
export ORACLE_HOST="your_host"
export ORACLE_PORT="1521"
export ORACLE_SERVICE="your_service_name"
export ORACLE_USER="your_username"
export ORACLE_PASSWORD="your_password"
```

## Testing the Connection

Run the test script to verify connectivity:

```bash
# Windows
python scripts\test_connection.py

# macOS/Linux
python scripts/test_connection.py
```

## Usage Example

```python
from db_connection import DatabaseConnection

# Create connection instance
db = DatabaseConnection(
    host="localhost",
    port=1521,
    service_name="ORCLCDB",
    user="system",
    password="password",
    thick_mode=True,  # Set to True if using Oracle Client libraries
    lib_dir="C:/oracle/instantclient_19_20"  # Path to Oracle Client libraries
)

# Execute query and get results as DataFrame
df = db.query_to_df("SELECT * FROM your_table")

# Close connection
db.close()
```

## Features

The `DatabaseConnection` class provides:

- Support for both thick (cx_Oracle) and thin (oracledb) client modes
- Connection pooling via SQLAlchemy
- DataFrame integration with pandas
- Bulk data operations
- Context manager support
- Parameterized queries
- Transaction management

## Dependencies

- cx-Oracle==8.3.0
- oracledb==1.4.2
- SQLAlchemy==2.0.27
- pandas==2.2.1
- numpy==1.26.4
- python-dotenv==1.0.1
- typing-extensions==4.9.0
- greenlet==3.0.3

## Error Handling

The package includes comprehensive error handling for:
- Connection failures
- Query execution errors
- Resource cleanup
- Data type mismatches
- Transaction issues

## Best Practices

1. Always use context managers or explicitly close connections
2. Use parameterized queries to prevent SQL injection
3. Handle large datasets in batches
4. Set appropriate timeouts for long-running operations
5. Monitor connection pool usage

## Troubleshooting

Common issues and solutions:

1. **Connection Errors**
   - Verify host, port, and service name
   - Check firewall settings
   - Ensure Oracle client libraries are accessible
   - For Windows: Check PATH environment variable includes Oracle Client directory
   - For macOS/Linux: Check ORACLE_HOME environment variable is set correctly

2. **Authentication Issues**
   - Verify username and password
   - Check account locks and expiration
   - Confirm database privileges

3. **Performance Issues**
   - Monitor connection pool settings
   - Review query optimization
   - Check network latency

4. **Platform-Specific Issues**
   - Windows: Ensure Oracle Client is 64-bit if using 64-bit Python
   - macOS: Check Oracle Client architecture matches Python architecture
   - Linux: Verify Oracle Client libraries are in the correct location

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review Oracle documentation
3. Contact your database administrator 