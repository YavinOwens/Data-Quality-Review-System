# Oracle 19c Offline Connectivity Package

This package provides a comprehensive solution for connecting to Oracle 19c databases in an offline environment, with support for both thick and thin mode connections.

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

## Included Wheels

The `wheels/` directory contains pre-downloaded wheel files for offline installation, including:

### Base Wheels (Required for Environment Setup)
- `setuptools-78.0.2-py3-none-any.whl` - Package development tools
- `pip-25.0.1-py3-none-any.whl` - Package installer
- `wheel-0.45.1-py3-none-any.whl` - Wheel package format support

### Core Dependencies
- `oracledb-1.4.2-cp310-cp310-macosx_10_9_universal2.whl` - Oracle database driver
- `pandas-2.2.1-cp310-cp310-macosx_11_0_arm64.whl` - Data manipulation library
- `numpy-1.26.4-cp310-cp310-macosx_11_0_arm64.whl` - Numerical computing library
- `SQLAlchemy-2.0.27-cp310-cp310-macosx_11_0_arm64.whl` - SQL toolkit and ORM

### Additional Dependencies
- Excel support: `openpyxl`, `xlrd`, `xlwt`, `XlsxWriter`
- Performance optimization: `psutil`, `py-cpuinfo`
- Security: `cryptography`, `cffi`
- Date/Time handling: `python-dateutil`, `pytz`, `tzdata`

## Platform Support

### Current Platform Support
The included wheels are specifically built for:
- Python 3.10
- macOS ARM64 (Apple Silicon)

### Users on Different Platforms
If you're using a different platform, you'll need to download the appropriate wheels:

1. **Windows Users**:
   ```bash
   # On an online Windows machine
   python scripts/download_wheels.py
   # Copy the wheels directory to your offline machine
   ```

2. **Linux Users**:
   ```bash
   # On an online Linux machine
   python scripts/download_wheels.py
   # Copy the wheels directory to your offline machine
   ```

3. **Intel Mac Users**:
   ```bash
   # On an online Intel Mac
   python scripts/download_wheels.py
   # Copy the wheels directory to your offline machine
   ```

## Setup Instructions

1. **Create Virtual Environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Unix/macOS
   # or
   .venv\Scripts\activate  # On Windows
   ```

2. **Install Base Wheels First**:
   ```bash
   pip install wheels/setuptools-78.0.2-py3-none-any.whl
   pip install wheels/wheel-0.45.1-py3-none-any.whl
   pip install wheels/pip-25.0.1-py3-none-any.whl
   ```

3. **Install All Dependencies**:
   ```bash
   python scripts/install_wheels.py
   ```

4. **Test Connection**:
   ```bash
   python scripts/test_connection.py
   ```

## Configuration

1. **Environment Variables**:
   Create a `.env` file in the project root:
   ```
   ORACLE_HOST=your_host
   ORACLE_PORT=1521
   ORACLE_SERVICE=your_service
   ORACLE_USER=your_username
   ORACLE_PASSWORD=your_password
   ORACLE_THICK_MODE=true
   ORACLE_LIB_DIR=/path/to/oracle/instantclient
   ```

2. **Thick Mode Setup**:
   - Download Oracle Instant Client
   - Set `ORACLE_THICK_MODE=true`
   - Configure `ORACLE_LIB_DIR`

## Usage Examples

### Basic Connection
```python
from db_connection import DatabaseConnection

# Using environment variables
with DatabaseConnection() as db:
    result = db.query_to_df("SELECT * FROM your_table")
    print(result)
```

### Custom Connection
```python
from db_connection import DatabaseConnection

# Custom connection parameters
db = DatabaseConnection(
    host="your_host",
    port=1521,
    service_name="your_service",
    user="your_username",
    password="your_password",
    thick_mode=True,
    lib_dir="/path/to/oracle/instantclient"
)

# Test connection
if db.test_connection():
    print("Connection successful!")
    # Execute queries
    result = db.query_to_df("SELECT * FROM your_table")
    print(result)
else:
    print("Connection failed!")

# Close connection
db.close()
```

### Bulk Insert Example
```python
from db_connection import DatabaseConnection
import pandas as pd

# Create sample data
data = {
    'column1': [1, 2, 3],
    'column2': ['a', 'b', 'c']
}
df = pd.DataFrame(data)

# Insert data
with DatabaseConnection() as db:
    db.bulk_insert('your_table', df)
```

## Features

- **Offline Installation**: All dependencies included as wheel files
- **Platform Support**: Compatible with Windows, Linux, and macOS
- **Connection Modes**: Support for both thick and thin mode
- **Data Handling**: Pandas DataFrame support
- **Bulk Operations**: Efficient bulk insert capabilities
- **Error Handling**: Comprehensive error handling and logging
- **Connection Pooling**: Efficient connection management
- **Security**: Secure password handling and connection encryption

## Dependencies

Core dependencies are included as wheel files in the `wheels/` directory:
- oracledb==1.4.2
- pandas==2.2.1
- numpy==1.26.4
- SQLAlchemy==2.0.27
- Additional dependencies for Excel support, performance optimization, and security

## Error Handling

The package includes comprehensive error handling:
- Connection failures
- Query execution errors
- Bulk insert failures
- Environment configuration issues
- Platform-specific errors

## Best Practices

1. **Connection Management**:
   ```python
   # Use context manager for automatic cleanup
   with DatabaseConnection() as db:
       # Your database operations
       pass
   ```

2. **Query Execution**:
   ```python
   # Use parameterized queries
   result = db.query_to_df(
       "SELECT * FROM table WHERE column = :1",
       params=['value']
   )
   ```

3. **Bulk Operations**:
   ```python
   # Use bulk_insert for large datasets
   db.bulk_insert('table_name', dataframe)
   ```

4. **Resource Cleanup**:
   ```python
   # Always close connections
   db.close()
   ```

## Troubleshooting

1. **Connection Issues**:
   - Verify network connectivity
   - Check firewall settings
   - Validate credentials
   - Confirm Oracle service status

2. **Thick Mode Problems**:
   - Verify Instant Client installation
   - Check library path configuration
   - Validate Oracle client version

3. **Performance Issues**:
   - Enable connection pooling
   - Use bulk operations for large datasets
   - Optimize query execution

## Support

For issues and support:
1. Check the troubleshooting guide
2. Review error messages
3. Verify configuration
4. Contact system administrator

## License

This package is proprietary and confidential. 