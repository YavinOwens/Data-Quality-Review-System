# Python Data Engineering and HR Core Validation

A comprehensive Python environment for data engineering workflows and HR Core data validation, featuring automated package management, data validation, database connectivity, and comprehensive data quality checks.

## Key Features

### Environment and Package Management
- **Offline Package Management**: Downloads and manages Python wheels for offline installation
- **Environment Isolation**: Creates dedicated virtual environments for project isolation
- **Automated Setup**: Scripts for environment creation and package installation

### Data Processing and Validation
- **Data Validation**: Multiple validation frameworks:
  - Great Expectations for robust data quality checks
  - Pandera schemas for structured data validation
- **High-Performance Processing**:
  - Polars for high-performance data operations
  - RapidFuzz for efficient fuzzy string matching
- **Data Profiling**: Comprehensive profiling with ydata-profiling
- **Visualization**: Interactive visualization with Plotly

### Database and Connectivity
- **Database Support**: 
  - Oracle DB connections via cx_Oracle
  - PostgreSQL support via psycopg2
  - SQLAlchemy for ORM functionality
- **Security**: Environment variable based credential management

## Project Structure

```
Python_offline/
├── _DE_Wheels/              # Downloaded Python wheels for offline installation
├── hr_core_validation/      # HR Core validation framework
│   ├── data_sources/        # Source CSV files
│   ├── documentation/       # Generated reports and diagrams
│   ├── schemas/            # Data validation schemas
│   ├── utils/             # Utility functions
│   └── tests/             # Test suites
├── scripts/                # Setup and verification scripts
└── venv/                  # Virtual environment
```

## Security Considerations

- All downloaded wheels are stored in `_DE_Wheels/` directory
- Virtual environments are isolated from system Python
- No sensitive credentials are stored in scripts
- Database connections use environment variables for credentials
- `.gitignore` configured to exclude sensitive files

## Quick Start Guide

1. **Initial Setup**:
   ```bash
   # Clone the repository
   git clone https://github.com/yourusername/Python_offline.git
   cd Python_offline
   
   # Run the setup script
   python3 setup_data_engineering_env.py
   ```

2. **Environment Activation**:
   ```bash
   source venv/bin/activate  # On Unix/macOS
   .\venv\Scripts\activate   # On Windows
   ```

3. **Package Installation**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Verify Installation**:
   ```bash
   python3 verify_packages.py
   ```

## HR Core Data Validation

### Validation Rules

#### Workers
- Unique ID validation
- Required fields: First Name, Last Name
- Date format validation
- Nationality code length check

#### Addresses
- Unique ID validation
- Required fields: Worker ID, Address Type
- Address Type enumeration
- Postal code format validation

#### Communications
- Unique ID validation
- Required fields: Worker ID, Contact Type, Contact Value
- Contact Type enumeration
- Date format validation

### Output
- Validation summary reports (HTML)
- Data profile reports
- Entity-Relationship diagrams
- Success rate visualizations
- Table-level quality analysis

## Best Practices

1. **Virtual Environment Management**:
   - Use provided virtual environment
   - Don't mix with system Python
   - Recreate environment if issues arise

2. **Package Management**:
   - Maintain offline wheels in `_DE_Wheels/`
   - Keep `requirements.txt` updated
   - Version lock dependencies

3. **Security**:
   - Use environment variables for credentials
   - Don't commit sensitive data
   - Regular security updates

## Troubleshooting

1. **Virtual Environment Issues**:
   ```bash
   # Remove existing environment
   rm -rf venv/  # Unix/macOS
   rmdir /s /q venv  # Windows
   
   # Recreate environment
   python3 setup_data_engineering_env.py
   ```

2. **Package Installation Failures**:
   - Check `_DE_Wheels/` directory
   - Verify Python version compatibility
   - Check dependency availability

3. **Database Connection Issues**:
   - Verify environment variables
   - Check network connectivity
   - Validate credentials

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
