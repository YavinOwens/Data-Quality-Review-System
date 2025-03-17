# Python_offline

A comprehensive Python environment setup for data engineering workflows, featuring automated package management, data validation, and database connectivity. This repository provides scripts to create isolated Python environments with pre-downloaded wheels for offline installation.

## Key Features

- **Offline Package Management**: Downloads and manages Python wheels for offline installation
- **Data Validation**: Integrates Great Expectations for robust data quality checks
- **High-Performance String Matching**: Includes RapidFuzz for efficient fuzzy string matching
- **Database Connectivity**: Supports Oracle DB connections via cx_Oracle and SQLAlchemy
- **Environment Isolation**: Creates dedicated virtual environments for project isolation

## Security Considerations

- All downloaded wheels are stored in `_DE_Wheels/` directory
- Virtual environments are isolated from system Python
- No sensitive credentials are stored in scripts
- Database connections should use environment variables for credentials
- `.gitignore` is configured to exclude sensitive files and virtual environments

## Quick Start Guide

1. **Initial Setup**:
   ```bash
   # Clone the repository
   git clone https://github.com/yourusername/Python_offline.git
   cd Python_offline
   
   # Run the setup script to download wheels and create virtual environment
   python3 setup_data_engineering_env.py
   ```

2. **Package Installation**:
   - The script will create a virtual environment named `venv`
   - Downloads required wheels to `_DE_Wheels/` directory
   - Installs the following packages:
     - great_expectations==1.3.10
     - rapidfuzz==3.6.1
     - polars==0.20.6
     - sqlalchemy==2.0.27
     - numpy==1.26.4
     - pandas==2.2.1

3. **Verify Installation**:
   ```bash
   # Activate the virtual environment
   source venv/bin/activate  # On Unix/macOS
   .\venv\Scripts\activate   # On Windows
   
   # Run the verification script
   python3 verify_packages.py
   ```

## Script Descriptions

### setup_data_engineering_env.py
- Creates a Python virtual environment
- Downloads specified package wheels and dependencies
- Stores wheels in `_DE_Wheels/` for offline access
- Installs packages in the virtual environment

### verify_packages.py
- Validates successful installation of all packages
- Tests basic functionality of each package
- Includes sample data validation using Great Expectations
- Demonstrates database connectivity and string matching

## Best Practices

1. **Virtual Environment Management**:
   - Always use the provided virtual environment
   - Don't mix with system Python packages
   - Delete and recreate environment if issues arise

2. **Package Management**:
   - Keep wheels in `_DE_Wheels/` for offline installation
   - Update `requirements.txt` when adding new packages
   - Version lock all dependencies

3. **Security**:
   - Store sensitive credentials in environment variables
   - Don't commit `.env` files or credentials
   - Regularly update packages for security patches

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
   - Check `_DE_Wheels/` directory for missing wheels
   - Verify Python version compatibility
   - Ensure all dependencies are available

3. **Database Connection Issues**:
   - Verify environment variables are set
   - Check network connectivity
   - Confirm database credentials

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
