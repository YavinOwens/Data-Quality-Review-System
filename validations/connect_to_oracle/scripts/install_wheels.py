#!/usr/bin/env python3
"""
Install wheels offline for Oracle connectivity package.
"""

import os
import sys
import subprocess
from pathlib import Path
from typing import List, Optional


def install_base_wheels(wheel_dir: Path) -> bool:
    """Install base wheels (setuptools, wheel, pip) first."""
    base_wheels = [
        'setuptools-78.0.2-py3-none-any.whl',
        'wheel-0.45.1-py3-none-any.whl',
        'pip-25.0.1-py3-none-any.whl'
    ]
    
    print("\nInstalling base wheels...")
    
    # First install setuptools and wheel
    for wheel in ['setuptools-78.0.2-py3-none-any.whl', 'wheel-0.45.1-py3-none-any.whl']:
        wheel_path = wheel_dir / wheel
        if not wheel_path.exists():
            print(f"Warning: Base wheel {wheel} not found in {wheel_dir}")
            continue
            
        print(f"Installing: {wheel}")
        try:
            result = subprocess.run(
                [sys.executable, '-m', 'pip', 'install', '--no-index', str(wheel_path)],
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                print(f"Error installing base wheel {wheel}:")
                print(f"stdout: {result.stdout}")
                print(f"stderr: {result.stderr}")
                return False
            print(f"Successfully installed {wheel}")
        except Exception as e:
            print(f"Error installing base wheel {wheel}: {str(e)}")
            return False
    
    # Special handling for pip installation
    pip_wheel = wheel_dir / 'pip-25.0.1-py3-none-any.whl'
    if pip_wheel.exists():
        print("\nInstalling pip...")
        try:
            # Use get-pip.py approach for pip installation
            result = subprocess.run(
                [sys.executable, '-m', 'ensurepip', '--upgrade'],
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                print(f"Error upgrading pip:")
                print(f"stdout: {result.stdout}")
                print(f"stderr: {result.stderr}")
                return False
            print("Successfully upgraded pip")
        except Exception as e:
            print(f"Error upgrading pip: {str(e)}")
            return False
    
    return True


def install_dependency_wheels(wheel_dir: Path) -> bool:
    """Install dependency wheels in the correct order."""
    # Define installation order for dependencies
    dependency_order = [
        # Core dependencies
        "zipp",
        "typing_extensions",
        "packaging",
        "markupsafe",
        "importlib_metadata",
        "attrs",
        "blosc2",  # Added blosc2 before tables
        "tables",
        "pyarrow",
        "numpy",
        "pandas",
        "sqlalchemy",
        "oracledb",
        "python_dateutil",  # Changed from python-dateutil
        "pytz",
        "tzdata",
        "openpyxl",
        "xlrd",
        "xlwt",
        "XlsxWriter",  # Keep as is since it matches the wheel filename
        "psutil",
        "py_cpuinfo",  # Changed from py-cpuinfo
        "cryptography",
        "cffi",
        "python_dotenv"
    ]
    
    print("\nInstalling dependency wheels...")
    failed_packages = []
    
    for package in dependency_order:
        try:
            # Find the wheel file for this package (case-insensitive)
            wheel_files = []
            for wheel in wheel_dir.glob('*.whl'):
                # Handle special cases for package names
                wheel_name = wheel.stem
                if package == "XlsxWriter" and wheel_name.startswith("XlsxWriter"):
                    wheel_files.append(wheel)
                elif wheel_name.lower().startswith(package.lower()):
                    wheel_files.append(wheel)
            
            if not wheel_files:
                print(f"Warning: No wheel found for {package}")
                failed_packages.append(f"{package} (wheel not found)")
                continue
                
            wheel_file = wheel_files[0]  # Use the first matching wheel
            print(f"\nInstalling {wheel_file.name}...")
            
            # Install the wheel
            result = subprocess.run([
                sys.executable, "-m", "pip", "install",
                "--no-index",
                "--no-deps",
                str(wheel_file)
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"Successfully installed {wheel_file.name}")
            else:
                print(f"Error installing {wheel_file.name}:")
                print(f"stdout: {result.stdout}")
                print(f"stderr: {result.stderr}")
                failed_packages.append(f"{package} (installation failed)")
                
        except Exception as e:
            print(f"Failed to install {package}: {str(e)}")
            failed_packages.append(f"{package} ({str(e)})")
            continue
    
    if failed_packages:
        print("\nWarning: Some packages failed to install:")
        for package in failed_packages:
            print(f"- {package}")
        print("\nContinuing with installation...")
    
    return True


def install_wheels(wheel_dir: Path) -> bool:
    """Install all wheel files from the specified directory."""
    if not wheel_dir.exists():
        print(f"Error: Wheel directory {wheel_dir} does not exist")
        return False
        
    print(f"\nFound wheel directory: {wheel_dir}")
    print("Contents of wheel directory:")
    for wheel in wheel_dir.glob('*.whl'):
        print(f"- {wheel.name}")
    
    # Install base wheels first
    if not install_base_wheels(wheel_dir):
        print("Failed to install base wheels")
        return False
    
    # Install dependency wheels in order
    if not install_dependency_wheels(wheel_dir):
        print("Failed to install dependency wheels")
        return False
    
    print("\nAll wheels installed successfully!")
    return True


def main():
    """Main function to install wheels."""
    # Get the directory containing this script
    script_dir = Path(__file__).parent
    # Get the parent directory (connect_to_oracle)
    parent_dir = script_dir.parent
    # Get the wheels directory
    wheel_dir = parent_dir / 'wheels'
    
    print(f"Script directory: {script_dir}")
    print(f"Parent directory: {parent_dir}")
    print(f"Wheel directory: {wheel_dir}")
    
    if not wheel_dir.exists():
        print(f"Error: Wheel directory not found at {wheel_dir}")
        print("Please run download_wheels.py first to download the required wheels")
        return False
    
    return install_wheels(wheel_dir)


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)

"""
Example Usage:
-------------
1. On a Windows offline machine:
   C:\> cd path\to\connect_to_oracle
   C:\> python scripts\install_wheels.py

2. On a macOS offline machine:
   $ cd /path/to/connect_to_oracle
   $ python scripts/install_wheels.py

3. On a Linux offline machine:
   $ cd /path/to/connect_to_oracle
   $ python scripts/install_wheels.py

Expected Output:
---------------
Wheel directory: /path/to/connect_to_oracle/wheels
Contents:
  - setuptools-69.0.0-py3-none-any.whl
  - wheel-0.42.0-py3-none-any.whl
  - pip-24.0-py3-none-any.whl
  - cx_Oracle-8.3.0-cp310-cp310-win_amd64.whl
  - oracledb-1.4.2-cp310-cp310-win_amd64.whl
  - SQLAlchemy-2.0.27-cp310-cp310-win_amd64.whl
  ...

Installing base wheels (setuptools and pip)...
Installing: setuptools-69.0.0-py3-none-any.whl
Successfully installed: setuptools-69.0.0-py3-none-any.whl
...

Base wheels installed successfully!

Found 35 wheel files to install...
Installing: cx_Oracle-8.3.0-cp310-cp310-win_amd64.whl
Successfully installed: cx_Oracle-8.3.0-cp310-cp310-win_amd64.whl
...

All wheels installed successfully!

Testing imports:
cx_Oracle version: 8.3.0
oracledb version: 1.4.2
SQLAlchemy version: 2.0.27

Oracle connectivity packages installed successfully!

Next steps:
1. Configure Oracle environment variables
2. Test database connection using test_connection.py
""" 