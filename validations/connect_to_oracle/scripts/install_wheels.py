#!/usr/bin/env python3
"""
Script to install wheel files for Oracle 19c connectivity.
This script should be run on the offline machine.
"""

import os
import subprocess
import sys
from pathlib import Path

def install_base_wheels(wheel_dir):
    """Install setuptools and pip wheels first."""
    try:
        print("\nInstalling base wheels (setuptools and pip)...")
        base_wheels = [
            "setuptools",
            "wheel",
            "pip"
        ]
        
        for wheel_name in base_wheels:
            wheels = list(Path(wheel_dir).glob(f"{wheel_name}*.whl"))
            if wheels:
                wheel = wheels[0]  # Get the first matching wheel
                print(f"\nInstalling: {wheel.name}")
                cmd = [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "--no-index",
                    "--find-links", str(wheel_dir),
                    wheel.name
                ]
                subprocess.run(cmd, check=True, capture_output=True, text=True)
                print(f"Successfully installed: {wheel.name}")
            else:
                print(f"Warning: No wheel found for {wheel_name}")
        
        print("Base wheels installed successfully!")
        
    except subprocess.CalledProcessError as e:
        print(f"Error installing base wheels: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        sys.exit(1)

def install_wheels(wheel_dir):
    """Install all wheel files from the specified directory."""
    try:
        # Get list of wheel files
        wheels = list(Path(wheel_dir).glob("*.whl"))
        if not wheels:
            print(f"No wheel files found in: {wheel_dir}")
            sys.exit(1)
        
        print(f"\nFound {len(wheels)} wheel files to install...")
        
        # Install each wheel file
        for wheel in wheels:
            # Skip base wheels as they're installed first
            if any(wheel.name.startswith(base) for base in ["setuptools", "wheel", "pip"]):
                continue
                
            print(f"\nInstalling: {wheel.name}")
            cmd = [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--no-index",
                "--find-links", str(wheel_dir),
                wheel.name
            ]
            
            try:
                subprocess.run(cmd, check=True, capture_output=True, text=True)
                print(f"Successfully installed: {wheel.name}")
            except subprocess.CalledProcessError as e:
                print(f"Error installing {wheel.name}:")
                print(f"stdout: {e.stdout}")
                print(f"stderr: {e.stderr}")
                raise
            
        print("\nAll wheels installed successfully!")
        
    except subprocess.CalledProcessError as e:
        print(f"Error installing wheels: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

def test_oracle_connection():
    """Test Oracle connectivity after installation."""
    try:
        import cx_Oracle
        import oracledb
        import sqlalchemy
        print("\nTesting imports:")
        print(f"cx_Oracle version: {cx_Oracle.version}")
        print(f"oracledb version: {oracledb.__version__}")
        print(f"SQLAlchemy version: {sqlalchemy.__version__}")
        print("\nOracle connectivity packages installed successfully!")
    except ImportError as e:
        print(f"\nError importing Oracle packages: {e}")
        sys.exit(1)

def main():
    # Get script directory
    script_dir = Path(__file__).parent.parent
    wheel_dir = script_dir / "wheels"
    
    # Verify wheel directory exists
    if not wheel_dir.exists():
        print(f"Error: Wheel directory not found: {wheel_dir}")
        sys.exit(1)
    
    # Print wheel directory contents
    print(f"\nWheel directory: {wheel_dir}")
    print("Contents:")
    for wheel in wheel_dir.glob("*.whl"):
        print(f"  - {wheel.name}")
    
    # Install base wheels first
    install_base_wheels(wheel_dir)
    
    # Install remaining wheels
    install_wheels(wheel_dir)
    
    # Test Oracle connectivity
    test_oracle_connection()
    
    print("\nNext steps:")
    print("1. Configure Oracle environment variables")
    print("2. Test database connection using test_connection.py")

if __name__ == "__main__":
    main()

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