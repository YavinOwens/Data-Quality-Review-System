#!/usr/bin/env python3
"""
Data Engineering Environment Setup Script

This script automates the setup of a data engineering environment by:
1. Creating and managing a Python virtual environment
2. Downloading specified data engineering packages and their dependencies
3. Preparing both wheel files and source distributions for offline installation

Note: For packages that require compilation from source, an initial internet connection
is required during the first installation to download build dependencies and resources.
Subsequent installations in other offline environments will work without internet
if all dependencies are properly downloaded.

Usage:
    python setup_data_engineering_env.py
"""

import os
import subprocess
import sys
import venv
from pathlib import Path

def create_virtual_environment():
    """
    Create a Python virtual environment if it doesn't exist.

    Returns:
        Path: Path to the created or existing virtual environment directory.
    """
    venv_path = Path("venv")
    if not venv_path.exists():
        print("Creating virtual environment...")
        venv.create("venv", with_pip=True)
    return venv_path

def activate_virtual_environment():
    """
    Activate the virtual environment.

    Note:
        This function checks for the appropriate activation script based on the
        operating system (Windows vs Unix-like systems).

    Raises:
        SystemExit: If the virtual environment activation script is not found.
    """
    if sys.platform == "win32":
        activate_script = "venv\\Scripts\\activate"
    else:
        activate_script = "venv/bin/activate"
    
    if not os.path.exists(activate_script):
        print("Virtual environment not found!")
        sys.exit(1)
    
    print("Virtual environment activated successfully!")

def create_wheels_directory():
    """
    Create a directory for storing package wheels and source distributions.

    Returns:
        Path: Path to the created or existing wheels directory.
    """
    wheels_dir = Path("_DE_Wheels")
    wheels_dir.mkdir(exist_ok=True)
    return wheels_dir

def download_packages():
    """
    Download specified data engineering packages and their dependencies.

    Downloads both wheel files and source distributions for the following packages:
    - pip: Package installer for Python (latest version)
    - great-expectations: Data validation and testing framework
    - rapidfuzz: Fast string matching library
    - polars: Fast DataFrame library
    - pandas: Data manipulation and analysis library
    - numpy: Numerical computing library
    - sqlalchemy: SQL toolkit and ORM

    Note:
        Some packages may require compilation from source. In such cases,
        an initial internet connection is required during the first installation
        to download build dependencies and resources.
    """
    packages = [
        "pip",  # Include pip itself
        "great-expectations",
        "rapidfuzz",
        "polars",
        "pandas",
        "numpy",
        "sqlalchemy"
    ]
    
    wheels_dir = create_wheels_directory()
    
    print("\nWARNING: Some packages may require compilation from source.")
    print("An initial internet connection will be needed during the first installation")
    print("to download build dependencies and resources.\n")
    
    # First, download all packages and their dependencies
    print("Downloading all packages and dependencies...")
    subprocess.run([
        "pip", "download",
        "--dest", str(wheels_dir),
        *packages
    ], check=True)
    
    # Then, download specific platform wheels for Python 3.10
    print("\nDownloading Python 3.10 specific wheels...")
    for package in packages:
        print(f"Downloading Python 3.10 wheels for {package}...")
        try:
            subprocess.run([
                "pip", "download",
                "--only-binary=:all:",
                "--python-version", "3.10",
                "--dest", str(wheels_dir),
                package
            ], check=True)
        except subprocess.CalledProcessError:
            print(f"Note: Could not find specific Python 3.10 wheel for {package}, using previously downloaded version")

def main():
    """
    Main function to orchestrate the environment setup process.

    The function:
    1. Creates a virtual environment if it doesn't exist
    2. Activates the virtual environment
    3. Downloads all required packages and their dependencies
    4. Provides instructions for offline installation
    """
    # Create virtual environment if it doesn't exist
    venv_path = create_virtual_environment()
    
    # Activate virtual environment
    activate_virtual_environment()
    
    # Download packages (both wheels and source distributions)
    download_packages()
    
    print("\nSetup completed successfully!")
    print("Packages have been downloaded to the _DE_Wheels directory")
    print("\nTo install these packages in an offline environment:")
    print("1. Copy the _DE_Wheels directory to the target machine")
    print("2. Create a new virtual environment on the target machine")
    print("3. Run: pip install --no-index --find-links=_DE_Wheels -r requirements.txt")
    print("\nNote: First-time installation of packages requiring compilation")
    print("will need an internet connection to download build dependencies.")

if __name__ == "__main__":
    main() 