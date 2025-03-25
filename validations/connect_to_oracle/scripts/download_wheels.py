#!/usr/bin/env python3
"""
Script to download wheel files for Oracle 19c connectivity.
This script should be run on a machine with internet access.
"""

import os
import platform
import subprocess
import sys
from pathlib import Path

def get_platform_tag():
    """Get the appropriate platform tag for wheel downloads."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    
    if system == 'darwin':
        if 'arm' in machine:
            return 'macosx_11_0_arm64'
        return 'macosx_10_9_x86_64'
    elif system == 'windows':
        return 'win_amd64' if '64' in machine else 'win32'
    elif system == 'linux':
        return 'manylinux_2_17_x86_64' if '64' in machine else 'manylinux_2_17_i686'
    else:
        raise ValueError(f"Unsupported platform: {system}")

def create_directory(path):
    """Create directory if it doesn't exist."""
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created directory: {path}")

def download_base_wheels(wheel_dir):
    """Download setuptools and pip wheels."""
    try:
        print("\nDownloading base wheels (setuptools and pip)...")
        cmd = [
            sys.executable,
            "-m",
            "pip",
            "download",
            "--only-binary=:all:",
            "--dest", str(wheel_dir),
            "setuptools>=69.0.0",
            "wheel>=0.42.0",
            "pip>=24.0"
        ]
        subprocess.run(cmd, check=True)
        print("Base wheels downloaded successfully!")
    except subprocess.CalledProcessError as e:
        print(f"Error downloading base wheels: {e}")
        sys.exit(1)

def download_wheels():
    """Download all required wheels for offline installation."""
    # Get system information
    system = platform.system().lower()
    machine = platform.machine().lower()
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    
    print(f"System: {system}")
    print(f"Machine: {machine}")
    print(f"Python version: {python_version}")
    
    # Create wheels directory if it doesn't exist
    wheel_dir = Path(__file__).parent.parent / "wheels"
    wheel_dir.mkdir(exist_ok=True)
    
    # Download base wheels first
    download_base_wheels(wheel_dir)
    
    # Determine platform-specific suffix
    if system == "darwin":
        if machine == "arm64":
            platform_suffix = "macosx_11_0_arm64"
        else:
            platform_suffix = "macosx_10_9_x86_64"
    elif system == "windows":
        platform_suffix = "win_amd64"
    else:  # Linux
        platform_suffix = "manylinux_2_17_x86_64.manylinux2014_x86_64"
    
    print(f"\nDownloading wheels for platform: {platform_suffix}")
    
    # List of packages to download
    packages = [
        "oracledb>=2.0.1",
        "pandas>=2.2.0",
        "numpy>=1.26.0",
        "sqlalchemy>=2.0.0",
        "python-dateutil>=2.8.2",
        "pytz>=2024.1",
        "tzdata>=2024.1",
        "openpyxl>=3.1.2",
        "xlrd>=2.0.1",
        "xlwt>=1.3.0",
        "XlsxWriter>=3.1.9",
        "tables>=3.8.0",
        "pyarrow>=15.0.0",
        "psutil>=5.9.8",
        "py-cpuinfo>=9.0.0",
        "cryptography>=42.0.0",
        "cffi>=1.16.0",
        "python_dotenv>=1.0.0",
        "blosc2>=2.3.0",
        "markupsafe>=2.1.5",
        "attrs>=23.2.0",
        "zipp>=3.17.0",
        "typing_extensions>=4.9.0",
        "packaging>=24.0",
        "importlib_metadata>=7.0.1",
        "jupyter>=1.0.0",
        "nbconvert>=7.16.0",
        "jupytext>=1.16.0",
        "notebook>=7.0.0"
    ]
    
    # Download wheels for each package
    for package in packages:
        try:
            print(f"\nDownloading wheel for {package}...")
            subprocess.run([
                sys.executable, "-m", "pip", "download",
                "--only-binary=:all:",
                "--platform", platform_suffix,
                "--python-version", python_version,
                "--dest", str(wheel_dir),
                package
            ], check=True)
            print(f"Successfully downloaded wheel for {package}")
        except subprocess.CalledProcessError as e:
            print(f"Error downloading wheel for {package}:")
            print(f"stdout: {e.stdout.decode() if e.stdout else 'No output'}")
            print(f"stderr: {e.stderr.decode() if e.stderr else 'No error output'}")
            raise

def main():
    # Get script directory
    script_dir = Path(__file__).parent.parent
    
    # Set paths
    requirements_file = script_dir / "requirements" / "requirements.txt"
    wheel_dir = script_dir / "wheels"
    
    # Print system information
    print(f"\nSystem Information:")
    print(f"Operating System: {platform.system()} {platform.release()}")
    print(f"Machine: {platform.machine()}")
    print(f"Python Version: {sys.version.split()[0]}")
    
    # Download wheels
    download_wheels()
    
    print("\nWheel files downloaded successfully!")
    print("\nNext steps:")
    print("1. Copy the entire 'connect_to_oracle' folder to the offline machine")
    print("2. Run install_wheels.py on the offline machine")

if __name__ == "__main__":
    main()

"""
Example Usage:
-------------
1. On a Windows machine with internet access:
   C:\> cd path\to\connect_to_oracle
   C:\> python scripts\download_wheels.py

2. On a macOS machine with internet access:
   $ cd /path/to/connect_to_oracle
   $ python scripts/download_wheels.py

3. On a Linux machine with internet access:
   $ cd /path/to/connect_to_oracle
   $ python scripts/download_wheels.py

Expected Output:
---------------
System Information:
Operating System: macOS 13.0
Machine: arm64
Python Version: 3.10.0

Downloading base wheels (setuptools and pip)...
Base wheels downloaded successfully!

Note: Using oracledb instead of cx-Oracle for Apple Silicon support
Downloading wheels for platform: macosx_11_0_arm64
Successfully downloaded wheels to: /path/to/connect_to_oracle/wheels

Wheel files downloaded successfully!

Next steps:
1. Copy the entire 'connect_to_oracle' folder to the offline machine
2. Run install_wheels.py on the offline machine
""" 