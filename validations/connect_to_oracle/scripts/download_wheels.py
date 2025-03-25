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

def download_wheels(requirements_file, wheel_dir):
    """Download wheel files based on requirements.txt."""
    try:
        # Create wheel directory if it doesn't exist
        create_directory(wheel_dir)
        
        # Download base wheels first
        download_base_wheels(wheel_dir)
        
        # Get platform-specific tag
        platform_tag = get_platform_tag()
        
        # Check if we're on Apple Silicon
        is_apple_silicon = platform.system().lower() == 'darwin' and 'arm' in platform.machine().lower()
        
        # Download wheels
        cmd = [
            sys.executable, 
            "-m", 
            "pip", 
            "download",
            "--only-binary=:all:",
            "--platform", platform_tag,
            "--python-version", "310",  # Python 3.10
            "--implementation", "cp",  # CPython
            "--dest", str(wheel_dir),
            "-r", str(requirements_file)
        ]
        
        print(f"\nDownloading wheels for platform: {platform_tag}")
        
        # For Apple Silicon, we'll skip cx-Oracle and use oracledb instead
        if is_apple_silicon:
            print("\nNote: Using oracledb instead of cx-Oracle for Apple Silicon support")
            # Remove cx-Oracle from requirements temporarily
            temp_requirements = requirements_file.parent / "temp_requirements.txt"
            with open(requirements_file, 'r') as f:
                requirements = f.readlines()
            with open(temp_requirements, 'w') as f:
                for req in requirements:
                    if not req.strip().startswith('cx-Oracle'):
                        f.write(req)
            cmd[-1] = str(temp_requirements)
        
        subprocess.run(cmd, check=True)
        
        # Clean up temporary requirements file if it exists
        if is_apple_silicon and temp_requirements.exists():
            temp_requirements.unlink()
        
        print(f"Successfully downloaded wheels to: {wheel_dir}")
        
    except subprocess.CalledProcessError as e:
        print(f"Error downloading wheels: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

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
    download_wheels(requirements_file, wheel_dir)
    
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