#!/usr/bin/env python3
"""
Script to run test_installs.py and save outputs to PDF.
"""
import os
import subprocess
from pathlib import Path

def run_tests_and_save_pdf():
    # Get the directory of this script
    script_dir = Path(__file__).parent
    parent_dir = script_dir.parent
    
    # Create output directory if it doesn't exist
    output_dir = parent_dir / "test_outputs"
    output_dir.mkdir(exist_ok=True)
    
    # Convert test_installs.py to notebook
    print("Converting test_installs.py to notebook...")
    subprocess.run([
        "jupytext",
        "--to", "notebook",
        str(script_dir / "test_installs.py"),
        "-o", str(output_dir / "test_installs.ipynb")
    ], check=True)
    
    # Run the notebook and save outputs
    print("Running notebook and saving outputs...")
    subprocess.run([
        "jupyter", "nbconvert",
        "--execute",
        "--to", "pdf",
        "--output", str(output_dir / "test_results"),
        str(output_dir / "test_installs.ipynb")
    ], check=True)
    
    print(f"\nTest results saved to: {output_dir / 'test_results.pdf'}")

if __name__ == "__main__":
    run_tests_and_save_pdf() 