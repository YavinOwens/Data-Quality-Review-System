#!/usr/bin/env python3
"""
Verification script to test if all installed packages are working correctly.
"""

def test_development_tools():
    print("\nTesting development tools...")
    import pytest
    import black
    import pylint
    import rope
    print("✓ Development tools imported successfully")
    print(f"pytest version: {pytest.__version__}")
    print(f"black version: {black.__version__}")

def test_numpy():
    print("\nTesting numpy...")
    import numpy as np
    arr = np.array([1, 2, 3, 4, 5])
    print(f"NumPy array operation: {arr.mean()}")
    print(f"NumPy version: {np.__version__}")

def test_pandas():
    print("\nTesting pandas...")
    import pandas as pd
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    print(f"Pandas DataFrame:\n{df}")
    print(f"Pandas version: {pd.__version__}")

def test_polars():
    print("\nTesting polars...")
    import polars as pl
    df = pl.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    print(f"Polars DataFrame:\n{df}")
    print(f"Polars version: {pl.__version__}")

def test_sqlalchemy():
    print("\nTesting SQLAlchemy...")
    from sqlalchemy import create_engine, text
    engine = create_engine('sqlite:///:memory:')
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print(f"SQLAlchemy query result: {result.scalar()}")
    import sqlalchemy
    print(f"SQLAlchemy version: {sqlalchemy.__version__}")

def test_visualization():
    print("\nTesting visualization packages...")
    import matplotlib
    import seaborn as sns
    print(f"Matplotlib version: {matplotlib.__version__}")
    print(f"Seaborn version: {sns.__version__}")

def test_jupyter_environment():
    print("\nTesting Jupyter environment...")
    import jupyter_core
    import notebook
    import jupyterlab
    print(f"Jupyter Core version: {jupyter_core.__version__}")
    print(f"Notebook version: {notebook.__version__}")
    print(f"JupyterLab version: {jupyterlab.__version__}")

def test_data_validation():
    print("\nTesting data validation...")
    import pydantic
    from pydantic import BaseModel
    
    class User(BaseModel):
        id: int
        name: str
        age: int
    
    user = User(id=1, name="Test", age=30)
    print(f"Pydantic validation successful: {user.model_dump()}")
    print(f"Pydantic version: {pydantic.__version__}")

def test_additional_dependencies():
    print("\nTesting additional dependencies...")
    import requests
    import yaml
    import dotenv
    import toml
    
    print(f"Requests version: {requests.__version__}")
    print(f"PyYAML version: {yaml.__version__}")
    print(f"python-dotenv version: {dotenv.__version__}")
    print(f"toml version: {toml.__version__}")

def main():
    """Run all package verification tests."""
    print("Starting package verification...")
    
    tests = [
        test_development_tools,
        test_numpy,
        test_pandas,
        test_polars,
        test_sqlalchemy,
        test_visualization,
        test_jupyter_environment,
        test_data_validation,
        test_additional_dependencies
    ]
    
    for test in tests:
        try:
            test()
            print("✅ Test passed!")
        except Exception as e:
            print(f"❌ Test failed: {str(e)}")
    
    print("\nVerification complete!")

if __name__ == "__main__":
    main() 