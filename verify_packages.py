#!/usr/bin/env python3
"""
Verification script to test if all installed packages are working correctly.
"""

def test_numpy():
    print("\nTesting numpy...")
    import numpy as np
    arr = np.array([1, 2, 3, 4, 5])
    print(f"NumPy array operation: {arr.mean()}")

def test_pandas():
    print("\nTesting pandas...")
    import pandas as pd
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    print(f"Pandas DataFrame:\n{df}")

def test_polars():
    print("\nTesting polars...")
    import polars as pl
    df = pl.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    print(f"Polars DataFrame:\n{df}")

def test_sqlalchemy():
    print("\nTesting SQLAlchemy...")
    from sqlalchemy import create_engine, text
    engine = create_engine('sqlite:///:memory:')
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print(f"SQLAlchemy query result: {result.scalar()}")

def test_rapidfuzz():
    print("\nTesting rapidfuzz...")
    from rapidfuzz import fuzz
    similarity = fuzz.ratio("hello world", "hello there")
    print(f"Fuzzy string similarity: {similarity}%")

def test_great_expectations():
    print("\nTesting great_expectations...")
    import great_expectations as gx
    import pandas as pd
    from great_expectations.core import ExpectationSuite
    from great_expectations.core.expectation_validation_result import ExpectationValidationResult
    
    # Create sample data
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, -5],  # Note: -5 is invalid age
        'email': ['alice@email.com', 'bob@email.com', 'charlie@email.com', 'invalid_email', 'eve@email.com']
    })
    
    # Create an ephemeral context
    context = gx.get_context(mode="ephemeral")
    print(f"Created context type: {type(context).__name__}")
    
    # Create an expectation suite
    suite = ExpectationSuite(name="my_suite")
    
    # Create a data source
    data_source = context.data_sources.add_pandas(name="my_pandas_source")
    print("Created pandas data source")
    
    # Create a data asset
    data_asset = data_source.add_dataframe_asset(name="my_dataframe_asset")
    print("Created dataframe asset")
    
    # Create a batch definition
    batch_def = data_asset.add_batch_definition_whole_dataframe(name="my_batch")
    print("Created batch definition")
    
    # Create batch parameters
    batch_parameters = {"dataframe": df}
    
    # Get the batch request
    batch_request = batch_def.build_batch_request(batch_parameters)
    
    # Create a validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite
    )
    
    # Add expectations using the validator with COMPLETE result format
    age_result = validator.expect_column_values_to_be_between(
        column="age",
        min_value=0,
        max_value=120,
        result_format="COMPLETE"
    )
    
    email_result = validator.expect_column_values_to_match_regex(
        column="email",
        regex=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        result_format="COMPLETE"
    )
    
    print(f"\nGreat Expectations version: {gx.__version__}")
    print(f"Validation Results:")
    
    # Process age validation results
    print("\nAge validation:")
    if isinstance(age_result, ExpectationValidationResult):
        print(f"Success: {age_result.success}")
        if not age_result.success and 'result' in age_result:
            unexpected = age_result.result.get('unexpected_list', [])
            print(f"Invalid ages found: {unexpected}")
            print(f"Total records: {age_result.result.get('element_count', 0)}")
            print(f"Unexpected count: {age_result.result.get('unexpected_count', 0)}")
    
    # Process email validation results
    print("\nEmail validation:")
    if isinstance(email_result, ExpectationValidationResult):
        print(f"Success: {email_result.success}")
        if not email_result.success and 'result' in email_result:
            unexpected = email_result.result.get('unexpected_list', [])
            print(f"Invalid emails found: {unexpected}")
            print(f"Total records: {email_result.result.get('element_count', 0)}")
            print(f"Unexpected count: {email_result.result.get('unexpected_count', 0)}")

def main():
    print("Starting package verification...")
    
    tests = [
        test_numpy,
        test_pandas,
        test_polars,
        test_sqlalchemy,
        test_rapidfuzz,
        test_great_expectations
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