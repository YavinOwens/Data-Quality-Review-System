"""
Unit tests for data schema validation functionality.
"""
import pytest
import pandas as pd
from datetime import datetime
import uuid
from hr_core_validation.schemas.data_schemas import (
    workers_schema,
    addresses_schema,
    communications_schema,
    assignments_schema
)

def test_worker_schema_validation_valid_data():
    """Test worker schema validation with valid data."""
    valid_data = pd.DataFrame({
        'UNIQUE_ID': [str(uuid.uuid4()) for _ in range(3)],
        'PERSON_ID': ['P001', 'P002', 'P003'],
        'EMPLOYEE_NUMBER': ['E001', 'E002', 'E003'],
        'FIRST_NAME': ['John', 'Jane', 'Bob'],
        'LAST_NAME': ['Doe', 'Smith', 'Johnson'],
        'BIRTH_DATE': ['1990-01-01', '1985-06-15', '1992-12-31'],
        'SEX': ['M', 'F', 'O'],
        'MARITAL_STATUS': ['S', 'M', 'D'],
        'NATIONALITY': ['US', 'UK', 'CA'],
        'EFFECTIVE_FROM': ['2020-01-01', '2020-01-01', '2020-01-01'],
        'EFFECTIVE_TO': ['2099-12-31', '2099-12-31', '2099-12-31']
    })
    
    # This should not raise any validation errors
    validated_df = workers_schema.validate(valid_data)
    assert len(validated_df) == 3

def test_worker_schema_validation_invalid_data():
    """Test worker schema validation with invalid data."""
    invalid_data = pd.DataFrame({
        'UNIQUE_ID': ['invalid-uuid', str(uuid.uuid4()), 'another-invalid'],
        'PERSON_ID': ['', 'P002', None],  # Empty and null values
        'EMPLOYEE_NUMBER': ['E001', '', 'E003'],  # Empty value
        'FIRST_NAME': ['J', None, 'Bob'],  # Too short and null
        'LAST_NAME': ['Doe', '', 'Johnson'],  # Empty value
        'BIRTH_DATE': ['2025-01-01', 'invalid-date', '1800-01-01'],  # Future, invalid, and too old
        'SEX': ['X', 'F', 'INVALID'],  # Invalid values
        'MARITAL_STATUS': ['X', 'M', 'INVALID'],  # Invalid values
        'NATIONALITY': ['USA', 'U', 'CANADA'],  # Too long, too short, too long
        'EFFECTIVE_FROM': ['2020-01-01', 'invalid-date', '2020-01-01'],
        'EFFECTIVE_TO': ['2019-12-31', '2099-12-31', 'invalid-date']  # Before effective_from
    })
    
    # This should raise validation errors
    with pytest.raises(Exception) as exc_info:
        workers_schema.validate(invalid_data)
    
    error_message = str(exc_info.value)
    assert any(phrase in error_message for phrase in [
        "UNIQUE_ID",  # Invalid UUID format
        "PERSON_ID",  # Empty/null values
        "FIRST_NAME",  # Too short name
        "BIRTH_DATE",  # Invalid dates
        "SEX",  # Invalid sex values
        "NATIONALITY"  # Invalid country codes
    ])

def test_schema_validation_empty_dataframe():
    """Test schema validation with empty DataFrame."""
    empty_df = pd.DataFrame(columns=[
        'UNIQUE_ID', 'PERSON_ID', 'EMPLOYEE_NUMBER', 'FIRST_NAME', 'LAST_NAME',
        'BIRTH_DATE', 'SEX', 'MARITAL_STATUS', 'NATIONALITY',
        'EFFECTIVE_FROM', 'EFFECTIVE_TO'
    ])
    
    # Empty DataFrame with correct columns should validate
    validated_df = workers_schema.validate(empty_df)
    assert len(validated_df) == 0

def test_schema_validation_missing_required_columns():
    """Test schema validation with missing required columns."""
    incomplete_data = pd.DataFrame({
        'UNIQUE_ID': [str(uuid.uuid4())],
        'FIRST_NAME': ['John'],
        'LAST_NAME': ['Doe']
        # Missing other required columns
    })
    
    with pytest.raises(Exception) as exc_info:
        workers_schema.validate(incomplete_data)
    
    error_message = str(exc_info.value)
    # Pandera reports missing columns one at a time
    assert "column 'PERSON_ID' not in dataframe" in error_message or \
           "column 'EMPLOYEE_NUMBER' not in dataframe" in error_message

def test_schema_validation_incorrect_data_types():
    """Test schema validation with incorrect data types."""
    invalid_types_data = pd.DataFrame({
        'UNIQUE_ID': [123],  # Should be string
        'PERSON_ID': [456],  # Should be string
        'EMPLOYEE_NUMBER': [789],  # Should be string
        'FIRST_NAME': [True],  # Should be string
        'LAST_NAME': [1.23],  # Should be string
        'BIRTH_DATE': [20200101],  # Should be string in date format
        'SEX': [1],  # Should be string
        'MARITAL_STATUS': [True],  # Should be string
        'NATIONALITY': [123],  # Should be string
        'EFFECTIVE_FROM': [20200101],  # Should be string in date format
        'EFFECTIVE_TO': [20991231]  # Should be string in date format
    })
    
    with pytest.raises(Exception) as exc_info:
        workers_schema.validate(invalid_types_data)
    
    error_message = str(exc_info.value)
    assert "expected series 'UNIQUE_ID' to have type str" in error_message

def test_assignment_schema_validation_valid_data():
    """Test assignment schema validation with valid data."""
    valid_data = pd.DataFrame({
        'UNIQUE_ID': [str(uuid.uuid4()) for _ in range(3)],
        'WORKER_UNIQUE_ID': [str(uuid.uuid4()) for _ in range(3)],
        'ASSIGNMENT_NUMBER': ['A001', 'A002', 'A003'],
        'POSITION_ID': ['POS001', 'POS002', 'POS003'],
        'DEPARTMENT_ID': ['DEP001', 'DEP002', 'DEP003'],
        'LOCATION_ID': ['LOC001', 'LOC002', 'LOC003'],
        'EFFECTIVE_FROM': ['2020-01-01', '2020-01-01', '2020-01-01'],
        'EFFECTIVE_TO': ['2099-12-31', '2099-12-31', '2023-12-31']
    })
    
    # This should not raise any validation errors
    validated_df = assignments_schema.validate(valid_data)
    assert len(validated_df) == 3

def test_assignment_schema_validation_invalid_data():
    """Test assignment schema validation with invalid data."""
    invalid_data = pd.DataFrame({
        'UNIQUE_ID': ['invalid-uuid', str(uuid.uuid4()), 'another-invalid'],
        'WORKER_UNIQUE_ID': ['invalid-uuid', str(uuid.uuid4()), None],
        'ASSIGNMENT_NUMBER': ['', 'A002', None],  # Empty, valid, null
        'POSITION_ID': ['', 'POS002', None],  # Empty, valid, null
        'DEPARTMENT_ID': ['', 'DEP002', None],  # Empty, valid, null
        'LOCATION_ID': ['', 'LOC002', None],  # Empty, valid, null
        'EFFECTIVE_FROM': ['invalid-date', '2020-01-01', '2020-13-01'],
        'EFFECTIVE_TO': ['2019-12-31', 'invalid-date', '2020-00-00']
    })
    
    # This should raise validation errors
    with pytest.raises(Exception) as exc_info:
        assignments_schema.validate(invalid_data)
    
    error_message = str(exc_info.value)
    assert any(phrase in error_message for phrase in [
        "UNIQUE_ID",  # Invalid UUID format
        "ASSIGNMENT_NUMBER",  # Empty/null values
        "POSITION_ID",  # Empty/null values
        "DEPARTMENT_ID",  # Empty/null values
        "LOCATION_ID",  # Empty/null values
        "EFFECTIVE_FROM",  # Invalid dates
        "EFFECTIVE_TO"  # Invalid dates
    ])

def test_assignment_date_range_validation():
    """Test validation of assignment date ranges."""
    test_cases = [
        # Valid date ranges
        {
            'EFFECTIVE_FROM': ['2020-01-01', '2021-01-01'],
            'EFFECTIVE_TO': ['2020-12-31', '2099-12-31'],
            'should_pass': True
        },
        # Invalid date ranges (end before start)
        {
            'EFFECTIVE_FROM': ['2020-01-01'],
            'EFFECTIVE_TO': ['2019-12-31'],
            'should_pass': False
        },
        # Invalid date formats
        {
            'EFFECTIVE_FROM': ['2020/01/01'],
            'EFFECTIVE_TO': ['2020/12/31'],
            'should_pass': False
        }
    ]
    
    for case in test_cases:
        data = pd.DataFrame({
            'UNIQUE_ID': [str(uuid.uuid4()) for _ in range(len(case['EFFECTIVE_FROM']))],
            'WORKER_UNIQUE_ID': [str(uuid.uuid4()) for _ in range(len(case['EFFECTIVE_FROM']))],
            'ASSIGNMENT_NUMBER': [f'A{i+1:03d}' for i in range(len(case['EFFECTIVE_FROM']))],
            'POSITION_ID': ['POS001' for _ in range(len(case['EFFECTIVE_FROM']))],
            'DEPARTMENT_ID': ['DEP001' for _ in range(len(case['EFFECTIVE_FROM']))],
            'LOCATION_ID': ['LOC001' for _ in range(len(case['EFFECTIVE_FROM']))],
            'EFFECTIVE_FROM': case['EFFECTIVE_FROM'],
            'EFFECTIVE_TO': case['EFFECTIVE_TO']
        })
        
        if case['should_pass']:
            validated_df = assignments_schema.validate(data)
            assert len(validated_df) == len(case['EFFECTIVE_FROM'])
        else:
            with pytest.raises(Exception) as exc_info:
                assignments_schema.validate(data)
            assert any(phrase in str(exc_info.value) for phrase in ["EFFECTIVE_FROM", "EFFECTIVE_TO"])

def test_assignment_status_transitions():
    """Test validation of assignment status transitions."""
    # Create a sequence of assignments for the same worker
    worker_id = str(uuid.uuid4())
    status_transition_data = pd.DataFrame({
        'UNIQUE_ID': [str(uuid.uuid4()) for _ in range(3)],
        'WORKER_UNIQUE_ID': [worker_id] * 3,
        'ASSIGNMENT_NUMBER': ['A001', 'A002', 'A003'],
        'POSITION_ID': ['POS001'] * 3,
        'DEPARTMENT_ID': ['DEP001'] * 3,
        'LOCATION_ID': ['LOC001'] * 3,
        'EFFECTIVE_FROM': ['2020-01-01', '2020-06-01', '2020-12-01'],
        'EFFECTIVE_TO': ['2020-05-31', '2020-11-30', '2099-12-31']
    })
    
    # This should validate as the schema doesn't enforce status transition rules
    # (that would be handled by business rules validation)
    validated_df = assignments_schema.validate(status_transition_data)
    assert len(validated_df) == 3

def test_address_schema_validation_valid_data():
    """Test address schema validation with valid data."""
    valid_data = pd.DataFrame({
        'UNIQUE_ID': [str(uuid.uuid4()) for _ in range(3)],
        'WORKER_UNIQUE_ID': [str(uuid.uuid4()) for _ in range(3)],
        'ADDRESS_TYPE': ['HOME', 'WORK', 'OTHER'],
        'ADDRESS_LINE1': ['123 Main St', '456 Office Blvd', '789 Other Ave'],
        'ADDRESS_LINE2': ['Apt 1', None, 'Suite 100'],
        'CITY': ['Springfield', 'Metropolis', 'Gotham'],
        'STATE': ['IL', 'NY', 'NJ'],
        'POSTAL_CODE': ['62701', '10001', '07001'],
        'COUNTRY': ['US', 'CA', 'UK'],
        'EFFECTIVE_FROM': ['2020-01-01', '2020-01-01', '2020-01-01'],
        'EFFECTIVE_TO': ['2099-12-31', '2099-12-31', '2099-12-31']
    })
    
    # This should not raise any validation errors
    validated_df = addresses_schema.validate(valid_data)
    assert len(validated_df) == 3

def test_address_schema_validation_invalid_data():
    """Test address schema validation with invalid data."""
    invalid_data = pd.DataFrame({
        'UNIQUE_ID': ['invalid-uuid', str(uuid.uuid4()), 'another-invalid'],
        'WORKER_UNIQUE_ID': ['invalid-uuid', str(uuid.uuid4()), None],
        'ADDRESS_TYPE': ['INVALID', 'WORK', None],
        'ADDRESS_LINE1': ['', 'Too long' * 20, None],  # Empty, too long, null
        'ADDRESS_LINE2': ['Too long' * 20, None, 'Valid'],  # Too long
        'CITY': ['', None, 'C' * 51],  # Empty, null, too long
        'STATE': ['', 'Too long' * 10, None],  # Empty, too long, null
        'POSTAL_CODE': ['', '1' * 21, None],  # Empty, too long, null
        'COUNTRY': ['USA', 'U', 'INVALID'],  # Too long, too short, invalid
        'EFFECTIVE_FROM': ['invalid-date', '2020-01-01', '2020-13-01'],
        'EFFECTIVE_TO': ['2019-12-31', 'invalid-date', '2020-00-00']
    })
    
    # This should raise validation errors
    with pytest.raises(Exception) as exc_info:
        addresses_schema.validate(invalid_data)
    
    error_message = str(exc_info.value)
    assert any(phrase in error_message for phrase in [
        "UNIQUE_ID",  # Invalid UUID format
        "ADDRESS_TYPE",  # Invalid type
        "ADDRESS_LINE1",  # Invalid length
        "CITY",  # Invalid city
        "COUNTRY",  # Invalid country code
        "EFFECTIVE_FROM",  # Invalid dates
        "EFFECTIVE_TO"  # Invalid dates
    ])

def test_address_schema_nullable_fields():
    """Test address schema validation with nullable fields."""
    data_with_nulls = pd.DataFrame({
        'UNIQUE_ID': [str(uuid.uuid4())],
        'WORKER_UNIQUE_ID': [str(uuid.uuid4())],
        'ADDRESS_TYPE': ['HOME'],
        'ADDRESS_LINE1': ['123 Main St'],
        'ADDRESS_LINE2': [None],  # ADDRESS_LINE2 is nullable
        'CITY': ['Springfield'],
        'STATE': ['IL'],
        'POSTAL_CODE': ['62701'],
        'COUNTRY': ['US'],
        'EFFECTIVE_FROM': ['2020-01-01'],
        'EFFECTIVE_TO': ['2099-12-31']
    })
    
    # This should not raise any validation errors
    validated_df = addresses_schema.validate(data_with_nulls)
    assert len(validated_df) == 1
    assert pd.isna(validated_df['ADDRESS_LINE2'].iloc[0])  # Verify null is preserved

def test_communication_schema_validation_valid_data():
    """Test communication schema validation with valid data."""
    valid_data = pd.DataFrame({
        'UNIQUE_ID': [str(uuid.uuid4()) for _ in range(3)],
        'WORKER_UNIQUE_ID': [str(uuid.uuid4()) for _ in range(3)],
        'CONTACT_TYPE': ['EMAIL', 'PHONE', 'EMAIL'],
        'CONTACT_VALUE': [
            'john.doe@example.com',
            '+1234567890',
            'jane.smith@company.com'
        ],
        'PRIMARY_FLAG': ['Y', 'N', 'N'],
        'EFFECTIVE_FROM': ['2020-01-01', '2020-01-01', '2020-01-01'],
        'EFFECTIVE_TO': ['2099-12-31', '2099-12-31', '2099-12-31']
    })
    
    # This should not raise any validation errors
    validated_df = communications_schema.validate(valid_data)
    assert len(validated_df) == 3

def test_communication_schema_validation_invalid_data():
    """Test communication schema validation with invalid data."""
    invalid_data = pd.DataFrame({
        'UNIQUE_ID': ['invalid-uuid', str(uuid.uuid4()), 'another-invalid'],
        'WORKER_UNIQUE_ID': ['invalid-uuid', str(uuid.uuid4()), None],
        'CONTACT_TYPE': ['INVALID', 'EMAIL', 'PHONE'],
        'CONTACT_VALUE': [
            'invalid-email',  # Invalid email format
            'not.an.email',   # Invalid email format
            '123'             # Invalid phone format
        ],
        'PRIMARY_FLAG': ['X', 'Y', None],  # Invalid flag, valid flag, null
        'EFFECTIVE_FROM': ['invalid-date', '2020-01-01', '2020-13-01'],
        'EFFECTIVE_TO': ['2019-12-31', 'invalid-date', '2020-00-00']
    })
    
    # This should raise validation errors
    with pytest.raises(Exception) as exc_info:
        communications_schema.validate(invalid_data)
    
    error_message = str(exc_info.value)
    assert any(phrase in error_message for phrase in [
        "UNIQUE_ID",  # Invalid UUID format
        "CONTACT_TYPE",  # Invalid contact type
        "Invalid contact value format",  # Invalid contact values
        "PRIMARY_FLAG",  # Invalid flag values
        "EFFECTIVE_FROM",  # Invalid dates
        "EFFECTIVE_TO"  # Invalid dates
    ])

def test_communication_contact_value_validation():
    """Test specific validation of contact values based on contact type."""
    test_cases = [
        # Valid cases
        {
            'CONTACT_TYPE': ['EMAIL', 'PHONE'],
            'CONTACT_VALUE': ['test@example.com', '+1234567890'],
            'should_pass': True
        },
        # Invalid email
        {
            'CONTACT_TYPE': ['EMAIL'],
            'CONTACT_VALUE': ['not-an-email'],
            'should_pass': False
        },
        # Invalid phone
        {
            'CONTACT_TYPE': ['PHONE'],
            'CONTACT_VALUE': ['123'],  # Too short
            'should_pass': False
        }
    ]
    
    for case in test_cases:
        data = pd.DataFrame({
            'UNIQUE_ID': [str(uuid.uuid4()) for _ in range(len(case['CONTACT_TYPE']))],
            'WORKER_UNIQUE_ID': [str(uuid.uuid4()) for _ in range(len(case['CONTACT_TYPE']))],
            'CONTACT_TYPE': case['CONTACT_TYPE'],
            'CONTACT_VALUE': case['CONTACT_VALUE'],
            'PRIMARY_FLAG': ['Y' for _ in range(len(case['CONTACT_TYPE']))],
            'EFFECTIVE_FROM': ['2020-01-01' for _ in range(len(case['CONTACT_TYPE']))],
            'EFFECTIVE_TO': ['2099-12-31' for _ in range(len(case['CONTACT_TYPE']))]
        })
        
        if case['should_pass']:
            validated_df = communications_schema.validate(data)
            assert len(validated_df) == len(case['CONTACT_TYPE'])
        else:
            with pytest.raises(Exception) as exc_info:
                communications_schema.validate(data)
            assert "Invalid contact value format" in str(exc_info.value)

def test_communication_primary_flag_constraints():
    """Test constraints on primary flag values."""
    # Test multiple primary contacts of the same type
    worker_id = str(uuid.uuid4())
    multiple_primary_data = pd.DataFrame({
        'UNIQUE_ID': [str(uuid.uuid4()) for _ in range(3)],
        'WORKER_UNIQUE_ID': [worker_id] * 3,
        'CONTACT_TYPE': ['EMAIL'] * 3,
        'CONTACT_VALUE': [
            'primary1@example.com',
            'primary2@example.com',
            'secondary@example.com'
        ],
        'PRIMARY_FLAG': ['Y', 'Y', 'N'],  # Multiple primary emails
        'EFFECTIVE_FROM': ['2020-01-01'] * 3,
        'EFFECTIVE_TO': ['2099-12-31'] * 3
    })
    
    # This should still validate as the schema doesn't enforce uniqueness
    # (that would be handled by business rules validation)
    validated_df = communications_schema.validate(multiple_primary_data)
    assert len(validated_df) == 3

# TODO: Implement remaining test functions for assignment schema 