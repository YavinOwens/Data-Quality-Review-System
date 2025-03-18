"""
Pandera schemas for HR Core data validation
"""

import pandera as pa
from pandera.typing import Series
from datetime import datetime
import re
import pandas as pd

# Common UUID pattern
UUID_PATTERN = r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'

# Common date pattern
DATE_PATTERN = r'^\d{4}-\d{2}-\d{2}$'

# Common check functions
def validate_date_range(series: pd.Series, min_date: str = '1900-01-01', max_date: str = '2099-12-31') -> pd.Series:
    """Validate that dates are within a specified range."""
    try:
        dates = pd.to_datetime(series, format='%Y-%m-%d')
        min_dt = pd.to_datetime(min_date)
        max_dt = pd.to_datetime(max_date)
        return (dates >= min_dt) & (dates <= max_dt)
    except ValueError:
        return pd.Series([False] * len(series))

def validate_effective_dates(df: pd.DataFrame) -> pd.Series:
    """Validate that EFFECTIVE_TO is not before EFFECTIVE_FROM."""
    try:
        from_dates = pd.to_datetime(df['EFFECTIVE_FROM'], format='%Y-%m-%d')
        to_dates = pd.to_datetime(df['EFFECTIVE_TO'], format='%Y-%m-%d')
        return to_dates >= from_dates
    except ValueError:
        return pd.Series([False] * len(df))

def validate_birth_date(series: pd.Series) -> pd.Series:
    """Validate birth dates are reasonable (not in future, not too old)."""
    return validate_date_range(series, min_date='1900-01-01', max_date=datetime.now().strftime('%Y-%m-%d'))

def validate_contact_value(df: pd.DataFrame) -> pd.Series:
    def check_value(row):
        contact_type = row.CONTACT_TYPE
        contact_value = row.CONTACT_VALUE
        if contact_type == 'EMAIL':
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            return bool(re.match(email_pattern, contact_value))
        elif contact_type == 'PHONE':
            phone_pattern = r'^\+?1?\d{9,15}$'
            return bool(re.match(phone_pattern, contact_value))
        return False
    return df.apply(check_value, axis=1)

# Schema definitions
workers_schema = pa.DataFrameSchema({
    'UNIQUE_ID': pa.Column(str, checks=[
        pa.Check.str_matches(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
    ]),
    'PERSON_ID': pa.Column(str, checks=[pa.Check.str_length(min_value=1, max_value=50)]),
    'EMPLOYEE_NUMBER': pa.Column(str, checks=[pa.Check.str_length(min_value=1, max_value=50)]),
    'FIRST_NAME': pa.Column(str, checks=[pa.Check.str_length(min_value=2, max_value=50)]),
    'LAST_NAME': pa.Column(str, checks=[pa.Check.str_length(min_value=2, max_value=50)]),
    'BIRTH_DATE': pa.Column(str, checks=[
        pa.Check(lambda x: validate_birth_date(x), error="Birth date must be between 1900-01-01 and current date")
    ]),
    'SEX': pa.Column(str, checks=[pa.Check.isin(['M', 'F', 'O'])]),
    'MARITAL_STATUS': pa.Column(str, checks=[pa.Check.isin(['S', 'M', 'D', 'W'])]),
    'NATIONALITY': pa.Column(str, checks=[pa.Check.str_length(min_value=2, max_value=3)]),
    'EFFECTIVE_FROM': pa.Column(str, checks=[
        pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$'),
        pa.Check(lambda x: validate_date_range(x), error="Effective from date must be between 1900-01-01 and 2099-12-31")
    ]),
    'EFFECTIVE_TO': pa.Column(str, checks=[
        pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$'),
        pa.Check(lambda x: validate_date_range(x), error="Effective to date must be between 1900-01-01 and 2099-12-31")
    ])
}, checks=[
    pa.Check(validate_effective_dates, error="EFFECTIVE_TO date must not be before EFFECTIVE_FROM date")
])

addresses_schema = pa.DataFrameSchema({
    'UNIQUE_ID': pa.Column(str, checks=[
        pa.Check.str_matches(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
    ]),
    'WORKER_UNIQUE_ID': pa.Column(str, checks=[
        pa.Check.str_matches(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
    ]),
    'ADDRESS_TYPE': pa.Column(str, checks=[pa.Check.isin(['HOME', 'WORK', 'OTHER'])]),
    'ADDRESS_LINE1': pa.Column(str, checks=[pa.Check.str_length(min_value=1, max_value=100)]),
    'ADDRESS_LINE2': pa.Column(str, nullable=True, checks=[pa.Check.str_length(min_value=0, max_value=100)]),
    'CITY': pa.Column(str, checks=[pa.Check.str_length(min_value=1, max_value=50)]),
    'STATE': pa.Column(str, checks=[pa.Check.str_length(min_value=1, max_value=50)]),
    'POSTAL_CODE': pa.Column(str, checks=[pa.Check.str_length(min_value=1, max_value=20)]),
    'COUNTRY': pa.Column(str, checks=[pa.Check.str_length(min_value=2, max_value=3)]),
    'EFFECTIVE_FROM': pa.Column(str, checks=[
        pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$'),
        pa.Check(lambda x: validate_date_range(x), error="Effective from date must be between 1900-01-01 and 2099-12-31")
    ]),
    'EFFECTIVE_TO': pa.Column(str, checks=[
        pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$'),
        pa.Check(lambda x: validate_date_range(x), error="Effective to date must be between 1900-01-01 and 2099-12-31")
    ])
}, checks=[
    pa.Check(validate_effective_dates, error="EFFECTIVE_TO date must not be before EFFECTIVE_FROM date")
])

communications_schema = pa.DataFrameSchema({
    'UNIQUE_ID': pa.Column(str, checks=[
        pa.Check.str_matches(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
    ]),
    'WORKER_UNIQUE_ID': pa.Column(str, checks=[
        pa.Check.str_matches(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
    ]),
    'CONTACT_TYPE': pa.Column(str, checks=[pa.Check.isin(['EMAIL', 'PHONE'])]),
    'CONTACT_VALUE': pa.Column(str),
    'PRIMARY_FLAG': pa.Column(str, checks=[pa.Check.isin(['Y', 'N'])]),
    'EFFECTIVE_FROM': pa.Column(str, checks=[
        pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$'),
        pa.Check(lambda x: validate_date_range(x), error="Effective from date must be between 1900-01-01 and 2099-12-31")
    ]),
    'EFFECTIVE_TO': pa.Column(str, checks=[
        pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$'),
        pa.Check(lambda x: validate_date_range(x), error="Effective to date must be between 1900-01-01 and 2099-12-31")
    ])
}, checks=[
    pa.Check(validate_contact_value, error="Invalid contact value format for the specified contact type"),
    pa.Check(validate_effective_dates, error="EFFECTIVE_TO date must not be before EFFECTIVE_FROM date")
])

assignments_schema = pa.DataFrameSchema({
    'UNIQUE_ID': pa.Column(str, checks=[
        pa.Check.str_matches(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
    ]),
    'WORKER_UNIQUE_ID': pa.Column(str, checks=[
        pa.Check.str_matches(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
    ]),
    'ASSIGNMENT_NUMBER': pa.Column(str, checks=[pa.Check.str_length(min_value=1, max_value=50)]),
    'POSITION_ID': pa.Column(str, checks=[pa.Check.str_length(min_value=1, max_value=50)]),
    'DEPARTMENT_ID': pa.Column(str, checks=[pa.Check.str_length(min_value=1, max_value=50)]),
    'LOCATION_ID': pa.Column(str, checks=[pa.Check.str_length(min_value=1, max_value=50)]),
    'EFFECTIVE_FROM': pa.Column(str, checks=[
        pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$'),
        pa.Check(lambda x: validate_date_range(x), error="Effective from date must be between 1900-01-01 and 2099-12-31")
    ]),
    'EFFECTIVE_TO': pa.Column(str, checks=[
        pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$'),
        pa.Check(lambda x: validate_date_range(x), error="Effective to date must be between 1900-01-01 and 2099-12-31")
    ])
}, checks=[
    pa.Check(validate_effective_dates, error="EFFECTIVE_TO date must not be before EFFECTIVE_FROM date")
]) 