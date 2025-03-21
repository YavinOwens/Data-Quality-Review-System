"""
Oracle EBS 12.1.1 HR Core Configuration
Contains connection details and table mappings for HR Core validation
"""

# Oracle EBS Connection Configuration
ORACLE_CONFIG = {
    'host': '${ORACLE_HOST}',
    'port': '${ORACLE_PORT}',
    'service_name': '${ORACLE_SERVICE}',
    'user': '${ORACLE_USER}',
    'password': '${ORACLE_PASSWORD}',
    'schema': 'HR'
}

# HR Core Table Mappings
HR_CORE_TABLES = {
    'workers': {
        'table_name': 'PER_ALL_PEOPLE_F',
        'description': 'Active Workers Information',
        'key_columns': ['PERSON_ID', 'EFFECTIVE_START_DATE', 'EFFECTIVE_END_DATE'],
        'required_columns': [
            'PERSON_ID',
            'EFFECTIVE_START_DATE',
            'EFFECTIVE_END_DATE',
            'FIRST_NAME',
            'LAST_NAME',
            'SEX',
            'DATE_OF_BIRTH',
            'NATIONAL_IDENTIFIER',
            'EMPLOYEE_NUMBER',
            'CURRENT_EMPLOYEE_FLAG'
        ]
    },
    'assignments': {
        'table_name': 'PER_ALL_ASSIGNMENTS_F',
        'description': 'Worker Assignments',
        'key_columns': ['ASSIGNMENT_ID', 'EFFECTIVE_START_DATE', 'EFFECTIVE_END_DATE'],
        'required_columns': [
            'ASSIGNMENT_ID',
            'PERSON_ID',
            'EFFECTIVE_START_DATE',
            'EFFECTIVE_END_DATE',
            'ASSIGNMENT_TYPE',
            'ASSIGNMENT_STATUS_TYPE_ID',
            'BUSINESS_GROUP_ID',
            'ORGANIZATION_ID',
            'JOB_ID',
            'POSITION_ID',
            'GRADE_ID'
        ]
    },
    'addresses': {
        'table_name': 'PER_ADDRESSES_F',
        'description': 'Worker Addresses',
        'key_columns': ['ADDRESS_ID', 'EFFECTIVE_START_DATE', 'EFFECTIVE_END_DATE'],
        'required_columns': [
            'ADDRESS_ID',
            'PERSON_ID',
            'EFFECTIVE_START_DATE',
            'EFFECTIVE_END_DATE',
            'ADDRESS_TYPE',
            'ADDRESS_LINE1',
            'TOWN_OR_CITY',
            'POSTAL_CODE',
            'COUNTRY'
        ]
    },
    'communications': {
        'table_name': 'PER_CONTACTS_F',
        'description': 'Worker Contact Information',
        'key_columns': ['CONTACT_ID', 'EFFECTIVE_START_DATE', 'EFFECTIVE_END_DATE'],
        'required_columns': [
            'CONTACT_ID',
            'PERSON_ID',
            'EFFECTIVE_START_DATE',
            'EFFECTIVE_END_DATE',
            'CONTACT_TYPE',
            'CONTACT_VALUE',
            'PRIMARY_FLAG'
        ]
    }
}

# Validation Rules Configuration
VALIDATION_RULES = {
    'workers': {
        'date_rules': {
            'EFFECTIVE_START_DATE': {
                'not_null': True,
                'date_format': 'YYYY-MM-DD',
                'valid_range': {
                    'min': '1900-01-01',
                    'max': '4712-12-31'
                }
            },
            'EFFECTIVE_END_DATE': {
                'not_null': True,
                'date_format': 'YYYY-MM-DD',
                'valid_range': {
                    'min': '1900-01-01',
                    'max': '4712-12-31'
                }
            },
            'DATE_OF_BIRTH': {
                'not_null': True,
                'date_format': 'YYYY-MM-DD',
                'valid_range': {
                    'min': '1900-01-01',
                    'max': '4712-12-31'
                }
            }
        },
        'string_rules': {
            'FIRST_NAME': {
                'not_null': True,
                'min_length': 1,
                'max_length': 150,
                'pattern': r'^[A-Za-z\s\-\.]+$'
            },
            'LAST_NAME': {
                'not_null': True,
                'min_length': 1,
                'max_length': 150,
                'pattern': r'^[A-Za-z\s\-\.]+$'
            },
            'SEX': {
                'not_null': True,
                'allowed_values': ['M', 'F', 'X']
            },
            'EMPLOYEE_NUMBER': {
                'not_null': True,
                'unique': True,
                'pattern': r'^[A-Z0-9]+$'
            }
        }
    },
    'assignments': {
        'date_rules': {
            'EFFECTIVE_START_DATE': {
                'not_null': True,
                'date_format': 'YYYY-MM-DD'
            },
            'EFFECTIVE_END_DATE': {
                'not_null': True,
                'date_format': 'YYYY-MM-DD'
            }
        },
        'relationship_rules': {
            'PERSON_ID': {
                'references': 'PER_ALL_PEOPLE_F.PERSON_ID',
                'not_null': True
            }
        }
    },
    'addresses': {
        'date_rules': {
            'EFFECTIVE_START_DATE': {
                'not_null': True,
                'date_format': 'YYYY-MM-DD'
            },
            'EFFECTIVE_END_DATE': {
                'not_null': True,
                'date_format': 'YYYY-MM-DD'
            }
        },
        'relationship_rules': {
            'PERSON_ID': {
                'references': 'PER_ALL_PEOPLE_F.PERSON_ID',
                'not_null': True
            }
        },
        'string_rules': {
            'ADDRESS_TYPE': {
                'not_null': True,
                'allowed_values': ['HOME', 'WORK', 'MAILING']
            },
            'POSTAL_CODE': {
                'not_null': True,
                'pattern': r'^[A-Z0-9\s\-]+$'
            }
        }
    },
    'communications': {
        'date_rules': {
            'EFFECTIVE_START_DATE': {
                'not_null': True,
                'date_format': 'YYYY-MM-DD'
            },
            'EFFECTIVE_END_DATE': {
                'not_null': True,
                'date_format': 'YYYY-MM-DD'
            }
        },
        'relationship_rules': {
            'PERSON_ID': {
                'references': 'PER_ALL_PEOPLE_F.PERSON_ID',
                'not_null': True
            }
        },
        'string_rules': {
            'CONTACT_TYPE': {
                'not_null': True,
                'allowed_values': ['EMAIL', 'PHONE', 'MOBILE']
            },
            'CONTACT_VALUE': {
                'not_null': True,
                'pattern': {
                    'EMAIL': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
                    'PHONE': r'^\+?[0-9\s\-\(\)]+$',
                    'MOBILE': r'^\+?[0-9\s\-\(\)]+$'
                }
            }
        }
    }
} 