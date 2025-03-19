# PL/SQL Data Validation Suite for Oracle EBS HR

This repository contains a comprehensive set of PL/SQL validation scripts designed for Oracle 19c EBS HR data. These scripts help identify and fix common data quality issues in HR core data.

## Validation Scripts

### Core Validations
- **validations.sql**: Original validation scripts for name cleansing

### Name Validations
- **name_field_validation.sql**: Comprehensive validation for first name, middle names, last name, preferred name, and known as fields

### Email Validations
- **email_validation.sql**: Email format validation, domain standardization, and duplicate detection

### Address Validations
- **address_validation.sql**: UK address validation including postcode validation using official GOV.UK regex patterns

### Job History Validations
- **job_history_validation.sql**: Validates job dates, standardizes job titles, and identifies overlapping assignments

### National Insurance Number Validations
- **national_insurance_validation.sql**: Validates UK National Insurance Numbers against HMRC standards

### Date of Birth Validations
- **date_of_birth_validation.sql**: Checks for valid dates, plausible age ranges, and common date format errors

### Data Quality Review
- **data_quality_review.sql**: Creates a masked data quality review table with consolidated validation results

### Execution Script
- **executions.sql**: Master script that executes all validations in sequence and provides comprehensive reporting

## Usage

1. Run individual validation scripts as needed:
   ```sql
   @validations.sql
   @name_field_validation.sql
   @email_validation.sql
   ```

2. Or run the complete validation suite using the executions script:
   ```sql
   @executions.sql
   ```

## Features

- Consistent validation pattern across all scripts
- Detailed logging of all changes
- Privacy-focused data masking for reporting
- Integration between validation results
- Summary statistics and demographic reporting

## Requirements

- Oracle 19c EBS
- Oracle SQL Developer or similar tool
- Appropriate database privileges
