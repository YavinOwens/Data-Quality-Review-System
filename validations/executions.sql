-- ======================================================================
-- COMPREHENSIVE EXECUTION SCRIPT FOR ALL VALIDATION PROCEDURES
-- ======================================================================
-- This script contains all the execution commands for running the 
-- various data quality validation procedures we've created.
-- It's structured to run each validation in a logical order and
-- includes SQL queries to view the validation results.
-- ======================================================================

SET SERVEROUTPUT ON;


-- ======================================================================
-- SECTION 1: ORIGINAL VALIDATIONS
-- ======================================================================
PROMPT *** Running Original Validations ***

-- Create a copy of the people table with today's date
SELECT * FROM per_all_people_f_clean;
EXEC create_people_clean_copy;

-- Clean the data
EXEC clean_name_spaces;
EXEC clean_numeric_suffixes;

-- View the log by field:
SELECT field_name, operation, records_affected, operation_date
FROM per_all_people_f_clean_log
ORDER BY operation_date;

-- View summary by field:
SELECT field_name, SUM(records_affected) as total_records_cleaned
FROM per_all_people_f_clean_log
GROUP BY field_name;

-- Show data 
SELECT 
row_number() OVER (ORDER BY person_id) as emp_rn,
person_id,
first_name,
middle_names,
last_name 
FROM per_all_people_f_clean
OFFSET 11 ROWS;

-- ======================================================================
-- SECTION 2: NAME FIELD VALIDATION
-- ======================================================================
PROMPT *** Running Name Field Validation ***

-- Create a copy of the table with today's date
EXEC create_names_clean_copy;

-- Run validation procedures
EXEC standardize_name_case;
EXEC clean_name_patterns;
EXEC check_name_consistency;
EXEC validate_required_names;

-- View the log by field:
SELECT field_name, operation, records_affected, operation_date
FROM per_name_validation_log
ORDER BY operation_date;

-- View summary by field:
SELECT field_name, SUM(records_affected) as total_records_cleaned
FROM per_name_validation_log
GROUP BY field_name;

-- Show records with missing required fields:
SELECT 
    person_id,
    employee_number,
    first_name,
    last_name
FROM per_names_clean
WHERE has_missing_required = 'Y';

-- Show records where name inconsistencies were found and fixed:
SELECT 
    person_id,
    employee_number,
    first_name,
    middle_names,
    last_name,
    known_as,
    preferred_name
FROM per_names_clean
WHERE has_name_inconsistency = 'Y';

-- Show statistics on optional fields:
SELECT 
    COUNT(*) as total_records,
    SUM(CASE WHEN middle_names IS NOT NULL AND TRIM(middle_names) != '' THEN 1 ELSE 0 END) as with_middle_names,
    SUM(CASE WHEN known_as IS NOT NULL AND TRIM(known_as) != '' THEN 1 ELSE 0 END) as with_known_as,
    SUM(CASE WHEN preferred_name IS NOT NULL AND TRIM(preferred_name) != '' THEN 1 ELSE 0 END) as with_preferred_name
FROM per_names_clean;

-- ======================================================================
-- SECTION 3: EMAIL VALIDATION
-- ======================================================================
PROMPT *** Running Email Validation ***

-- Create a copy of the email data
EXEC create_emails_clean_copy;

-- Run email validation procedures
EXEC standardize_email_format;
EXEC validate_email_domains;
EXEC check_duplicate_emails;

-- View email validation log
SELECT operation, field_name, records_affected, operation_date
FROM per_email_validation_log
ORDER BY operation_date;

-- View invalid emails
SELECT 
    employee_number,
    email_address,
    email_validation_message
FROM per_emails_clean
WHERE email_validation_status = 'INVALID'
ORDER BY employee_number;

-- View company emails with non-standard domains
SELECT 
    email_address,
    email_domain,
    standard_company_domain
FROM per_emails_clean
WHERE email_validation_status = 'NON_STANDARD_DOMAIN'
ORDER BY email_domain;

-- View duplicate emails
SELECT 
    email_address,
    COUNT(*) as occurrence_count
FROM per_emails_clean
WHERE email_validation_status = 'DUPLICATE'
GROUP BY email_address
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

-- ======================================================================
-- SECTION 4: UK ADDRESS VALIDATION
-- ======================================================================
PROMPT *** Running UK Address Validation ***

-- Create a copy of the address data (filtered for UKI only)
EXEC create_addresses_clean_copy;

-- Run address validation procedures
EXEC standardize_uk_counties;
EXEC validate_uk_postcodes;
EXEC identify_incomplete_addresses;

-- View address validation log
SELECT operation, field_name, records_affected, operation_date
FROM per_address_validation_log
ORDER BY operation_date;

-- View addresses with invalid UK postcodes
SELECT 
    address_id,
    address_line1,
    address_line2,
    town_or_city,
    county,
    postal_code
FROM per_addresses_clean
WHERE is_valid_uk_postcode = 'N'
ORDER BY address_id;

-- View incomplete UK addresses
SELECT 
    address_id,
    address_line1,
    address_line2,
    town_or_city,
    county,
    postal_code,
    missing_components
FROM per_addresses_clean
WHERE is_incomplete_address = 'Y'
ORDER BY address_id;

-- View summary of UK counties after standardization
SELECT 
    county,
    COUNT(*) as address_count
FROM per_addresses_clean
GROUP BY county
ORDER BY COUNT(*) DESC;

-- ======================================================================
-- SECTION 5: JOB HISTORY VALIDATION
-- ======================================================================
PROMPT *** Running Job History Validation ***

-- Create a copy of the job history data
EXEC create_job_history_clean_copy;

-- Run job history validation procedures
EXEC validate_job_dates;
EXEC standardize_job_titles;
EXEC identify_job_overlaps;

-- View job history validation log
SELECT operation, field_name, records_affected, operation_date
FROM per_job_validation_log
ORDER BY operation_date;

-- View job history with date issues
SELECT 
    employee_number,
    job_title,
    effective_start_date,
    effective_end_date,
    job_validation_message
FROM per_job_history_clean
WHERE job_validation_status = 'DATE_ERROR'
ORDER BY employee_number, effective_start_date;

-- View standardized job titles
SELECT 
    original_job_title,
    job_title,
    COUNT(*) as occurrence_count
FROM per_job_history_clean
WHERE original_job_title != job_title
GROUP BY original_job_title, job_title
ORDER BY COUNT(*) DESC;

-- View employees with overlapping job assignments
SELECT 
    employee_number,
    job_title,
    effective_start_date,
    effective_end_date,
    overlap_job_id
FROM per_job_history_clean
WHERE has_job_overlap = 'Y'
ORDER BY employee_number, effective_start_date;

-- ======================================================================
-- SECTION 6: NATIONAL INSURANCE NUMBER VALIDATION
-- ======================================================================
PROMPT *** Running National Insurance Number Validation ***

-- Create a copy of the table with relevant columns
EXEC create_nino_clean_copy;

-- Run validation procedures
EXEC standardize_nino_format;
EXEC validate_nino;
EXEC check_nino_typos;
EXEC add_nino_suggestions;

-- View the log by operation:
SELECT operation, field_name, records_affected, operation_date
FROM per_nino_validation_log
ORDER BY operation_date;

-- View summary of validation results:
SELECT nino_validation_status, COUNT(*) as count
FROM per_nino_clean
GROUP BY nino_validation_status
ORDER BY COUNT(*) DESC;

-- View invalid NINOs with suggested fixes:
SELECT 
    employee_number,
    national_identifier as original_nino,
    nino_validation_message,
    suggested_nino
FROM per_nino_clean
WHERE nino_validation_status = 'INVALID'
AND suggested_nino IS NOT NULL;

-- View all invalid NINOs:
SELECT 
    employee_number,
    national_identifier,
    nino_validation_message
FROM per_nino_clean
WHERE nino_validation_status = 'INVALID'
ORDER BY employee_number;

-- View administrative and temporary numbers:
SELECT 
    employee_number,
    national_identifier,
    nino_validation_status,
    nino_validation_message
FROM per_nino_clean
WHERE nino_validation_status IN ('ADMINISTRATIVE', 'TEMPORARY')
ORDER BY nino_validation_status, employee_number;

-- ======================================================================
-- SECTION 7: DATE OF BIRTH VALIDATION
-- ======================================================================
PROMPT *** Running Date of Birth Validation ***

-- Create a copy of the table with relevant columns
EXEC create_dob_clean_copy;

-- Run validation procedures
EXEC validate_date_integrity;
EXEC validate_age_ranges;
EXEC check_date_format_errors;
EXEC cross_validate_dates;
EXEC add_age_categories;

-- View the log by operation:
SELECT operation, field_name, records_affected, operation_date
FROM per_dob_validation_log
ORDER BY operation_date;

-- View summary of validation results:
SELECT dob_validation_status, COUNT(*) as count
FROM per_dob_clean
GROUP BY dob_validation_status
ORDER BY COUNT(*) DESC;

-- View potential date format errors with suggested fixes:
SELECT 
    employee_number,
    date_of_birth as original_dob,
    adjusted_date_of_birth as suggested_dob,
    dob_validation_message,
    FLOOR(MONTHS_BETWEEN(SYSDATE, date_of_birth)/12) as original_age,
    FLOOR(MONTHS_BETWEEN(SYSDATE, adjusted_date_of_birth)/12) as adjusted_age
FROM per_dob_clean
WHERE adjusted_date_of_birth IS NOT NULL
ORDER BY employee_number;

-- View age demographic breakdown:
SELECT 
    age_group,
    COUNT(*) as employee_count,
    ROUND(COUNT(*) * 100 / (SELECT COUNT(*) FROM per_dob_clean WHERE age_group IS NOT NULL), 2) as percentage
FROM per_dob_clean
WHERE age_group IS NOT NULL
GROUP BY age_group
ORDER BY 
    CASE age_group
        WHEN 'Under 20' THEN 1
        WHEN '20-29' THEN 2
        WHEN '30-39' THEN 3
        WHEN '40-49' THEN 4
        WHEN '50-59' THEN 5
        WHEN '60-69' THEN 6
        WHEN '70+' THEN 7
        ELSE 8
    END;

-- View records with cross-validation errors:
SELECT 
    employee_number,
    date_of_birth,
    hire_date,
    dob_validation_message
FROM per_dob_clean
WHERE dob_validation_status IN ('CROSS_ERROR', 'CROSS_WARNING')
ORDER BY employee_number;

-- ======================================================================
-- SECTION 8: DATA QUALITY REVIEW
-- ======================================================================
PROMPT *** Running Data Quality Review ***

-- Generate the DQ review table with masked data
EXEC generate_dq_review_table;

-- Add validation flags (if validation tables exist)
EXEC add_dq_validation_flags;

-- Generate summary statistics
EXEC generate_dq_summary;

-- View all employees in the DQ review
SELECT 
    employee_number,
    masked_first_name,
    masked_middle_name,
    masked_last_name,
    masked_known_as,
    person_type_count,
    person_types
FROM dq_review
ORDER BY employee_number;

-- View employees with validation issues
SELECT 
    employee_number,
    CASE WHEN has_name_issues = 'Y' THEN 'Yes' ELSE 'No' END as name_issues,
    CASE WHEN has_email_issues = 'Y' THEN 'Yes' ELSE 'No' END as email_issues,
    CASE WHEN has_address_issues = 'Y' THEN 'Yes' ELSE 'No' END as address_issues,
    CASE WHEN has_dob_issues = 'Y' THEN 'Yes' ELSE 'No' END as dob_issues,
    CASE WHEN has_nino_issues = 'Y' THEN 'Yes' ELSE 'No' END as nino_issues,
    validation_comments
FROM dq_review
WHERE has_name_issues = 'Y'
   OR has_email_issues = 'Y'
   OR has_address_issues = 'Y'
   OR has_dob_issues = 'Y'
   OR has_nino_issues = 'Y'
ORDER BY employee_number;

-- View DQ summary statistics
SELECT * FROM dq_summary;

-- ======================================================================
-- SECTION 9: CONSOLIDATED ISSUES REPORT
-- ======================================================================
PROMPT *** Generating Consolidated Issues Report ***

-- Create a consolidated issues view if it doesn't exist
BEGIN
   EXECUTE IMMEDIATE 'DROP VIEW consolidated_issues_view';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

-- Create a view that consolidates all issues from different validation processes
CREATE OR REPLACE VIEW consolidated_issues_view AS
WITH all_employees AS (
    SELECT DISTINCT employee_number
    FROM dq_review
),
name_issues AS (
    SELECT 
        nc.employee_number,
        'Name issue: ' || 
        CASE 
            WHEN nc.has_missing_required = 'Y' THEN 'Missing required fields. '
            WHEN nc.has_name_inconsistency = 'Y' THEN 'Inconsistent name fields. '
            ELSE ''
        END as issue_description
    FROM per_names_clean nc
    WHERE nc.has_missing_required = 'Y' OR nc.has_name_inconsistency = 'Y'
),
email_issues AS (
    SELECT 
        ec.employee_number,
        'Email issue: ' || ec.email_validation_message as issue_description
    FROM per_emails_clean ec
    WHERE ec.email_validation_status IN ('INVALID', 'DUPLICATE', 'NON_STANDARD_DOMAIN')
),
address_issues AS (
    SELECT 
        ac.employee_number,
        'Address issue: ' || 
        CASE 
            WHEN ac.is_incomplete_address = 'Y' THEN 'Incomplete address (' || ac.missing_components || '). '
            WHEN ac.is_valid_uk_postcode = 'N' THEN 'Invalid UK postcode. '
            ELSE ''
        END as issue_description
    FROM per_addresses_clean ac
    WHERE ac.is_incomplete_address = 'Y' OR ac.is_valid_uk_postcode = 'N'
),
dob_issues AS (
    SELECT 
        dc.employee_number,
        'DOB issue: ' || dc.dob_validation_message as issue_description
    FROM per_dob_clean dc
    WHERE dc.dob_validation_status NOT IN ('VALID', 'SENIOR')
),
nino_issues AS (
    SELECT 
        nc.employee_number,
        'NINO issue: ' || nc.nino_validation_message || 
        CASE 
            WHEN nc.suggested_nino IS NOT NULL THEN ' Suggested: ' || nc.suggested_nino
            ELSE ''
        END as issue_description
    FROM per_nino_clean nc
    WHERE nc.nino_validation_status NOT IN ('VALID')
)
SELECT 
    ae.employee_number,
    dr.masked_first_name,
    dr.masked_last_name,
    i.issue_description
FROM all_employees ae
JOIN dq_review dr ON ae.employee_number = dr.employee_number
LEFT JOIN (
    SELECT * FROM name_issues
    UNION ALL
    SELECT * FROM email_issues
    UNION ALL
    SELECT * FROM address_issues
    UNION ALL
    SELECT * FROM dob_issues
    UNION ALL
    SELECT * FROM nino_issues
) i ON ae.employee_number = i.employee_number
WHERE i.issue_description IS NOT NULL
ORDER BY ae.employee_number, i.issue_description;

-- View the consolidated issues
SELECT * FROM consolidated_issues_view;

-- Count issues by employee
SELECT 
    employee_number,
    masked_first_name,
    masked_last_name,
    COUNT(*) as issue_count
FROM consolidated_issues_view
GROUP BY employee_number, masked_first_name, masked_last_name
ORDER BY COUNT(*) DESC;

-- Count issues by type
SELECT 
    SUBSTR(issue_description, 1, INSTR(issue_description, ':')) as issue_type,
    COUNT(*) as issue_count,
    ROUND(COUNT(*) * 100 / (SELECT COUNT(*) FROM consolidated_issues_view), 2) as percentage
FROM consolidated_issues_view
GROUP BY SUBSTR(issue_description, 1, INSTR(issue_description, ':'))
ORDER BY COUNT(*) DESC;

PROMPT *** All validation procedures executed successfully ***
