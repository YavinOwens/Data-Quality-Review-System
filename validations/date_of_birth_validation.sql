-- Purpose of this script is to validate date of birth data in HR core records
-- Table is called "per_all_people_f" with date_of_birth field
-- This validation checks for valid dates, age ranges, and common date formatting errors

-- Drop existing objects first
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE per_dob_validation_log';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP SEQUENCE dob_validation_log_seq';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -2289 THEN RAISE; END IF;
END;
/

-- Create log table with correct structure
CREATE TABLE per_dob_validation_log (
   log_id NUMBER,
   operation VARCHAR2(100),
   field_name VARCHAR2(100),
   operation_date TIMESTAMP,
   records_affected NUMBER
);

-- Create sequence for log_id
CREATE SEQUENCE dob_validation_log_seq
   START WITH 1
   INCREMENT BY 1
   NOCACHE
   NOCYCLE;
   
-- Create trigger for log_id
CREATE OR REPLACE TRIGGER dob_validation_log_bir
BEFORE INSERT ON per_dob_validation_log
FOR EACH ROW
BEGIN
   IF :new.log_id IS NULL THEN
       :new.log_id := dob_validation_log_seq.NEXTVAL;
   END IF;
END;
/

-- This process will be done in four parts:
-- Part 1: Create a working copy of data with relevant columns
-- Part 2: Validate basic date integrity and plausibility
-- Part 3: Check for common date format problems and typos
-- Part 4: Cross-validate against other employee data

-- Create procedure to copy table and add validation columns
CREATE OR REPLACE PROCEDURE create_dob_clean_copy
IS
   v_sql VARCHAR2(4000);
BEGIN
   -- Drop the table if it exists
   BEGIN
       v_sql := 'DROP TABLE per_dob_clean';
       EXECUTE IMMEDIATE v_sql;
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -942 THEN  -- Table doesn't exist
               RAISE;
           END IF;
   END;
   
   -- Create new table with selected columns and validation columns
   v_sql := '
       CREATE TABLE per_dob_clean AS
       SELECT
           person_id,
           employee_number,
           date_of_birth,
           effective_start_date,
           CAST(''PENDING'' AS VARCHAR2(20)) as dob_validation_status,
           CAST(NULL AS VARCHAR2(4000)) as dob_validation_message,
           CAST(NULL AS DATE) as adjusted_date_of_birth,
           SYSDATE as dq_copy
       FROM per_all_people_f
       WHERE date_of_birth IS NOT NULL';
   EXECUTE IMMEDIATE v_sql;

   DBMS_OUTPUT.PUT_LINE('Table per_dob_clean created successfully with ' ||
       TO_CHAR((SELECT COUNT(*) FROM per_dob_clean)) || ' records.');
EXCEPTION
   WHEN OTHERS THEN
       DBMS_OUTPUT.PUT_LINE('Error creating table: ' || SQLERRM);
       RAISE;
END create_dob_clean_copy;
/

-- Create procedure to validate basic date integrity
CREATE OR REPLACE PROCEDURE validate_date_integrity
IS
   v_null_count NUMBER := 0;
   v_invalid_count NUMBER := 0;
   v_future_count NUMBER := 0;
   v_valid_count NUMBER := 0;
   v_current_date DATE := TRUNC(SYSDATE);
   v_sql VARCHAR2(4000);
BEGIN
   -- Mark records with NULL dates
   v_sql := 'UPDATE per_dob_clean 
             SET dob_validation_status = ''NULL'',
                 dob_validation_message = ''Missing date of birth''
             WHERE date_of_birth IS NULL
             RETURNING COUNT(*) INTO :1';
   EXECUTE IMMEDIATE v_sql USING OUT v_null_count;
   
   -- Some systems may store invalid dates that still have values
   -- Try to identify these using a basic validation test
   BEGIN
       v_sql := 'UPDATE per_dob_clean
                 SET dob_validation_status = ''INVALID'',
                     dob_validation_message = ''Invalid date format or value''
                 WHERE dob_validation_status = ''PENDING''
                 AND (
                     EXTRACT(YEAR FROM date_of_birth) < 1900 OR
                     EXTRACT(YEAR FROM date_of_birth) > EXTRACT(YEAR FROM SYSDATE) OR
                     EXTRACT(MONTH FROM date_of_birth) < 1 OR
                     EXTRACT(MONTH FROM date_of_birth) > 12 OR
                     EXTRACT(DAY FROM date_of_birth) < 1 OR
                     EXTRACT(DAY FROM date_of_birth) > 31
                 )
                 RETURNING COUNT(*) INTO :1';
       EXECUTE IMMEDIATE v_sql USING OUT v_invalid_count;
   EXCEPTION
       WHEN OTHERS THEN
           -- If exception occurs during validation, mark records we couldn't validate
           v_sql := 'UPDATE per_dob_clean
                     SET dob_validation_status = ''ERROR'',
                         dob_validation_message = ''Error validating date: '' || :1
                     WHERE dob_validation_status = ''PENDING''';
           EXECUTE IMMEDIATE v_sql USING SQLERRM;
           COMMIT;
           RAISE;
   END;
   
   -- Mark dates in the future (impossible for DOB)
   v_sql := 'UPDATE per_dob_clean
             SET dob_validation_status = ''FUTURE'',
                 dob_validation_message = ''Date of birth is in the future''
             WHERE dob_validation_status = ''PENDING''
             AND date_of_birth > :1
             RETURNING COUNT(*) INTO :2';
   EXECUTE IMMEDIATE v_sql USING v_current_date, OUT v_future_count;
   
   -- Mark the remaining as valid basic dates
   v_sql := 'UPDATE per_dob_clean
             SET dob_validation_status = ''VALID''
             WHERE dob_validation_status = ''PENDING''
             RETURNING COUNT(*) INTO :1';
   EXECUTE IMMEDIATE v_sql USING OUT v_valid_count;
   
   -- Log validation results
   INSERT INTO per_dob_validation_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Basic validation', 'DATE_OF_BIRTH', SYSTIMESTAMP, 
        v_null_count + v_invalid_count + v_future_count + v_valid_count);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Date integrity validation complete:');
   DBMS_OUTPUT.PUT_LINE('- Missing dates: ' || v_null_count);
   DBMS_OUTPUT.PUT_LINE('- Invalid dates: ' || v_invalid_count);
   DBMS_OUTPUT.PUT_LINE('- Future dates: ' || v_future_count);
   DBMS_OUTPUT.PUT_LINE('- Valid dates: ' || v_valid_count);

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error validating date integrity: ' || SQLERRM);
       RAISE;
END validate_date_integrity;
/

-- Create procedure to check for plausible age ranges
CREATE OR REPLACE PROCEDURE validate_age_ranges
IS
   v_minor_count NUMBER := 0;
   v_implausible_count NUMBER := 0;
   v_senior_count NUMBER := 0;
   v_current_date DATE := TRUNC(SYSDATE);
   v_min_working_age NUMBER := 16; -- Minimum legal working age in UK
   v_retirement_age NUMBER := 67;  -- Current UK state pension age
   v_max_plausible_age NUMBER := 110; -- Maximum plausible age
BEGIN
   -- Identify employees who would be minors (under minimum working age)
   UPDATE per_dob_clean
   SET dob_validation_status = 'WARNING',
       dob_validation_message = 'Employee appears to be under minimum working age'
   WHERE dob_validation_status = 'VALID'
   AND MONTHS_BETWEEN(v_current_date, date_of_birth)/12 < v_min_working_age
   RETURNING COUNT(*) INTO v_minor_count;
   
   -- Identify implausibly old employees (over maximum plausible age)
   UPDATE per_dob_clean
   SET dob_validation_status = 'IMPLAUSIBLE',
       dob_validation_message = 'Implausible age (over ' || v_max_plausible_age || ' years old)'
   WHERE dob_validation_status = 'VALID'
   AND MONTHS_BETWEEN(v_current_date, date_of_birth)/12 > v_max_plausible_age
   RETURNING COUNT(*) INTO v_implausible_count;
   
   -- Flag employees over retirement age (may be valid but worth checking)
   UPDATE per_dob_clean
   SET dob_validation_status = 'SENIOR',
       dob_validation_message = 'Employee is over retirement age'
   WHERE dob_validation_status = 'VALID'
   AND MONTHS_BETWEEN(v_current_date, date_of_birth)/12 > v_retirement_age
   RETURNING COUNT(*) INTO v_senior_count;
   
   -- Log validation results
   INSERT INTO per_dob_validation_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Age range validation', 'DATE_OF_BIRTH', SYSTIMESTAMP, 
        v_minor_count + v_implausible_count + v_senior_count);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Age range validation complete:');
   DBMS_OUTPUT.PUT_LINE('- Under minimum working age: ' || v_minor_count);
   DBMS_OUTPUT.PUT_LINE('- Implausibly old: ' || v_implausible_count);
   DBMS_OUTPUT.PUT_LINE('- Over retirement age: ' || v_senior_count);

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error validating age ranges: ' || SQLERRM);
       RAISE;
END validate_age_ranges;
/

-- Create procedure to check for common date format errors
CREATE OR REPLACE PROCEDURE check_date_format_errors
IS
   v_transposed_count NUMBER := 0;
   v_century_count NUMBER := 0;
   v_current_date DATE := TRUNC(SYSDATE);
   v_current_year NUMBER := EXTRACT(YEAR FROM v_current_date);
BEGIN
   -- Check for potential month/day transposition
   -- This is common when different regions use different formats (DD/MM vs MM/DD)
   UPDATE per_dob_clean
   SET dob_validation_status = 'TRANSPOSED',
       dob_validation_message = 'Possible month/day transposition',
       adjusted_date_of_birth = TO_DATE(
           EXTRACT(YEAR FROM date_of_birth) || '-' ||
           EXTRACT(DAY FROM date_of_birth) || '-' ||
           EXTRACT(MONTH FROM date_of_birth),
           'YYYY-DD-MM'
       )
   WHERE dob_validation_status IN ('INVALID', 'FUTURE')
   AND EXTRACT(MONTH FROM date_of_birth) > 12 -- This is actually a day value
   AND EXTRACT(DAY FROM date_of_birth) <= 12  -- This could be a month value
   AND EXTRACT(YEAR FROM date_of_birth) BETWEEN 1900 AND v_current_year
   RETURNING COUNT(*) INTO v_transposed_count;
   
   -- Check for potential century error (e.g., 1920 vs 2020 or 1980 vs 1880)
   -- Focus on dates that would make employee implausibly old or young
   UPDATE per_dob_clean
   SET dob_validation_status = 'CENTURY_ERROR',
       dob_validation_message = 'Possible wrong century',
       adjusted_date_of_birth = ADD_MONTHS(date_of_birth, 12*100) -- Add 100 years
   WHERE dob_validation_status = 'IMPLAUSIBLE'
   AND MONTHS_BETWEEN(v_current_date, ADD_MONTHS(date_of_birth, 12*100))/12 BETWEEN 18 AND 65
   RETURNING COUNT(*) INTO v_century_count;
   
   -- Check for another century pattern: typing 19XX instead of 20XX for recent births
   UPDATE per_dob_clean
   SET dob_validation_status = 'CENTURY_ERROR',
       dob_validation_message = 'Possible wrong century (19XX vs 20XX)',
       adjusted_date_of_birth = ADD_MONTHS(date_of_birth, 12*100) -- Add 100 years
   WHERE dob_validation_status = 'VALID'
   AND EXTRACT(YEAR FROM date_of_birth) < 1950
   AND MONTHS_BETWEEN(v_current_date, date_of_birth)/12 > 70
   AND MONTHS_BETWEEN(v_current_date, ADD_MONTHS(date_of_birth, 12*100))/12 BETWEEN 0 AND 30
   RETURNING COUNT(*) INTO v_century_count;
   
   -- Log validation results
   INSERT INTO per_dob_validation_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Date format checks', 'DATE_OF_BIRTH', SYSTIMESTAMP, 
        v_transposed_count + v_century_count);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Date format checks complete:');
   DBMS_OUTPUT.PUT_LINE('- Possible month/day transposition: ' || v_transposed_count);
   DBMS_OUTPUT.PUT_LINE('- Possible century errors: ' || v_century_count);

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error checking date formats: ' || SQLERRM);
       RAISE;
END check_date_format_errors;
/

-- Create procedure to cross-validate against other employee data
CREATE OR REPLACE PROCEDURE cross_validate_dates
IS
   v_hire_before_birth_count NUMBER := 0;
   v_hire_as_minor_count NUMBER := 0;
   v_min_working_age NUMBER := 16; -- Minimum legal working age in UK
   v_sql VARCHAR2(4000);
BEGIN
   -- Check if hire date is before birth date (impossible)
   v_sql := 'UPDATE per_dob_clean
             SET dob_validation_status = ''CROSS_ERROR'',
                 dob_validation_message = ''Hire date before birth date''
             WHERE dob_validation_status = ''VALID''
             AND effective_start_date IS NOT NULL
             AND effective_start_date < date_of_birth
             RETURNING COUNT(*) INTO :1';
   EXECUTE IMMEDIATE v_sql USING OUT v_hire_before_birth_count;
   
   -- Check if employee was hired under minimum working age
   v_sql := 'UPDATE per_dob_clean
             SET dob_validation_status = ''CROSS_WARNING'',
                 dob_validation_message = ''Hired below minimum working age''
             WHERE dob_validation_status = ''VALID''
             AND effective_start_date IS NOT NULL
             AND MONTHS_BETWEEN(effective_start_date, date_of_birth)/12 < :1
             RETURNING COUNT(*) INTO :2';
   EXECUTE IMMEDIATE v_sql USING v_min_working_age, OUT v_hire_as_minor_count;
   
   -- Log validation results
   INSERT INTO per_dob_validation_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Cross-validation', 'DATE_OF_BIRTH', SYSTIMESTAMP, 
        v_hire_before_birth_count + v_hire_as_minor_count);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Cross-validation complete:');
   DBMS_OUTPUT.PUT_LINE('- Hire date before birth date: ' || v_hire_before_birth_count);
   DBMS_OUTPUT.PUT_LINE('- Hired below minimum working age: ' || v_hire_as_minor_count);

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error in cross-validation: ' || SQLERRM);
       RAISE;
END cross_validate_dates;
/

-- Create procedure to add age categories for reporting
CREATE OR REPLACE PROCEDURE add_age_categories
IS
   v_current_date DATE := TRUNC(SYSDATE);
   v_sql VARCHAR2(4000);
   v_column_exists NUMBER;
BEGIN
   -- Check if age_years column exists
   SELECT COUNT(*)
   INTO v_column_exists
   FROM user_tab_columns
   WHERE table_name = 'PER_DOB_CLEAN'
   AND column_name = 'AGE_YEARS';

   -- Add age_years column if it doesn't exist
   IF v_column_exists = 0 THEN
       v_sql := 'ALTER TABLE per_dob_clean ADD (age_years NUMBER)';
       EXECUTE IMMEDIATE v_sql;
   END IF;

   -- Check if age_group column exists
   SELECT COUNT(*)
   INTO v_column_exists
   FROM user_tab_columns
   WHERE table_name = 'PER_DOB_CLEAN'
   AND column_name = 'AGE_GROUP';

   -- Add age_group column if it doesn't exist
   IF v_column_exists = 0 THEN
       v_sql := 'ALTER TABLE per_dob_clean ADD (age_group VARCHAR2(20))';
       EXECUTE IMMEDIATE v_sql;
   END IF;
   
   -- Calculate age in years
   v_sql := 'UPDATE per_dob_clean
             SET age_years = FLOOR(MONTHS_BETWEEN(:1, date_of_birth)/12)
             WHERE dob_validation_status = ''VALID''';
   EXECUTE IMMEDIATE v_sql USING v_current_date;
   
   -- Assign age groups for demographic reporting
   v_sql := 'UPDATE per_dob_clean
             SET age_group = 
                 CASE 
                     WHEN age_years < 20 THEN ''Under 20''
                     WHEN age_years BETWEEN 20 AND 29 THEN ''20-29''
                     WHEN age_years BETWEEN 30 AND 39 THEN ''30-39''
                     WHEN age_years BETWEEN 40 AND 49 THEN ''40-49''
                     WHEN age_years BETWEEN 50 AND 59 THEN ''50-59''
                     WHEN age_years BETWEEN 60 AND 69 THEN ''60-69''
                     WHEN age_years >= 70 THEN ''70+''
                     ELSE ''Unknown''
                 END
             WHERE dob_validation_status = ''VALID''';
   EXECUTE IMMEDIATE v_sql;
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Age categories added successfully');
   
   -- Log the results
   v_sql := 'INSERT INTO per_dob_validation_log
             (operation, field_name, operation_date, records_affected)
             SELECT 
                 ''Add age categories'',
                 ''DATE_OF_BIRTH'',
                 SYSTIMESTAMP,
                 COUNT(*)
             FROM per_dob_clean
             WHERE age_group IS NOT NULL';
   EXECUTE IMMEDIATE v_sql;

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error adding age categories: ' || SQLERRM);
       RAISE;
END add_age_categories;
/

-- Execution syntax
/*
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
WITH age_stats AS (
    SELECT COUNT(*) as total_count
    FROM per_dob_clean
    WHERE age_group IS NOT NULL
)
SELECT 
    age_group,
    COUNT(*) as employee_count,
    ROUND(COUNT(*) * 100 / (SELECT total_count FROM age_stats), 2) as percentage
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
    effective_start_date,
    dob_validation_message
FROM per_dob_clean
WHERE dob_validation_status IN ('CROSS_ERROR', 'CROSS_WARNING')
ORDER BY employee_number;
*/

-- Add table comments after all objects are created
COMMENT ON TABLE per_dob_validation_log IS 'Log table tracking all date of birth validation operations';
COMMENT ON COLUMN per_dob_validation_log.log_id IS 'Unique identifier for the log entry';
COMMENT ON COLUMN per_dob_validation_log.operation IS 'Type of validation operation performed';
COMMENT ON COLUMN per_dob_validation_log.field_name IS 'Name of the field being validated';
COMMENT ON COLUMN per_dob_validation_log.operation_date IS 'Timestamp when the operation was performed';
COMMENT ON COLUMN per_dob_validation_log.records_affected IS 'Number of records affected by the operation';

COMMENT ON TABLE per_dob_clean IS 'Cleansed version of date of birth data with validation flags';
COMMENT ON COLUMN per_dob_clean.person_id IS 'Unique identifier for the person';
COMMENT ON COLUMN per_dob_clean.employee_number IS 'Employee number for the person';
COMMENT ON COLUMN per_dob_clean.date_of_birth IS 'The date of birth value';
COMMENT ON COLUMN per_dob_clean.effective_start_date IS 'Start date of the person record';
COMMENT ON COLUMN per_dob_clean.dob_validation_status IS 'Status of DOB validation (VALID, INVALID, NULL, FUTURE, etc.)';
COMMENT ON COLUMN per_dob_clean.dob_validation_message IS 'Detailed message explaining validation result';
COMMENT ON COLUMN per_dob_clean.adjusted_date_of_birth IS 'Suggested corrected version of invalid date of birth';
COMMENT ON COLUMN per_dob_clean.dq_copy IS 'Timestamp when the record was copied for cleansing';
COMMENT ON COLUMN per_dob_clean.age_years IS 'Calculated age in years';
COMMENT ON COLUMN per_dob_clean.age_group IS 'Demographic age group category';
