-- Purpose of this script is to validate and cleanse job history data in HR core data
-- following the same structure as the name cleansing validations.
-- table is called "per_all_assignments_f" for employee job history.

-- Drop existing objects first
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE per_assignments_clean_log';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP SEQUENCE job_clean_log_seq';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -2289 THEN RAISE; END IF;
END;
/

-- Create log table with correct structure
CREATE TABLE per_assignments_clean_log (
   log_id NUMBER,
   operation VARCHAR2(100),
   field_name VARCHAR2(100),
   operation_date TIMESTAMP,
   records_affected NUMBER
);

-- Create sequence for log_id
CREATE SEQUENCE job_clean_log_seq
   START WITH 1
   INCREMENT BY 1
   NOCACHE
   NOCYCLE;
   
-- Create trigger for log_id
CREATE OR REPLACE TRIGGER job_clean_log_bir
BEFORE INSERT ON per_assignments_clean_log
FOR EACH ROW
BEGIN
   IF :new.log_id IS NULL THEN
       :new.log_id := job_clean_log_seq.NEXTVAL;
   END IF;
END;
/

-- This process will be done in three parts:
-- Part 1
-- 1. Create a copy of the job history data called "per_assignments_clean"
-- 2. Select columns that are needed from the table "per_all_assignments_f" 
-- Part 2
-- 3. Validate date ranges (start date before end date)
-- 4. Standardize job titles
-- 5. Identify overlapping job assignments
-- Part 3
-- 6. Flag assignments with missing critical data
-- ** All changes are logged in the table "per_assignments_clean_log"
-- ** All changes are done on the per_assignments_clean table

-- Create procedure to copy table and add dq_copy column
CREATE OR REPLACE PROCEDURE create_assignments_clean_copy
IS
BEGIN
   -- Drop the table if it exists
   BEGIN
       EXECUTE IMMEDIATE 'DROP TABLE per_assignments_clean';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -942 THEN  -- Table doesn't exist
               RAISE;
           END IF;
   END;

   -- Create new table with selected columns and dq_copy timestamp
   EXECUTE IMMEDIATE '
       CREATE TABLE per_assignments_clean AS
       SELECT
           assignment_id,
           person_id,
           effective_start_date,
           effective_end_date,
           primary_flag,
           organization_id,
           job_id,
           position_id,
           grade_id,
           location_id,
           supervisor_id,
           assignment_status_type_id,
           SYSDATE as dq_copy
       FROM hr.per_all_assignments_f';

   DBMS_OUTPUT.PUT_LINE('Table per_assignments_clean created successfully with ' ||
       TO_CHAR((SELECT COUNT(*) FROM per_assignments_clean)) || ' records.');
EXCEPTION
   WHEN OTHERS THEN
       DBMS_OUTPUT.PUT_LINE('Error creating table: ' || SQLERRM);
       RAISE;
END create_assignments_clean_copy;
/

-- Create procedure to validate date ranges
CREATE OR REPLACE PROCEDURE validate_assignment_dates
IS
   v_affected_rows NUMBER := 0;
BEGIN
   -- Create a new column to mark invalid date ranges
   BEGIN
       EXECUTE IMMEDIATE 'ALTER TABLE per_assignments_clean ADD (has_valid_dates VARCHAR2(1) DEFAULT ''Y'')';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -1430 THEN  -- Column already exists
               RAISE;
           END IF;
   END;

   -- Identify records where start date is after end date
   UPDATE per_assignments_clean
   SET has_valid_dates = 'N'
   WHERE effective_start_date > effective_end_date
   RETURNING COUNT(*) INTO v_affected_rows;

   -- Log the validation operation
   INSERT INTO per_assignments_clean_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Validate dates', 'EFFECTIVE_DATES', SYSTIMESTAMP, v_affected_rows);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Identified ' || v_affected_rows || ' records with invalid date ranges.');

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error validating assignment dates: ' || SQLERRM);
       RAISE;
END validate_assignment_dates;
/

-- Create procedure to standardize job titles
-- Assumes a lookup table per_jobs with job_id and job_title
CREATE OR REPLACE PROCEDURE standardize_job_titles
IS
   v_affected_rows NUMBER := 0;
BEGIN
   -- Create temp table for standardized job titles if it doesn't exist
   BEGIN
       EXECUTE IMMEDIATE '
           CREATE TABLE per_std_job_titles AS
           SELECT 
               job_id,
               job_title as original_title,
               job_title as standard_title
           FROM hr.per_jobs';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -955 THEN  -- Table already exists
               RAISE;
           END IF;
   END;

   -- Example of standardizing specific job titles
   -- This would be customized based on actual job title patterns in your data
   UPDATE per_std_job_titles
   SET standard_title = 'Software Engineer'
   WHERE REGEXP_LIKE(original_title, '(Software|SW) Eng(ineer)?', 'i')
   OR REGEXP_LIKE(original_title, 'Programmer Analyst', 'i')
   RETURNING COUNT(*) INTO v_affected_rows;

   -- Log the standardization operation
   IF v_affected_rows > 0 THEN
       INSERT INTO per_assignments_clean_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Standardize titles', 'JOB_TITLE', SYSTIMESTAMP, v_affected_rows);
   END IF;

   -- Additional standardization examples
   UPDATE per_std_job_titles
   SET standard_title = 'Sales Representative'
   WHERE REGEXP_LIKE(original_title, 'Sales Rep(resentative)?', 'i')
   OR REGEXP_LIKE(original_title, 'Account Executive', 'i')
   RETURNING COUNT(*) INTO v_affected_rows;

   -- Log the standardization operation
   IF v_affected_rows > 0 THEN
       INSERT INTO per_assignments_clean_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Standardize titles', 'JOB_TITLE', SYSTIMESTAMP, v_affected_rows);
   END IF;

   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Job title standardization complete.');

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error standardizing job titles: ' || SQLERRM);
       RAISE;
END standardize_job_titles;
/

-- Create procedure to identify overlapping job assignments
CREATE OR REPLACE PROCEDURE identify_overlapping_assignments
IS
   v_affected_rows NUMBER := 0;
BEGIN
   -- Create a new column to mark overlapping assignments
   BEGIN
       EXECUTE IMMEDIATE 'ALTER TABLE per_assignments_clean ADD (has_overlap VARCHAR2(1) DEFAULT ''N'')';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -1430 THEN  -- Column already exists
               RAISE;
           END IF;
   END;

   -- Identify overlapping assignments for the same person
   UPDATE per_assignments_clean a1
   SET has_overlap = 'Y'
   WHERE EXISTS (
       SELECT 1
       FROM per_assignments_clean a2
       WHERE a1.person_id = a2.person_id
       AND a1.assignment_id != a2.assignment_id
       AND a1.primary_flag = 'Y'
       AND a2.primary_flag = 'Y'
       AND a1.effective_start_date <= a2.effective_end_date
       AND a1.effective_end_date >= a2.effective_start_date
   )
   RETURNING COUNT(*) INTO v_affected_rows;

   -- Log the overlap identification operation
   INSERT INTO per_assignments_clean_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Identify overlaps', 'ASSIGNMENTS', SYSTIMESTAMP, v_affected_rows);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Identified ' || v_affected_rows || ' overlapping primary assignments.');

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error identifying overlapping assignments: ' || SQLERRM);
       RAISE;
END identify_overlapping_assignments;
/

-- Create procedure to flag assignments with missing critical data
CREATE OR REPLACE PROCEDURE flag_incomplete_assignments
IS
   v_affected_rows NUMBER := 0;
BEGIN
   -- Create a new column to mark incomplete assignments
   BEGIN
       EXECUTE IMMEDIATE 'ALTER TABLE per_assignments_clean ADD (is_complete VARCHAR2(1) DEFAULT ''Y'')';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -1430 THEN  -- Column already exists
               RAISE;
           END IF;
   END;

   -- Identify assignments missing critical data
   UPDATE per_assignments_clean
   SET is_complete = 'N'
   WHERE (
       job_id IS NULL OR 
       organization_id IS NULL OR 
       effective_start_date IS NULL OR 
       effective_end_date IS NULL OR 
       assignment_status_type_id IS NULL
   )
   RETURNING COUNT(*) INTO v_affected_rows;

   -- Log the incomplete identification operation
   INSERT INTO per_assignments_clean_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Flag incomplete', 'ASSIGNMENTS', SYSTIMESTAMP, v_affected_rows);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Identified ' || v_affected_rows || ' incomplete assignments.');

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error flagging incomplete assignments: ' || SQLERRM);
       RAISE;
END flag_incomplete_assignments;
/

-- Execution syntax
/*
-- Create a copy of the table with today's date
EXEC create_assignments_clean_copy;

-- Run validation procedures
EXEC validate_assignment_dates;
EXEC standardize_job_titles;
EXEC identify_overlapping_assignments;
EXEC flag_incomplete_assignments;

-- View the log by field:
SELECT field_name, operation, records_affected, operation_date
FROM per_assignments_clean_log
ORDER BY operation_date;

-- View summary by field:
SELECT field_name, SUM(records_affected) as total_records_cleaned
FROM per_assignments_clean_log
GROUP BY field_name;

-- Show assignments with invalid dates
SELECT 
    assignment_id,
    person_id,
    effective_start_date,
    effective_end_date
FROM per_assignments_clean
WHERE has_valid_dates = 'N'
ORDER BY person_id;

-- Show overlapping assignments
SELECT 
    assignment_id,
    person_id,
    effective_start_date,
    effective_end_date
FROM per_assignments_clean
WHERE has_overlap = 'Y'
ORDER BY person_id, effective_start_date;

-- Show incomplete assignments
SELECT 
    assignment_id,
    person_id,
    job_id,
    organization_id,
    effective_start_date,
    effective_end_date,
    assignment_status_type_id
FROM per_assignments_clean
WHERE is_complete = 'N'
ORDER BY person_id;

-- Join with job titles table to see standardized titles
SELECT 
    a.assignment_id,
    a.person_id,
    j.original_title,
    j.standard_title
FROM per_assignments_clean a
JOIN per_std_job_titles j ON a.job_id = j.job_id
ORDER BY j.standard_title;
*/
