-- purpose of this script is to cleanse the suffixes from the names, this is to ensure that the names are consistent and to avoid duplicates.
-- table is called "per_all_people_f".
-- Drop existing objects first
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE per_all_people_f_clean_log';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP SEQUENCE clean_log_seq';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -2289 THEN RAISE; END IF;
END;
/
-- Create log table with correct structure
CREATE TABLE per_all_people_f_clean_log (
   log_id NUMBER,
   operation VARCHAR2(100),
   field_name VARCHAR2(100),
   operation_date TIMESTAMP,
   records_affected NUMBER
);
-- Create sequence for log_id
CREATE SEQUENCE clean_log_seq
   START WITH 1
   INCREMENT BY 1
   NOCACHE
   NOCYCLE;
   
-- Create trigger for log_id
CREATE OR REPLACE TRIGGER clean_log_bir
BEFORE INSERT ON per_all_people_f_clean_log
FOR EACH ROW
BEGIN
   IF :new.log_id IS NULL THEN
       :new.log_id := clean_log_seq.NEXTVAL;
   END IF;
END;
/
-- this process will be done in three parts:
-- part 1
-- 1. create a copy of the table called "per_all_people_f_clean"
-- 2. select columns that are needed from the table "per_all_people_f"
-- part 2
-- 3. cleanse the first name, middle names to have no leading or trailing spaces
-- 4. remove values in the suffix column that are not in the fnd list of suffixes if avaliable
-- part 3
-- 5. procedure creates a table that provides the count of changes agains the copy of the table
-- ** all changes are logged in the table "per_all_people_f_clean_log"
-- ** all changes are done on the per_all_people_f_clean table
-- Create procedure to copy table and add dq_copy column
CREATE OR REPLACE PROCEDURE create_people_clean_copy
IS
BEGIN
   -- Drop the table if it exists
   BEGIN
       EXECUTE IMMEDIATE 'DROP TABLE per_all_people_f_clean';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -942 THEN  -- Table doesn't exist
               RAISE;
           END IF;
   END;
   -- Create new table with selected columns and dq_copy timestamp
   EXECUTE IMMEDIATE '
       CREATE TABLE per_all_people_f_clean AS
       SELECT
           person_id,
           employee_number,
           first_name,
           middle_names,
           last_name,
           known_as,
--           preferred_name,
           date_of_birth,
           national_identifier,
           suffix,
           SYSDATE as dq_copy
       FROM hr.per_all_people_f';
--   DBMS_OUTPUT.PUT_LINE('Table per_all_people_f_clean created successfully with ' ||
--       TO_CHAR((SELECT COUNT(*) FROM per_all_people_f_clean)) || ' records.');
EXCEPTION
   WHEN OTHERS THEN
       DBMS_OUTPUT.PUT_LINE('Error creating table: ' || SQLERRM);
       RAISE;
END create_people_clean_copy;
/
-- Create procedure to clean numeric suffixes
CREATE OR REPLACE PROCEDURE clean_numeric_suffixes
IS
   v_affected_rows NUMBER := 0;
BEGIN
   -- Update records with any numeric content, including spaces between numbers
   UPDATE per_all_people_f_clean
   SET suffix = NULL
   WHERE suffix IS NOT NULL
   AND (
       -- Pure numbers
       REGEXP_LIKE(suffix, '^[0-9]+$')
       -- Numbers with spaces between them
       OR REGEXP_LIKE(REPLACE(TRIM(suffix), ' ', ''), '^[0-9]+$')
       -- Contains any numbers
       OR REGEXP_LIKE(suffix, '[0-9]')
       -- Multiple spaces between characters
       OR suffix LIKE '% %'
   )
   RETURNING COUNT(*) INTO v_affected_rows;
   -- Log the cleaning operation
   INSERT INTO per_all_people_f_clean_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Remove numeric values', 'SUFFIX', SYSTIMESTAMP, v_affected_rows);
   COMMIT;
   DBMS_OUTPUT.PUT_LINE('Cleaned ' || v_affected_rows || ' records with numeric or invalid suffixes.');
   -- Double check if any numbers remain
   FOR r IN (
       SELECT DISTINCT suffix
       FROM per_all_people_f_clean
       WHERE suffix IS NOT NULL
       AND REGEXP_LIKE(suffix, '[0-9]')
   ) LOOP
       DBMS_OUTPUT.PUT_LINE('Warning: Suffix still contains numbers: ' || r.suffix);
   END LOOP;
EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error cleaning numeric suffixes: ' || SQLERRM);
       RAISE;
END clean_numeric_suffixes;
/
-- Create procedure to clean trailing and leading spaces from name columns
CREATE OR REPLACE PROCEDURE clean_name_spaces
IS
   v_first_name_affected NUMBER := 0;
   v_middle_names_affected NUMBER := 0;
   v_last_name_affected NUMBER := 0;
BEGIN
   -- Clean first_name column
   UPDATE per_all_people_f_clean
   SET first_name = TRIM(first_name)
   WHERE first_name IS NOT NULL
   AND (first_name LIKE ' %' OR first_name LIKE '% ')
   RETURNING COUNT(*) INTO v_first_name_affected;
   -- Log first_name changes
   IF v_first_name_affected > 0 THEN
       INSERT INTO per_all_people_f_clean_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Remove spaces', 'FIRST_NAME', SYSTIMESTAMP, v_first_name_affected);
   END IF;
   -- Clean middle_names column
   UPDATE per_all_people_f_clean
   SET middle_names = TRIM(middle_names)
   WHERE middle_names IS NOT NULL
   AND (middle_names LIKE ' %' OR middle_names LIKE '% ')
   RETURNING COUNT(*) INTO v_middle_names_affected;
   -- Log middle_names changes
   IF v_middle_names_affected > 0 THEN
       INSERT INTO per_all_people_f_clean_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Remove spaces', 'MIDDLE_NAMES', SYSTIMESTAMP, v_middle_names_affected);
   END IF;
   -- Clean last_name column
   UPDATE per_all_people_f_clean
   SET last_name = TRIM(last_name)
   WHERE last_name IS NOT NULL
   AND (last_name LIKE ' %' OR last_name LIKE '% ')
   RETURNING COUNT(*) INTO v_last_name_affected;
   -- Log last_name changes
   IF v_last_name_affected > 0 THEN
       INSERT INTO per_all_people_f_clean_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Remove spaces', 'LAST_NAME', SYSTIMESTAMP, v_last_name_affected);
   END IF;
   COMMIT;
   -- Output detailed results
   DBMS_OUTPUT.PUT_LINE('Name space cleaning complete:');
   DBMS_OUTPUT.PUT_LINE('- First names cleaned: ' || v_first_name_affected);
   DBMS_OUTPUT.PUT_LINE('- Middle names cleaned: ' || v_middle_names_affected);
   DBMS_OUTPUT.PUT_LINE('- Last names cleaned: ' || v_last_name_affected);
   DBMS_OUTPUT.PUT_LINE('Total records affected: ' ||
       (v_first_name_affected + v_middle_names_affected + v_last_name_affected));
EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error cleaning name spaces: ' || SQLERRM);
       RAISE;
END clean_name_spaces;
/
-- Execution syntax
/*
-- Create a copy of the table with today's date
select * from per_all_people_f_clean
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
 
-- show data 
select 
row_number() over (order by person_id) as emp_rn,
person_id,
first_name,
middle_names,
last_name 
from per_all_people_f_clean
offset 11 rows
 
*/