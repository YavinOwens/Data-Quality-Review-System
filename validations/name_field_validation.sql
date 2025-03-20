-- Purpose of this script is to perform comprehensive validation of all name fields in HR core data
-- following the same structure as the existing validations.
-- Table is called "per_all_people_f" with name fields.
-- This validation handles: first_name, middle_names, last_name, preferred_name, known_as
-- Accounts for the fact that not all individuals will have all name fields populated

-- Drop existing objects first
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE per_name_validation_log';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP SEQUENCE name_validation_log_seq';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -2289 THEN RAISE; END IF;
END;
/

-- Create log table with correct structure
CREATE TABLE per_name_validation_log (
   log_id NUMBER,
   operation VARCHAR2(100),
   field_name VARCHAR2(100),
   operation_date TIMESTAMP,
   records_affected NUMBER
);

-- Create sequence for log_id
CREATE SEQUENCE name_validation_log_seq
   START WITH 1
   INCREMENT BY 1
   NOCACHE
   NOCYCLE;
   
-- Create trigger for log_id
CREATE OR REPLACE TRIGGER name_validation_log_bir
BEFORE INSERT ON per_name_validation_log
FOR EACH ROW
BEGIN
   IF :new.log_id IS NULL THEN
       :new.log_id := name_validation_log_seq.NEXTVAL;
   END IF;
END;
/

-- This process will be done in four parts:
-- Part 1
-- 1. Create a copy of the name data called "per_names_clean"
-- 2. Select columns that are needed from the table "per_all_people_f"
-- Part 2
-- 3. Cleanse all name fields to remove leading/trailing spaces and standardize case
-- 4. Handle NULL values appropriately for optional fields
-- Part 3
-- 5. Flag inconsistencies between name fields
-- 6. Validate name field patterns and content
-- Part 4
-- 7. Create reports on name completeness and quality

-- Create procedure to copy table and add dq_copy column
CREATE OR REPLACE PROCEDURE create_names_clean_copy
IS
BEGIN
   -- Drop the table if it exists
   BEGIN
       EXECUTE IMMEDIATE 'DROP TABLE per_names_clean';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -942 THEN  -- Table doesn't exist
               RAISE;
           END IF;
   END;

   -- Create new table with selected columns and dq_copy timestamp
   EXECUTE IMMEDIATE '
       CREATE TABLE per_names_clean AS
       SELECT
           person_id,
           employee_number,
           first_name,
           middle_names,
           last_name,
           NVL(known_as, '''') as known_as,
           NVL(preferred_name, '''') as preferred_name,
           SYSDATE as dq_copy
       FROM per_all_people_f';

   DBMS_OUTPUT.PUT_LINE('Table per_names_clean created successfully with ' ||
       TO_CHAR((SELECT COUNT(*) FROM per_names_clean)) || ' records.');
EXCEPTION
   WHEN OTHERS THEN
       DBMS_OUTPUT.PUT_LINE('Error creating table: ' || SQLERRM);
       RAISE;
END create_names_clean_copy;
/

-- Create procedure to standardize case for name fields
CREATE OR REPLACE PROCEDURE standardize_name_case
IS
   v_first_name_affected NUMBER := 0;
   v_middle_names_affected NUMBER := 0;
   v_last_name_affected NUMBER := 0;
   v_known_as_affected NUMBER := 0;
   v_preferred_name_affected NUMBER := 0;
BEGIN
   -- Proper case for first_name (first letter uppercase, rest lowercase)
   UPDATE per_names_clean
   SET first_name = INITCAP(TRIM(first_name))
   WHERE first_name IS NOT NULL
   AND (INITCAP(TRIM(first_name)) != first_name)
   RETURNING COUNT(*) INTO v_first_name_affected;

   -- Log first_name changes
   IF v_first_name_affected > 0 THEN
       INSERT INTO per_name_validation_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Standardize case', 'FIRST_NAME', SYSTIMESTAMP, v_first_name_affected);
   END IF;

   -- Proper case for middle_names
   UPDATE per_names_clean
   SET middle_names = INITCAP(TRIM(middle_names))
   WHERE middle_names IS NOT NULL
   AND (INITCAP(TRIM(middle_names)) != middle_names)
   RETURNING COUNT(*) INTO v_middle_names_affected;

   -- Log middle_names changes
   IF v_middle_names_affected > 0 THEN
       INSERT INTO per_name_validation_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Standardize case', 'MIDDLE_NAMES', SYSTIMESTAMP, v_middle_names_affected);
   END IF;

   -- Proper case for last_name
   UPDATE per_names_clean
   SET last_name = INITCAP(TRIM(last_name))
   WHERE last_name IS NOT NULL
   AND (INITCAP(TRIM(last_name)) != last_name)
   RETURNING COUNT(*) INTO v_last_name_affected;

   -- Log last_name changes
   IF v_last_name_affected > 0 THEN
       INSERT INTO per_name_validation_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Standardize case', 'LAST_NAME', SYSTIMESTAMP, v_last_name_affected);
   END IF;

   -- Proper case for known_as
   UPDATE per_names_clean
   SET known_as = INITCAP(TRIM(known_as))
   WHERE known_as IS NOT NULL
   AND known_as != ''
   AND (INITCAP(TRIM(known_as)) != known_as)
   RETURNING COUNT(*) INTO v_known_as_affected;

   -- Log known_as changes
   IF v_known_as_affected > 0 THEN
       INSERT INTO per_name_validation_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Standardize case', 'KNOWN_AS', SYSTIMESTAMP, v_known_as_affected);
   END IF;

   -- Proper case for preferred_name
   UPDATE per_names_clean
   SET preferred_name = INITCAP(TRIM(preferred_name))
   WHERE preferred_name IS NOT NULL
   AND preferred_name != ''
   AND (INITCAP(TRIM(preferred_name)) != preferred_name)
   RETURNING COUNT(*) INTO v_preferred_name_affected;

   -- Log preferred_name changes
   IF v_preferred_name_affected > 0 THEN
       INSERT INTO per_name_validation_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Standardize case', 'PREFERRED_NAME', SYSTIMESTAMP, v_preferred_name_affected);
   END IF;

   COMMIT;
   
   -- Output detailed results
   DBMS_OUTPUT.PUT_LINE('Name case standardization complete:');
   DBMS_OUTPUT.PUT_LINE('- First names standardized: ' || v_first_name_affected);
   DBMS_OUTPUT.PUT_LINE('- Middle names standardized: ' || v_middle_names_affected);
   DBMS_OUTPUT.PUT_LINE('- Last names standardized: ' || v_last_name_affected);
   DBMS_OUTPUT.PUT_LINE('- Known as standardized: ' || v_known_as_affected);
   DBMS_OUTPUT.PUT_LINE('- Preferred names standardized: ' || v_preferred_name_affected);
   DBMS_OUTPUT.PUT_LINE('Total records affected: ' ||
       (v_first_name_affected + v_middle_names_affected + v_last_name_affected + 
        v_known_as_affected + v_preferred_name_affected));

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error standardizing name case: ' || SQLERRM);
       RAISE;
END standardize_name_case;
/

-- Create procedure to handle special characters and multi-part names
CREATE OR REPLACE PROCEDURE clean_name_patterns
IS
   v_first_name_affected NUMBER := 0;
   v_middle_names_affected NUMBER := 0;
   v_last_name_affected NUMBER := 0;
BEGIN
   -- Handle multiple spaces in first_name
   UPDATE per_names_clean
   SET first_name = REGEXP_REPLACE(first_name, '\s{2,}', ' ')
   WHERE first_name IS NOT NULL
   AND REGEXP_LIKE(first_name, '\s{2,}')
   RETURNING COUNT(*) INTO v_first_name_affected;
   
   -- Handle multiple spaces in middle_names
   UPDATE per_names_clean
   SET middle_names = REGEXP_REPLACE(middle_names, '\s{2,}', ' ')
   WHERE middle_names IS NOT NULL
   AND REGEXP_LIKE(middle_names, '\s{2,}')
   RETURNING COUNT(*) INTO v_middle_names_affected;
   
   -- Handle multiple spaces in last_name
   UPDATE per_names_clean
   SET last_name = REGEXP_REPLACE(last_name, '\s{2,}', ' ')
   WHERE last_name IS NOT NULL
   AND REGEXP_LIKE(last_name, '\s{2,}')
   RETURNING COUNT(*) INTO v_last_name_affected;
   
   -- Handle hyphenated last names - ensure proper formatting
   UPDATE per_names_clean
   SET last_name = REGEXP_REPLACE(last_name, '\s*-\s*', '-')
   WHERE last_name IS NOT NULL
   AND REGEXP_LIKE(last_name, '\s*-\s*')
   AND REGEXP_REPLACE(last_name, '\s*-\s*', '-') != last_name;
   
   -- Apostrophes in names (e.g., O'Brien) - ensure proper formatting
   UPDATE per_names_clean
   SET last_name = REGEXP_REPLACE(last_name, '\s*''\s*', '''')
   WHERE last_name IS NOT NULL
   AND REGEXP_LIKE(last_name, '\s*''\s*')
   AND REGEXP_REPLACE(last_name, '\s*''\s*', '''') != last_name;
   
   -- Update names with Mc and Mac prefixes to ensure proper capitalization
   -- For example: Mcdonald -> McDonald, Macintosh -> MacIntosh
   UPDATE per_names_clean
   SET last_name = REGEXP_REPLACE(
                      last_name,
                      '^(Mc)([a-z])(.*)',
                      '\1' || UPPER(SUBSTR(REGEXP_REPLACE(last_name, '^(Mc)([a-z])(.*)', '\2'), 1, 1)) || '\3'
                   )
   WHERE last_name IS NOT NULL
   AND REGEXP_LIKE(last_name, '^Mc[a-z]');
   
   UPDATE per_names_clean
   SET last_name = REGEXP_REPLACE(
                      last_name,
                      '^(Mac)([a-z])(.*)',
                      '\1' || UPPER(SUBSTR(REGEXP_REPLACE(last_name, '^(Mac)([a-z])(.*)', '\2'), 1, 1)) || '\3'
                   )
   WHERE last_name IS NOT NULL
   AND REGEXP_LIKE(last_name, '^Mac[a-z]');
   
   -- Log the pattern cleaning operations
   INSERT INTO per_name_validation_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Clean name patterns', 'NAME_FIELDS', SYSTIMESTAMP, 
        v_first_name_affected + v_middle_names_affected + v_last_name_affected);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Name pattern cleaning complete.');
   DBMS_OUTPUT.PUT_LINE('Total patterns cleaned: ' || 
       (v_first_name_affected + v_middle_names_affected + v_last_name_affected));

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error cleaning name patterns: ' || SQLERRM);
       RAISE;
END clean_name_patterns;
/

-- Create procedure to check and fix name field consistency
CREATE OR REPLACE PROCEDURE check_name_consistency
IS
   v_known_as_updated NUMBER := 0;
   v_preferred_name_updated NUMBER := 0;
BEGIN
   -- Create a column to flag records with name field inconsistencies
   BEGIN
       EXECUTE IMMEDIATE 'ALTER TABLE per_names_clean ADD (has_name_inconsistency VARCHAR2(1) DEFAULT ''N'')';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -1430 THEN  -- Column already exists
               RAISE;
           END IF;
   END;
   
   -- If known_as is empty but preferred_name exists, use preferred_name as known_as
   UPDATE per_names_clean
   SET known_as = preferred_name,
       has_name_inconsistency = 'Y'
   WHERE (known_as IS NULL OR known_as = '')
   AND preferred_name IS NOT NULL
   AND preferred_name != ''
   RETURNING COUNT(*) INTO v_known_as_updated;
   
   -- Log the known_as updates
   IF v_known_as_updated > 0 THEN
       INSERT INTO per_name_validation_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Update empty known_as', 'KNOWN_AS', SYSTIMESTAMP, v_known_as_updated);
   END IF;
   
   -- If preferred_name is empty but known_as exists, use known_as as preferred_name
   UPDATE per_names_clean
   SET preferred_name = known_as,
       has_name_inconsistency = 'Y'
   WHERE (preferred_name IS NULL OR preferred_name = '')
   AND known_as IS NOT NULL
   AND known_as != ''
   RETURNING COUNT(*) INTO v_preferred_name_updated;
   
   -- Log the preferred_name updates
   IF v_preferred_name_updated > 0 THEN
       INSERT INTO per_name_validation_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Update empty preferred_name', 'PREFERRED_NAME', SYSTIMESTAMP, v_preferred_name_updated);
   END IF;
   
   -- Flag records where known_as or preferred_name matches first_name or part of full name
   UPDATE per_names_clean
   SET has_name_inconsistency = 'N'
   WHERE has_name_inconsistency = 'Y'
   AND (
       (known_as = first_name) OR
       (preferred_name = first_name) OR
       (known_as = SUBSTR(first_name, 1, 1) || SUBSTR(last_name, 1)) OR
       (preferred_name = SUBSTR(first_name, 1, 1) || SUBSTR(last_name, 1))
   );
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Name consistency check complete:');
   DBMS_OUTPUT.PUT_LINE('- Known as fields updated: ' || v_known_as_updated);
   DBMS_OUTPUT.PUT_LINE('- Preferred name fields updated: ' || v_preferred_name_updated);

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error checking name consistency: ' || SQLERRM);
       RAISE;
END check_name_consistency;
/

-- Create procedure to validate required name fields
CREATE OR REPLACE PROCEDURE validate_required_names
IS
   v_affected_rows NUMBER := 0;
BEGIN
   -- Create column to track records with missing required fields
   BEGIN
       EXECUTE IMMEDIATE 'ALTER TABLE per_names_clean ADD (has_missing_required VARCHAR2(1) DEFAULT ''N'')';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -1430 THEN  -- Column already exists
               RAISE;
           END IF;
   END;
   
   -- Identify records missing required name fields (first_name and last_name)
   UPDATE per_names_clean
   SET has_missing_required = 'Y'
   WHERE first_name IS NULL
   OR last_name IS NULL
   OR TRIM(first_name) = ''
   OR TRIM(last_name) = ''
   RETURNING COUNT(*) INTO v_affected_rows;
   
   -- Log the validation operation
   INSERT INTO per_name_validation_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Validate required fields', 'NAME_FIELDS', SYSTIMESTAMP, v_affected_rows);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Required name validation complete:');
   DBMS_OUTPUT.PUT_LINE('Records with missing required fields: ' || v_affected_rows);

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error validating required names: ' || SQLERRM);
       RAISE;
END validate_required_names;
/

-- Execution syntax
/*
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
*/
