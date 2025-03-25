-- Purpose of this script is to validate UK National Insurance Numbers (NINOs)
-- in the national_identifier column following the HMRC guidelines
-- https://www.gov.uk/hmrc-internal-manuals/national-insurance-manual/nim39110

-- Drop existing objects first
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE per_nino_validation_log';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP SEQUENCE nino_validation_log_seq';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -2289 THEN RAISE; END IF;
END;
/

-- Create log table with correct structure
CREATE TABLE per_nino_validation_log (
   log_id NUMBER,
   operation VARCHAR2(100),
   field_name VARCHAR2(100),
   operation_date TIMESTAMP,
   records_affected NUMBER
);

-- Create sequence for log_id
CREATE SEQUENCE nino_validation_log_seq
   START WITH 1
   INCREMENT BY 1
   NOCACHE
   NOCYCLE;
   
-- Create trigger for log_id
CREATE OR REPLACE TRIGGER nino_validation_log_bir
BEFORE INSERT ON per_nino_validation_log
FOR EACH ROW
BEGIN
   IF :new.log_id IS NULL THEN
       :new.log_id := nino_validation_log_seq.NEXTVAL;
   END IF;
END;
/

-- This process will be done in four parts:
-- Part 1: Create a working copy of data with relevant columns
-- Part 2: Format and clean the NINOs to standard format
-- Part 3: Validate the NINOs according to HMRC rules
-- Part 4: Report on validity and issues

-- Create procedure to copy table and add validation columns
CREATE OR REPLACE PROCEDURE create_nino_clean_copy
IS
   v_sql VARCHAR2(4000);
BEGIN
   -- Drop the table if it exists
   BEGIN
       v_sql := 'DROP TABLE per_nino_clean';
       EXECUTE IMMEDIATE v_sql;
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -942 THEN  -- Table doesn't exist
               RAISE;
           END IF;
   END;
   
   -- Create new table with selected columns and validation columns
   v_sql := '
       CREATE TABLE per_nino_clean AS
       SELECT
           person_id,
           employee_number,
           national_identifier,
           CAST(''PENDING'' AS VARCHAR2(20)) as nino_validation_status,
           CAST(NULL AS VARCHAR2(4000)) as nino_validation_message,
           SYSDATE as dq_copy
       FROM per_all_people_f
       WHERE national_identifier IS NOT NULL
       AND LENGTH(TRIM(national_identifier)) > 0';
   EXECUTE IMMEDIATE v_sql;

   DBMS_OUTPUT.PUT_LINE('Table per_nino_clean created successfully with ' ||
       TO_CHAR((SELECT COUNT(*) FROM per_nino_clean)) || ' records.');
EXCEPTION
   WHEN OTHERS THEN
       DBMS_OUTPUT.PUT_LINE('Error creating table: ' || SQLERRM);
       RAISE;
END create_nino_clean_copy;
/

-- Create procedure to standardize NINO format (remove spaces and add spaces where needed)
CREATE OR REPLACE PROCEDURE standardize_nino_format
IS
   v_records_affected NUMBER := 0;
BEGIN
   -- First, remove all spaces and convert to uppercase
   UPDATE per_nino_clean
   SET national_identifier = UPPER(REPLACE(TRIM(national_identifier), ' ', ''))
   WHERE national_identifier IS NOT NULL
   RETURNING COUNT(*) INTO v_records_affected;
   
   -- Log the formatting operation
   INSERT INTO per_nino_validation_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Remove spaces', 'NATIONAL_IDENTIFIER', SYSTIMESTAMP, v_records_affected);
   
   -- Format as standard NINO with a space after first two letters
   -- Only do this for NINOs that look like they might be valid (correct length)
   UPDATE per_nino_clean
   SET national_identifier = 
       SUBSTR(national_identifier, 1, 2) || ' ' ||
       SUBSTR(national_identifier, 3)
   WHERE LENGTH(national_identifier) = 9
   AND REGEXP_LIKE(national_identifier, '^[A-Z]{2}[0-9]{6}[A-Z]$');
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('NINO format standardization complete');
   DBMS_OUTPUT.PUT_LINE('Records affected: ' || v_records_affected);

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error standardizing NINO format: ' || SQLERRM);
       RAISE;
END standardize_nino_format;
/

-- Create procedure to validate NINOs based on HMRC rules
CREATE OR REPLACE PROCEDURE validate_nino
IS
   v_valid_count NUMBER := 0;
   v_invalid_count NUMBER := 0;
   v_administrative_count NUMBER := 0;
   v_temporary_count NUMBER := 0;
BEGIN
   -- Define the basic NINO pattern (2 letters, 6 numbers, 1 letter)
   -- Format should be: XX 999999X
   
   -- Mark records with completely invalid format
   UPDATE per_nino_clean
   SET nino_validation_status = 'INVALID',
       nino_validation_message = 'Invalid format - must be 2 letters, 6 numbers, 1 letter'
   WHERE NOT REGEXP_LIKE(national_identifier, '^[A-Z]{2} [0-9]{6}[A-Z]$')
   RETURNING COUNT(*) INTO v_invalid_count;
   
   -- Check for invalid first letter (D, F, I, Q, U, V not allowed)
   UPDATE per_nino_clean
   SET nino_validation_status = 'INVALID',
       nino_validation_message = 'Invalid prefix - first letter cannot be D, F, I, Q, U, or V'
   WHERE nino_validation_status = 'PENDING'
   AND SUBSTR(national_identifier, 1, 1) IN ('D', 'F', 'I', 'Q', 'U', 'V')
   RETURNING COUNT(*) INTO v_invalid_count;
   
   -- Check for invalid second letter (D, F, I, O, Q, U, V not allowed)
   UPDATE per_nino_clean
   SET nino_validation_status = 'INVALID',
       nino_validation_message = 'Invalid prefix - second letter cannot be D, F, I, O, Q, U, or V'
   WHERE nino_validation_status = 'PENDING'
   AND SUBSTR(national_identifier, 2, 1) IN ('D', 'F', 'I', 'O', 'Q', 'U', 'V')
   RETURNING COUNT(*) INTO v_invalid_count;
   
   -- Check for invalid prefix combinations (BG, GB, KN, NK, NT, TN, ZZ not allowed)
   UPDATE per_nino_clean
   SET nino_validation_status = 'INVALID',
       nino_validation_message = 'Invalid prefix combination - BG, GB, KN, NK, NT, TN, ZZ not allowed'
   WHERE nino_validation_status = 'PENDING'
   AND SUBSTR(national_identifier, 1, 2) IN ('BG', 'GB', 'KN', 'NK', 'NT', 'TN', 'ZZ')
   RETURNING COUNT(*) INTO v_invalid_count;
   
   -- Check for administrative numbers
   UPDATE per_nino_clean
   SET nino_validation_status = 'ADMINISTRATIVE',
       nino_validation_message = 'Administrative number - not a valid NINO for benefits'
   WHERE nino_validation_status = 'PENDING'
   AND (
       SUBSTR(national_identifier, 1, 2) = 'OO' OR -- Tax Credits admin
       SUBSTR(national_identifier, 1, 2) = 'FY' OR -- Attendance Allowance
       SUBSTR(national_identifier, 1, 2) = 'NC' OR -- Stakeholder Pensions (pre-2006)
       (SUBSTR(national_identifier, 1, 2) = 'PP' AND SUBSTR(national_identifier, 4, 6) = '999999') OR -- Pension scheme
       SUBSTR(national_identifier, 1, 2) IN ('PZ', 'PY') -- Tax-only cases pre-2002
   )
   RETURNING COUNT(*) INTO v_administrative_count;
   
   -- Check for temporary numbers (TN - no longer used)
   UPDATE per_nino_clean
   SET nino_validation_status = 'TEMPORARY',
       nino_validation_message = 'Temporary number (TN) - no longer permitted'
   WHERE nino_validation_status = 'PENDING'
   AND SUBSTR(national_identifier, 1, 2) = 'TN'
   RETURNING COUNT(*) INTO v_temporary_count;
   
   -- Check for valid suffix (A, B, C, D only)
   UPDATE per_nino_clean
   SET nino_validation_status = 'INVALID',
       nino_validation_message = 'Invalid suffix - must be A, B, C, or D'
   WHERE nino_validation_status = 'PENDING'
   AND SUBSTR(national_identifier, 10, 1) NOT IN ('A', 'B', 'C', 'D')
   RETURNING COUNT(*) INTO v_invalid_count;
   
   -- Mark remaining records as valid
   UPDATE per_nino_clean
   SET nino_validation_status = 'VALID',
       nino_validation_message = 'Valid UK NINO'
   WHERE nino_validation_status = 'PENDING'
   RETURNING COUNT(*) INTO v_valid_count;
   
   -- Log the validation results
   INSERT INTO per_nino_validation_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Validate NINO', 'NATIONAL_IDENTIFIER', SYSTIMESTAMP, 
        v_valid_count + v_invalid_count + v_administrative_count + v_temporary_count);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('NINO validation complete:');
   DBMS_OUTPUT.PUT_LINE('- Valid NINOs: ' || v_valid_count);
   DBMS_OUTPUT.PUT_LINE('- Invalid NINOs: ' || v_invalid_count);
   DBMS_OUTPUT.PUT_LINE('- Administrative numbers: ' || v_administrative_count);
   DBMS_OUTPUT.PUT_LINE('- Temporary numbers: ' || v_temporary_count);

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error validating NINOs: ' || SQLERRM);
       RAISE;
END validate_nino;
/

-- Create procedure to check for potential typos or common mistakes
CREATE OR REPLACE PROCEDURE check_nino_typos
IS
   v_typo_count NUMBER := 0;
BEGIN
   -- Check for common typos like letter O instead of zero
   UPDATE per_nino_clean
   SET nino_validation_message = nino_validation_message || ' (Possible typo: letter O instead of zero)'
   WHERE nino_validation_status = 'INVALID'
   AND REGEXP_LIKE(national_identifier, '[A-Z]{2} [0-9O]{6}[A-Z]')
   AND REGEXP_LIKE(national_identifier, 'O')
   RETURNING COUNT(*) INTO v_typo_count;
   
   -- Check for S instead of 5
   UPDATE per_nino_clean
   SET nino_validation_message = nino_validation_message || ' (Possible typo: letter S instead of 5)'
   WHERE nino_validation_status = 'INVALID'
   AND REGEXP_LIKE(national_identifier, '[A-Z]{2} [0-9S]{6}[A-Z]')
   AND REGEXP_LIKE(national_identifier, 'S')
   RETURNING COUNT(*) INTO v_typo_count;
   
   -- Check for I instead of 1
   UPDATE per_nino_clean
   SET nino_validation_message = nino_validation_message || ' (Possible typo: letter I instead of 1)'
   WHERE nino_validation_status = 'INVALID'
   AND REGEXP_LIKE(national_identifier, '[A-Z]{2} [0-9I]{6}[A-Z]')
   AND REGEXP_LIKE(national_identifier, 'I')
   RETURNING COUNT(*) INTO v_typo_count;
   
   -- Log the typo check results
   INSERT INTO per_nino_validation_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Check NINO typos', 'NATIONAL_IDENTIFIER', SYSTIMESTAMP, v_typo_count);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('NINO typo check complete:');
   DBMS_OUTPUT.PUT_LINE('- Potential typos identified: ' || v_typo_count);

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error checking NINO typos: ' || SQLERRM);
       RAISE;
END check_nino_typos;
/

-- Create function to suggest fix for common typos
CREATE OR REPLACE FUNCTION suggest_nino_fix(p_nino VARCHAR2)
RETURN VARCHAR2
IS
   v_suggested_nino VARCHAR2(11);
BEGIN
   -- Make a copy of the input
   v_suggested_nino := p_nino;
   
   -- Replace letter O with digit 0 in the numeric part
   v_suggested_nino := REGEXP_REPLACE(
      v_suggested_nino, 
      '([A-Z]{2} )([0-9O]{6})([A-Z])', 
      '\1' || REPLACE(REGEXP_SUBSTR(v_suggested_nino, '([A-Z]{2} )([0-9O]{6})([A-Z])', 1, 1, NULL, 2), 'O', '0') || '\3'
   );
   
   -- Replace letter S with digit 5
   v_suggested_nino := REGEXP_REPLACE(
      v_suggested_nino, 
      '([A-Z]{2} )([0-9S]{6})([A-Z])', 
      '\1' || REPLACE(REGEXP_SUBSTR(v_suggested_nino, '([A-Z]{2} )([0-9S]{6})([A-Z])', 1, 1, NULL, 2), 'S', '5') || '\3'
   );
   
   -- Replace letter I with digit 1
   v_suggested_nino := REGEXP_REPLACE(
      v_suggested_nino, 
      '([A-Z]{2} )([0-9I]{6})([A-Z])', 
      '\1' || REPLACE(REGEXP_SUBSTR(v_suggested_nino, '([A-Z]{2} )([0-9I]{6})([A-Z])', 1, 1, NULL, 2), 'I', '1') || '\3'
   );
   
   -- If the input and output are the same, no changes were made
   IF v_suggested_nino = p_nino THEN
      RETURN NULL;
   ELSE
      RETURN v_suggested_nino;
   END IF;
END suggest_nino_fix;
/

-- Create procedure to add suggested fixes to the table
CREATE OR REPLACE PROCEDURE add_nino_suggestions
IS
   v_suggestion_count NUMBER := 0;
   v_sql VARCHAR2(4000);
   v_suggested_nino VARCHAR2(11);
BEGIN
   -- Add column for suggested fixes if it doesn't exist
   BEGIN
       v_sql := 'ALTER TABLE per_nino_clean ADD (suggested_nino VARCHAR2(11))';
       EXECUTE IMMEDIATE v_sql;
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -1430 THEN  -- Column already exists
               RAISE;
           END IF;
   END;
   
   -- Add suggestions for invalid NINOs using a cursor
   FOR r IN (
       SELECT person_id, national_identifier
       FROM per_nino_clean
       WHERE nino_validation_status = 'INVALID'
   ) LOOP
       BEGIN
           -- Get the suggested fix first
           v_suggested_nino := suggest_nino_fix(r.national_identifier);
           
           -- Only update if we have a valid suggestion
           IF v_suggested_nino IS NOT NULL THEN
               v_sql := 'UPDATE per_nino_clean 
                         SET suggested_nino = :1 
                         WHERE person_id = :2 
                         AND national_identifier = :3';
               EXECUTE IMMEDIATE v_sql 
               USING v_suggested_nino, r.person_id, r.national_identifier;
               
               -- Only increment counter if update was successful
               IF SQL%ROWCOUNT > 0 THEN
                   v_suggestion_count := v_suggestion_count + 1;
               END IF;
           END IF;
       EXCEPTION
           WHEN OTHERS THEN
               DBMS_OUTPUT.PUT_LINE('Error processing NINO for person_id ' || r.person_id || ': ' || SQLERRM);
               -- Continue with next record
               CONTINUE;
       END;
   END LOOP;
   
   -- Log the suggestion results
   INSERT INTO per_nino_validation_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Add NINO suggestions', 'NATIONAL_IDENTIFIER', SYSTIMESTAMP, v_suggestion_count);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('NINO suggestions complete:');
   DBMS_OUTPUT.PUT_LINE('- Suggestions added: ' || v_suggestion_count);

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error adding NINO suggestions: ' || SQLERRM);
       RAISE;
END add_nino_suggestions;
/

-- Execution syntax
/*
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
*/

-- Add table comments after all objects are created
COMMENT ON TABLE per_nino_validation_log IS 'Log table tracking all NINO validation operations';
COMMENT ON COLUMN per_nino_validation_log.log_id IS 'Unique identifier for the log entry';
COMMENT ON COLUMN per_nino_validation_log.operation IS 'Type of validation operation performed';
COMMENT ON COLUMN per_nino_validation_log.field_name IS 'Name of the field being validated';
COMMENT ON COLUMN per_nino_validation_log.operation_date IS 'Timestamp when the operation was performed';
COMMENT ON COLUMN per_nino_validation_log.records_affected IS 'Number of records affected by the operation';

COMMENT ON TABLE per_nino_clean IS 'Cleansed version of NINOs with validation flags';
COMMENT ON COLUMN per_nino_clean.person_id IS 'Unique identifier for the person';
COMMENT ON COLUMN per_nino_clean.employee_number IS 'Employee number for the person';
COMMENT ON COLUMN per_nino_clean.national_identifier IS 'The NINO value';
COMMENT ON COLUMN per_nino_clean.nino_validation_status IS 'Status of NINO validation (VALID, INVALID, ADMINISTRATIVE, TEMPORARY)';
COMMENT ON COLUMN per_nino_clean.nino_validation_message IS 'Detailed message explaining validation result';
COMMENT ON COLUMN per_nino_clean.dq_copy IS 'Timestamp when the record was copied for cleansing';
COMMENT ON COLUMN per_nino_clean.suggested_nino IS 'Suggested corrected version of invalid NINO';
