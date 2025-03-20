-- Purpose of this script is to validate and cleanse email addresses in HR core data
-- following the same structure as the name cleansing validations.
-- table is called "per_all_people_f" with email data in "per_email_addresses".

-- Drop existing objects first
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE per_email_addresses_clean_log';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP SEQUENCE email_clean_log_seq';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -2289 THEN RAISE; END IF;
END;
/

-- Create log table with correct structure
CREATE TABLE per_email_addresses_clean_log (
   log_id NUMBER,
   operation VARCHAR2(100),
   field_name VARCHAR2(100),
   operation_date TIMESTAMP,
   records_affected NUMBER
);

-- Create sequence for log_id
CREATE SEQUENCE email_clean_log_seq
   START WITH 1
   INCREMENT BY 1
   NOCACHE
   NOCYCLE;
   
-- Create trigger for log_id
CREATE OR REPLACE TRIGGER email_clean_log_bir
BEFORE INSERT ON per_email_addresses_clean_log
FOR EACH ROW
BEGIN
   IF :new.log_id IS NULL THEN
       :new.log_id := email_clean_log_seq.NEXTVAL;
   END IF;
END;
/

-- This process will be done in three parts:
-- Part 1
-- 1. Create a copy of the email data called "per_email_addresses_clean"
-- 2. Select columns that are needed from the table "per_email_addresses" 
-- Part 2
-- 3. Validate email format using regex pattern
-- 4. Standardize email domains for company emails
-- Part 3
-- 5. Identify and mark duplicate email addresses
-- ** All changes are logged in the table "per_email_addresses_clean_log"
-- ** All changes are done on the per_email_addresses_clean table

-- Create procedure to copy table and add dq_copy column
CREATE OR REPLACE PROCEDURE create_email_clean_copy
IS
BEGIN
   -- Drop the table if it exists
   BEGIN
       EXECUTE IMMEDIATE 'DROP TABLE per_email_addresses_clean';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -942 THEN  -- Table doesn't exist
               RAISE;
           END IF;
   END;

   -- Create new table with selected columns and dq_copy timestamp
   EXECUTE IMMEDIATE '
       CREATE TABLE per_email_addresses_clean AS
       SELECT
           email_address_id,
           person_id,
           email_address,
           email_type,
           primary_flag,
           date_from,
           date_to,
           SYSDATE as dq_copy
       FROM per_email_addresses';

   DBMS_OUTPUT.PUT_LINE('Table per_email_addresses_clean created successfully with ' ||
       TO_CHAR((SELECT COUNT(*) FROM per_email_addresses_clean)) || ' records.');
EXCEPTION
   WHEN OTHERS THEN
       DBMS_OUTPUT.PUT_LINE('Error creating table: ' || SQLERRM);
       RAISE;
END create_email_clean_copy;
/

-- Create procedure to validate email format
CREATE OR REPLACE PROCEDURE validate_email_format
IS
   v_affected_rows NUMBER := 0;
BEGIN
   -- Create a new column to mark invalid emails
   BEGIN
       EXECUTE IMMEDIATE 'ALTER TABLE per_email_addresses_clean ADD (is_valid_format VARCHAR2(1) DEFAULT ''Y'')';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -1430 THEN  -- Column already exists
               RAISE;
           END IF;
   END;

   -- Update records with invalid email format
   UPDATE per_email_addresses_clean
   SET is_valid_format = 'N'
   WHERE email_address IS NOT NULL
   AND NOT REGEXP_LIKE(email_address, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
   RETURNING COUNT(*) INTO v_affected_rows;

   -- Log the validation operation
   INSERT INTO per_email_addresses_clean_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Validate format', 'EMAIL_ADDRESS', SYSTIMESTAMP, v_affected_rows);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Identified ' || v_affected_rows || ' records with invalid email format.');

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error validating email format: ' || SQLERRM);
       RAISE;
END validate_email_format;
/

-- Create procedure to standardize company email domains
CREATE OR REPLACE PROCEDURE standardize_company_domains
IS
   v_affected_rows NUMBER := 0;
   v_standard_domain VARCHAR2(100) := 'company.com';  -- Replace with actual company domain
BEGIN
   -- Standardize company email domains (example: correct variations like company.org to company.com)
   UPDATE per_email_addresses_clean
   SET email_address = REGEXP_REPLACE(
                           email_address, 
                           '@company\.(org|net|co|io)', 
                           '@' || v_standard_domain
                       )
   WHERE email_address IS NOT NULL
   AND REGEXP_LIKE(email_address, '@company\.(org|net|co|io)$')
   RETURNING COUNT(*) INTO v_affected_rows;

   -- Log the standardization operation
   IF v_affected_rows > 0 THEN
       INSERT INTO per_email_addresses_clean_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Standardize domain', 'EMAIL_ADDRESS', SYSTIMESTAMP, v_affected_rows);
   END IF;

   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Standardized ' || v_affected_rows || ' company email domains.');

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error standardizing company domains: ' || SQLERRM);
       RAISE;
END standardize_company_domains;
/

-- Create procedure to identify duplicate emails
CREATE OR REPLACE PROCEDURE identify_duplicate_emails
IS
   v_affected_rows NUMBER := 0;
BEGIN
   -- Create a new column to mark duplicate emails
   BEGIN
       EXECUTE IMMEDIATE 'ALTER TABLE per_email_addresses_clean ADD (is_duplicate VARCHAR2(1) DEFAULT ''N'')';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -1430 THEN  -- Column already exists
               RAISE;
           END IF;
   END;

   -- Identify duplicate email addresses
   UPDATE per_email_addresses_clean e1
   SET is_duplicate = 'Y'
   WHERE EXISTS (
       SELECT 1
       FROM per_email_addresses_clean e2
       WHERE e1.email_address = e2.email_address
       AND e1.person_id != e2.person_id
       AND e1.email_address IS NOT NULL
   )
   RETURNING COUNT(*) INTO v_affected_rows;

   -- Log the duplicate identification operation
   INSERT INTO per_email_addresses_clean_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Identify duplicates', 'EMAIL_ADDRESS', SYSTIMESTAMP, v_affected_rows);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Identified ' || v_affected_rows || ' duplicate email addresses.');

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error identifying duplicate emails: ' || SQLERRM);
       RAISE;
END identify_duplicate_emails;
/

-- Execution syntax
/*
-- Create a copy of the table with today's date
EXEC create_email_clean_copy;

-- Run validation procedures
EXEC validate_email_format;
EXEC standardize_company_domains;
EXEC identify_duplicate_emails;

-- View the log by field:
SELECT field_name, operation, records_affected, operation_date
FROM per_email_addresses_clean_log
ORDER BY operation_date;

-- View summary by field:
SELECT field_name, SUM(records_affected) as total_records_cleaned
FROM per_email_addresses_clean_log
GROUP BY field_name;

-- Show invalid format emails
SELECT 
    email_address_id,
    person_id,
    email_address,
    email_type
FROM per_email_addresses_clean
WHERE is_valid_format = 'N';

-- Show duplicate emails
SELECT 
    email_address_id,
    person_id,
    email_address,
    email_type
FROM per_email_addresses_clean
WHERE is_duplicate = 'Y'
ORDER BY email_address;
*/
