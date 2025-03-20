-- Purpose of this script is to validate and cleanse UK address data in HR core data
-- following the same structure as the name cleansing validations.
-- table is called "per_addresses" for employee addresses.
-- This validation is specifically for United Kingdom addresses.

-- Drop existing objects first
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE per_addresses_clean_log';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP SEQUENCE addr_clean_log_seq';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -2289 THEN RAISE; END IF;
END;
/

-- Create log table with correct structure
CREATE TABLE per_addresses_clean_log (
   log_id NUMBER,
   operation VARCHAR2(100),
   field_name VARCHAR2(100),
   operation_date TIMESTAMP,
   records_affected NUMBER
);

-- Create sequence for log_id
CREATE SEQUENCE addr_clean_log_seq
   START WITH 1
   INCREMENT BY 1
   NOCACHE
   NOCYCLE;
   
-- Create trigger for log_id
CREATE OR REPLACE TRIGGER addr_clean_log_bir
BEFORE INSERT ON per_addresses_clean_log
FOR EACH ROW
BEGIN
   IF :new.log_id IS NULL THEN
       :new.log_id := addr_clean_log_seq.NEXTVAL;
   END IF;
END;
/

-- This process will be done in three parts:
-- Part 1
-- 1. Create a copy of the UK address data called "per_addresses_clean"
-- 2. Select columns that are needed from the table "per_addresses" where country = 'GB'
-- Part 2
-- 3. Standardize UK county names
-- 4. Validate and standardize UK postcodes using official GOV.UK regex
-- 5. Cleanse address lines from leading/trailing spaces
-- Part 3
-- 6. Identify incomplete UK addresses
-- ** All changes are logged in the table "per_addresses_clean_log"
-- ** All changes are done on the per_addresses_clean table

-- Create procedure to copy table and add dq_copy column
CREATE OR REPLACE PROCEDURE create_address_clean_copy
IS
BEGIN
   -- Drop the table if it exists
   BEGIN
       EXECUTE IMMEDIATE 'DROP TABLE per_addresses_clean';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -942 THEN  -- Table doesn't exist
               RAISE;
           END IF;
   END;

   -- Create new table with selected columns and dq_copy timestamp
   EXECUTE IMMEDIATE '
       CREATE TABLE per_addresses_clean AS
       SELECT
           address_id,
           person_id,
           address_line1,
           address_line2,
           address_line3,
           town_or_city,
           region_1,    -- County
           region_2,    -- District
           postal_code,
           country,
           primary_flag,
           SYSDATE as dq_copy
       FROM per_addresses
       WHERE country = 'GB';

   DBMS_OUTPUT.PUT_LINE('Table per_addresses_clean created successfully with ' ||
       TO_CHAR((SELECT COUNT(*) FROM per_addresses_clean)) || ' records.');
EXCEPTION
   WHEN OTHERS THEN
       DBMS_OUTPUT.PUT_LINE('Error creating table: ' || SQLERRM);
       RAISE;
END create_address_clean_copy;
/

-- Create procedure to standardize UK counties
CREATE OR REPLACE PROCEDURE standardize_uk_counties
IS
   v_affected_rows NUMBER := 0;
BEGIN
   -- Update common county name variations to standard format
   -- Example: Update various forms of 'Greater London' to standard format
   UPDATE per_addresses_clean
   SET region_1 = 'Greater London'
   WHERE region_1 IN ('London', 'LONDON', 'G London', 'G. London', 'Greater London Area')
   RETURNING COUNT(*) INTO v_affected_rows;

   -- Log the standardization operation
   IF v_affected_rows > 0 THEN
       INSERT INTO per_addresses_clean_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Standardize county', 'REGION_1', SYSTIMESTAMP, v_affected_rows);
   END IF;

   -- Example: Yorkshire variations
   UPDATE per_addresses_clean
   SET region_1 = 'West Yorkshire'
   WHERE region_1 IN ('W. Yorkshire', 'W Yorkshire', 'WEST YORKSHIRE', 'W.Yorkshire')
   RETURNING COUNT(*) INTO v_affected_rows;

   -- Log the standardization operation
   IF v_affected_rows > 0 THEN
       INSERT INTO per_addresses_clean_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Standardize county', 'REGION_1', SYSTIMESTAMP, v_affected_rows);
   END IF;
   
   -- Example: Lancashire variations
   UPDATE per_addresses_clean
   SET region_1 = 'Lancashire'
   WHERE region_1 IN ('Lancs', 'LANCS', 'Lancs.', 'LANCASHIRE')
   RETURNING COUNT(*) INTO v_affected_rows;

   -- Log the standardization operation
   IF v_affected_rows > 0 THEN
       INSERT INTO per_addresses_clean_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Standardize county', 'REGION_1', SYSTIMESTAMP, v_affected_rows);
   END IF;

   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Standardized UK counties complete.');

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error standardizing UK counties: ' || SQLERRM);
       RAISE;
END standardize_uk_counties;
/

-- Create procedure to validate and standardize UK postcodes
CREATE OR REPLACE PROCEDURE validate_uk_postcodes
IS
   v_affected_invalid NUMBER := 0;
   v_affected_format NUMBER := 0;
   v_uk_regex VARCHAR2(500) := '^([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9]?[A-Za-z])))) [0-9][A-Za-z]{2})$';
BEGIN
   -- Create a new column to mark invalid UK postcodes
   BEGIN
       EXECUTE IMMEDIATE 'ALTER TABLE per_addresses_clean ADD (is_valid_uk_postcode VARCHAR2(1) DEFAULT ''Y'')';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -1430 THEN  -- Column already exists
               RAISE;
           END IF;
   END;
   
   -- Validate UK postcodes using the official GOV.UK regex pattern
   -- First mark invalid postcodes
   UPDATE per_addresses_clean
   SET is_valid_uk_postcode = 'N'
   WHERE postal_code IS NOT NULL
   AND NOT REGEXP_LIKE(TRIM(postal_code), v_uk_regex)
   RETURNING COUNT(*) INTO v_affected_invalid;
   
   -- Log the UK postcode validation
   IF v_affected_invalid > 0 THEN
       INSERT INTO per_addresses_clean_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Mark invalid UK postcodes', 'POSTAL_CODE', SYSTIMESTAMP, v_affected_invalid);
   END IF;
   
   -- For valid UK postcodes, ensure consistent formatting (uppercase with proper spacing)
   UPDATE per_addresses_clean
   SET postal_code = REGEXP_REPLACE(
                       UPPER(REGEXP_REPLACE(TRIM(postal_code), '\s+', ' ')),
                       '(^[A-Z]{1,2}[0-9][A-Z0-9]?) ([0-9][A-Z]{2}$)',
                       '\1 \2'
                     )
   WHERE postal_code IS NOT NULL
   AND is_valid_uk_postcode = 'Y'
   AND REGEXP_REPLACE(
           UPPER(REGEXP_REPLACE(TRIM(postal_code), '\s+', ' ')),
           '(^[A-Z]{1,2}[0-9][A-Z0-9]?) ([0-9][A-Z]{2}$)',
           '\1 \2'
       ) != postal_code
   RETURNING COUNT(*) INTO v_affected_format;
   
   -- Log the formatting operation
   IF v_affected_format > 0 THEN
       INSERT INTO per_addresses_clean_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Format UK postcodes', 'POSTAL_CODE', SYSTIMESTAMP, v_affected_format);
   END IF;
   
   -- Handle common formatting issues (missing space)
   UPDATE per_addresses_clean
   SET postal_code = REGEXP_REPLACE(
                       UPPER(TRIM(postal_code)),
                       '^([A-Z]{1,2}[0-9][A-Z0-9]?)([0-9][A-Z]{2})$', 
                       '\1 \2'
                     ),
       is_valid_uk_postcode = 'Y'
   WHERE postal_code IS NOT NULL
   AND is_valid_uk_postcode = 'N'
   AND REGEXP_LIKE(
          UPPER(TRIM(postal_code)),
          '^([A-Z]{1,2}[0-9][A-Z0-9]?)([0-9][A-Z]{2})$'
       );

   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Identified ' || v_affected_invalid || ' invalid UK postcodes.');
   DBMS_OUTPUT.PUT_LINE('Standardized format for ' || v_affected_format || ' UK postcodes.');

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error validating UK postcodes: ' || SQLERRM);
       RAISE;
END validate_uk_postcodes;
/

-- Create procedure to clean UK address lines from spaces and standardize formatting
CREATE OR REPLACE PROCEDURE clean_address_spaces
IS
   v_addr1_affected NUMBER := 0;
   v_addr2_affected NUMBER := 0;
   v_city_affected NUMBER := 0;
BEGIN
   -- Clean address_line1 column
   UPDATE per_addresses_clean
   SET address_line1 = TRIM(REGEXP_REPLACE(address_line1, '\s{2,}', ' '))
   WHERE address_line1 IS NOT NULL
   AND (address_line1 LIKE ' %' OR address_line1 LIKE '% ' OR address_line1 LIKE '%  %')
   RETURNING COUNT(*) INTO v_addr1_affected;

   -- Log address_line1 changes
   IF v_addr1_affected > 0 THEN
       INSERT INTO per_addresses_clean_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Remove spaces', 'ADDRESS_LINE1', SYSTIMESTAMP, v_addr1_affected);
   END IF;

   -- Clean address_line2 column
   UPDATE per_addresses_clean
   SET address_line2 = TRIM(REGEXP_REPLACE(address_line2, '\s{2,}', ' '))
   WHERE address_line2 IS NOT NULL
   AND (address_line2 LIKE ' %' OR address_line2 LIKE '% ' OR address_line2 LIKE '%  %')
   RETURNING COUNT(*) INTO v_addr2_affected;

   -- Log address_line2 changes
   IF v_addr2_affected > 0 THEN
       INSERT INTO per_addresses_clean_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Remove spaces', 'ADDRESS_LINE2', SYSTIMESTAMP, v_addr2_affected);
   END IF;

   -- Clean town_or_city column
   UPDATE per_addresses_clean
   SET town_or_city = TRIM(REGEXP_REPLACE(town_or_city, '\s{2,}', ' '))
   WHERE town_or_city IS NOT NULL
   AND (town_or_city LIKE ' %' OR town_or_city LIKE '% ' OR town_or_city LIKE '%  %')
   RETURNING COUNT(*) INTO v_city_affected;

   -- Log town_or_city changes
   IF v_city_affected > 0 THEN
       INSERT INTO per_addresses_clean_log
           (operation, field_name, operation_date, records_affected)
       VALUES
           ('Remove spaces', 'TOWN_OR_CITY', SYSTIMESTAMP, v_city_affected);
   END IF;

   COMMIT;
   
   -- Output detailed results
   DBMS_OUTPUT.PUT_LINE('Address space cleaning complete:');
   DBMS_OUTPUT.PUT_LINE('- Address Line 1 cleaned: ' || v_addr1_affected);
   DBMS_OUTPUT.PUT_LINE('- Address Line 2 cleaned: ' || v_addr2_affected);
   DBMS_OUTPUT.PUT_LINE('- Town/City cleaned: ' || v_city_affected);
   DBMS_OUTPUT.PUT_LINE('Total UK address records affected: ' ||
       (v_addr1_affected + v_addr2_affected + v_city_affected));

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error cleaning address spaces: ' || SQLERRM);
       RAISE;
END clean_address_spaces;
/

-- Create procedure to identify incomplete UK addresses
CREATE OR REPLACE PROCEDURE identify_incomplete_addresses
IS
   v_affected_rows NUMBER := 0;
BEGIN
   -- Create a new column to mark incomplete addresses
   BEGIN
       EXECUTE IMMEDIATE 'ALTER TABLE per_addresses_clean ADD (is_complete VARCHAR2(1) DEFAULT ''Y'')';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -1430 THEN  -- Column already exists
               RAISE;
           END IF;
   END;

   -- Identify incomplete addresses (missing critical fields for UK addresses)
   UPDATE per_addresses_clean
   SET is_complete = 'N'
   WHERE (address_line1 IS NULL OR town_or_city IS NULL OR postal_code IS NULL OR
          NOT is_valid_uk_postcode = 'Y')
   RETURNING COUNT(*) INTO v_affected_rows;

   -- Log the incomplete identification operation
   INSERT INTO per_addresses_clean_log
       (operation, field_name, operation_date, records_affected)
   VALUES
       ('Identify incomplete UK addresses', 'ADDRESS', SYSTIMESTAMP, v_affected_rows);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('Identified ' || v_affected_rows || ' incomplete UK addresses.');

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error identifying incomplete addresses: ' || SQLERRM);
       RAISE;
END identify_incomplete_addresses;
/

-- Execution syntax
/*
-- Create a copy of the table with today's date
EXEC create_address_clean_copy;

-- Run validation procedures
EXEC standardize_uk_counties;
EXEC validate_uk_postcodes;
EXEC clean_address_spaces;
EXEC identify_incomplete_addresses;

-- View the log by field:
SELECT field_name, operation, records_affected, operation_date
FROM per_addresses_clean_log
ORDER BY operation_date;

-- View summary by field:
SELECT field_name, SUM(records_affected) as total_records_cleaned
FROM per_addresses_clean_log
GROUP BY field_name;

-- Show incomplete UK addresses
SELECT 
    address_id,
    person_id,
    address_line1,
    town_or_city,
    region_1 AS county,
    postal_code
FROM per_addresses_clean
WHERE is_complete = 'N'
ORDER BY person_id;

-- Show invalid UK postcodes
SELECT
    address_id,
    person_id,
    postal_code,
    address_line1,
    town_or_city
FROM per_addresses_clean
WHERE is_valid_uk_postcode = 'N'
ORDER BY person_id;
*/
