-- Purpose of this script is to create a data quality review table
-- that masks personal information for privacy while providing
-- enough data for DQ review purposes

-- Drop existing objects first
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE dq_review';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
BEGIN
   EXECUTE IMMEDIATE 'DROP SEQUENCE dq_review_log_seq';
EXCEPTION
   WHEN OTHERS THEN
       IF SQLCODE != -2289 THEN RAISE; END IF;
END;
/

-- Create log table with correct structure
CREATE TABLE dq_review_log (
   log_id NUMBER,
   operation VARCHAR2(100),
   operation_date TIMESTAMP,
   records_affected NUMBER
);

-- Create sequence for log_id
CREATE SEQUENCE dq_review_log_seq
   START WITH 1
   INCREMENT BY 1
   NOCACHE
   NOCYCLE;
   
-- Create trigger for log_id
CREATE OR REPLACE TRIGGER dq_review_log_bir
BEFORE INSERT ON dq_review_log
FOR EACH ROW
BEGIN
   IF :new.log_id IS NULL THEN
       :new.log_id := dq_review_log_seq.NEXTVAL;
   END IF;
END;
/

-- Create procedure to generate the DQ review table
CREATE OR REPLACE PROCEDURE generate_dq_review_table
IS
   v_record_count NUMBER;
   v_table_exists NUMBER;
   v_missing_tables VARCHAR2(4000);
BEGIN
   -- Initialize variables
   v_record_count := 0;
   v_table_exists := 0;
   v_missing_tables := '';
   
   -- Check if source tables exist
   BEGIN
       EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM hr.per_all_people_f' INTO v_table_exists;
   EXCEPTION
       WHEN OTHERS THEN
           v_missing_tables := v_missing_tables || 'hr.per_all_people_f, ';
   END;

   BEGIN
       EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM hr.per_person_type_usages_f' INTO v_table_exists;
   EXCEPTION
       WHEN OTHERS THEN
           v_missing_tables := v_missing_tables || 'hr.per_person_type_usages_f, ';
   END;

   BEGIN
       EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM hr.per_person_types' INTO v_table_exists;
   EXCEPTION
       WHEN OTHERS THEN
           v_missing_tables := v_missing_tables || 'hr.per_person_types, ';
   END;

   -- Check if validation tables exist
   BEGIN
       EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM per_names_clean' INTO v_table_exists;
   EXCEPTION
       WHEN OTHERS THEN
           v_missing_tables := v_missing_tables || 'per_names_clean, ';
   END;

   BEGIN
       EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM per_email_addresses_clean' INTO v_table_exists;
   EXCEPTION
       WHEN OTHERS THEN
           v_missing_tables := v_missing_tables || 'per_email_addresses_clean, ';
   END;

   BEGIN
       EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM per_dob_clean' INTO v_table_exists;
   EXCEPTION
       WHEN OTHERS THEN
           v_missing_tables := v_missing_tables || 'per_dob_clean, ';
   END;

   BEGIN
       EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM per_nino_clean' INTO v_table_exists;
   EXCEPTION
       WHEN OTHERS THEN
           v_missing_tables := v_missing_tables || 'per_nino_clean, ';
   END;

   BEGIN
       EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM clean_addresses' INTO v_table_exists;
   EXCEPTION
       WHEN OTHERS THEN
           v_missing_tables := v_missing_tables || 'clean_addresses, ';
   END;

   BEGIN
       EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM address_validation_results' INTO v_table_exists;
   EXCEPTION
       WHEN OTHERS THEN
           v_missing_tables := v_missing_tables || 'address_validation_results, ';
   END;

   -- If any tables are missing, raise an error with details
   IF v_missing_tables IS NOT NULL THEN
       v_missing_tables := RTRIM(v_missing_tables, ', ');
       DBMS_OUTPUT.PUT_LINE('Error: The following tables do not exist or are not accessible:');
       DBMS_OUTPUT.PUT_LINE(v_missing_tables);
       DBMS_OUTPUT.PUT_LINE('Please ensure all required tables exist before running this procedure.');
       RAISE_APPLICATION_ERROR(-20001, 'Missing required tables: ' || v_missing_tables);
   END IF;

   -- Drop the table if it exists
   BEGIN
       EXECUTE IMMEDIATE 'DROP TABLE dq_review';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -942 THEN  -- Table doesn't exist
               RAISE;
           END IF;
   END;

   -- Create the DQ review table with masked personal information and validation flags
   EXECUTE IMMEDIATE '
       CREATE TABLE dq_review AS
       SELECT 
           papf.person_id,
           papf.employee_number,
           substr(papf.first_name, 1, 1) || 
               CASE 
                   WHEN papf.first_name IS NULL THEN NULL 
                   WHEN LENGTH(papf.first_name) <= 1 THEN ''''
                   ELSE RPAD(''*'', LENGTH(papf.first_name)-1, ''*'') 
               END as masked_first_name,
           CASE 
               WHEN papf.middle_names IS NULL THEN NULL
               WHEN LENGTH(papf.middle_names) = 0 THEN NULL
               ELSE substr(papf.middle_names, 1, 1) || 
                    CASE 
                        WHEN LENGTH(papf.middle_names) <= 1 THEN ''''
                        ELSE RPAD(''*'', LENGTH(papf.middle_names)-1, ''*'') 
                    END
           END as masked_middle_name,
           substr(papf.last_name, 1, 1) || 
               CASE 
                   WHEN papf.last_name IS NULL THEN NULL 
                   WHEN LENGTH(papf.last_name) <= 1 THEN ''''
                   ELSE RPAD(''*'', LENGTH(papf.last_name)-1, ''*'') 
               END as masked_last_name,
           NVL(
               substr(papf.known_as, 1, 1) || 
               CASE 
                   WHEN papf.known_as IS NULL THEN NULL 
                   WHEN LENGTH(papf.known_as) <= 1 THEN ''''
                   ELSE RPAD(''*'', LENGTH(papf.known_as)-1, ''*'') 
               END, 
               NULL
           ) as masked_known_as,
           COUNT(DISTINCT ppt.user_person_type) as person_type_count,
           LISTAGG(DISTINCT ppt.user_person_type, '', '') WITHIN GROUP (ORDER BY ppt.user_person_type) as person_types,
           -- Name validation flags
           NVL(pnc.has_name_inconsistency, ''N'') as has_name_issues,
           NVL(pnc.has_missing_required, ''N'') as has_missing_name,
           -- Email validation flags
           CASE WHEN peac.email_validation_status IN (''INVALID'', ''DUPLICATE'') THEN ''Y'' ELSE ''N'' END as has_email_issues,
           -- DOB validation flags
           CASE WHEN pdc.dob_validation_status NOT IN (''VALID'', ''SENIOR'') THEN ''Y'' ELSE ''N'' END as has_dob_issues,
           -- NINO validation flags
           CASE WHEN pnino.nino_validation_status NOT IN (''VALID'') THEN ''Y'' ELSE ''N'' END as has_nino_issues,
           -- Address validation flags (if available)
           CASE WHEN ca.is_valid = ''N'' OR avr.postal_code_status = ''Invalid'' OR avr.missing_house_number = ''Yes'' 
                THEN ''Y'' ELSE ''N'' END as has_address_issues,
           -- Validation messages
           CASE 
               WHEN pnc.has_name_inconsistency = ''Y'' OR pnc.has_missing_required = ''Y'' THEN ''Name issues detected''
               WHEN peac.email_validation_status IN (''INVALID'', ''DUPLICATE'') THEN ''Email issues detected''
               WHEN pdc.dob_validation_status NOT IN (''VALID'', ''SENIOR'') THEN ''DOB issues detected''
               WHEN pnino.nino_validation_status NOT IN (''VALID'') THEN ''NINO issues detected''
               WHEN ca.is_valid = ''N'' OR avr.postal_code_status = ''Invalid'' OR avr.missing_house_number = ''Yes'' 
               THEN ''Address issues detected''
               ELSE NULL 
           END as validation_comments,
           SYSDATE as creation_date
       FROM 
           hr.per_all_people_f papf, 
           hr.per_person_type_usages_f pptu, 
           hr.per_person_types ppt,
           -- Join with validation tables
           per_names_clean pnc,
           per_email_addresses_clean peac,
           per_dob_clean pdc,
           per_nino_clean pnino,
           clean_addresses ca,
           address_validation_results avr
       WHERE 
           papf.person_id = pptu.person_id 
           AND papf.effective_start_date <= SYSDATE 
           AND papf.effective_end_date >= SYSDATE 
           AND pptu.effective_start_date <= SYSDATE 
           AND pptu.effective_end_date >= SYSDATE 
           AND pptu.person_type_id = ppt.person_type_id 
           AND papf.current_employee_flag = ''Y''
           -- Join conditions for validation tables
           AND papf.person_id = pnc.person_id(+)
           AND papf.person_id = peac.person_id(+)
           AND papf.person_id = pdc.person_id(+)
           AND papf.person_id = pnino.person_id(+)
           AND papf.person_id = ca.employee_id(+)
           AND papf.person_id = avr.row_number(+)
       GROUP BY 
           papf.person_id, 
           papf.employee_number,
           papf.first_name,
           papf.middle_names,
           papf.last_name,
           papf.known_as,
           pnc.has_name_inconsistency,
           pnc.has_missing_required,
           peac.email_validation_status,
           pdc.dob_validation_status,
           pnino.nino_validation_status,
           ca.is_valid,
           avr.postal_code_status,
           avr.missing_house_number
       HAVING 
           COUNT(DISTINCT ppt.user_person_type) > 0
       ORDER BY 
           papf.last_name, papf.first_name';
   
   -- Get the record count
   EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM dq_review' INTO v_record_count;
   
   -- Log the creation
   INSERT INTO dq_review_log
       (operation, operation_date, records_affected)
   VALUES
       ('Create DQ Review Table', SYSTIMESTAMP, v_record_count);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('DQ Review table created successfully with ' || 
                         v_record_count || ' records.');

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error creating DQ Review table: ' || SQLERRM);
       RAISE;
END generate_dq_review_table;
/

-- Procedure to generate summary statistics
CREATE OR REPLACE PROCEDURE generate_dq_summary AS
    v_total_records NUMBER;
    v_issues_count NUMBER;
    v_count NUMBER;
BEGIN
    -- Drop existing summary table if it exists
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE dq_summary';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN RAISE; END IF;
    END;

    -- Create summary table
    EXECUTE IMMEDIATE '
    CREATE TABLE dq_summary AS
    WITH issue_counts AS (
        SELECT COUNT(*) as total_records,
               SUM(CASE WHEN has_name_issues = ''Y'' OR has_email_issues = ''Y'' 
                        OR has_address_issues = ''Y'' OR has_dob_issues = ''Y'' 
                        OR has_nino_issues = ''Y'' THEN 1 ELSE 0 END) as records_with_issues,
               SUM(CASE WHEN has_name_issues = ''Y'' THEN 1 ELSE 0 END) as name_issues,
               SUM(CASE WHEN has_email_issues = ''Y'' THEN 1 ELSE 0 END) as email_issues,
               SUM(CASE WHEN has_dob_issues = ''Y'' THEN 1 ELSE 0 END) as dob_issues,
               SUM(CASE WHEN has_nino_issues = ''Y'' THEN 1 ELSE 0 END) as nino_issues,
               SUM(CASE WHEN has_address_issues = ''Y'' THEN 1 ELSE 0 END) as address_issues
        FROM dq_review
    ),
    person_type_issues AS (
        SELECT person_types, COUNT(*) as issue_count
        FROM dq_review
        WHERE has_name_issues = ''Y''
           OR has_email_issues = ''Y''
           OR has_address_issues = ''Y''
           OR has_dob_issues = ''Y''
           OR has_nino_issues = ''Y''
        GROUP BY person_types
    ),
    sample_records AS (
        SELECT person_id, person_types, validation_comments
        FROM dq_review
        WHERE (has_name_issues = ''Y''
           OR has_email_issues = ''Y''
           OR has_address_issues = ''Y''
           OR has_dob_issues = ''Y''
           OR has_nino_issues = ''Y'')
        AND ROWNUM <= 5
    )
    SELECT 
        ic.total_records,
        ic.records_with_issues,
        ROUND((ic.records_with_issues / NULLIF(ic.total_records, 0)) * 100, 2) as percentage_with_issues,
        ic.name_issues,
        ic.email_issues,
        ic.dob_issues,
        ic.nino_issues,
        ic.address_issues,
        (SELECT LISTAGG(person_types || '': '' || issue_count, CHR(10)) 
         FROM person_type_issues) as issues_by_person_type,
        (SELECT LISTAGG(''Person ID: '' || person_id || '' | Type: '' || person_types || 
                       '' | Issues: '' || validation_comments, CHR(10))
         FROM sample_records) as sample_records
    FROM issue_counts ic';

    -- Log the creation
    INSERT INTO dq_review_log
        (operation, operation_date, records_affected)
    VALUES
        ('Create DQ Summary', SYSTIMESTAMP, 1);
    
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DQ Summary table created successfully.');

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error creating DQ Summary: ' || SQLERRM);
        RAISE;
END;
/

-- Execution syntax
/*
-- Required validation tables must be created first by running:
-- 1. @date_of_birth_validation.sql
-- 2. @email_validation.sql
-- 3. @name_field_validation.sql
-- 4. @national_insurance_validation.sql

-- Generate the DQ review table with masked data and validation flags
EXEC generate_dq_review_table;

-- Generate summary statistics
EXEC generate_dq_summary;

-- View the summary
SELECT * FROM dq_summary;

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
*/
