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
   v_record_count NUMBER := 0;
BEGIN
   -- Drop the table if it exists
   BEGIN
       EXECUTE IMMEDIATE 'DROP TABLE dq_review';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -942 THEN  -- Table doesn't exist
               RAISE;
           END IF;
   END;

   -- Create the DQ review table with masked personal information
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
           LISTAGG(ppt.user_person_type, '', '') WITHIN GROUP (ORDER BY ppt.user_person_type) as person_types,
           SYSDATE as creation_date
       FROM 
           per_all_people_f papf, 
           per_person_type_usages_f pptu, 
           per_person_types ppt
       WHERE 
           papf.person_id = pptu.person_id 
           AND papf.effective_start_date <= SYSDATE 
           AND papf.effective_end_date >= SYSDATE 
           AND pptu.effective_start_date <= SYSDATE 
           AND pptu.effective_end_date >= SYSDATE 
           AND pptu.person_type_id = ppt.person_type_id 
           AND papf.current_employee_flag = ''Y''
       GROUP BY 
           papf.person_id, 
           papf.employee_number,
           papf.first_name,
           papf.middle_names,
           papf.last_name,
           papf.known_as
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

-- Create procedure to add validation flags to DQ review
CREATE OR REPLACE PROCEDURE add_dq_validation_flags
IS
   v_records_updated NUMBER := 0;
BEGIN
   -- Add validation columns if they don't exist
   BEGIN
       EXECUTE IMMEDIATE 'ALTER TABLE dq_review ADD (
           has_name_issues VARCHAR2(1) DEFAULT ''N'',
           has_email_issues VARCHAR2(1) DEFAULT ''N'',
           has_address_issues VARCHAR2(1) DEFAULT ''N'',
           has_dob_issues VARCHAR2(1) DEFAULT ''N'',
           has_nino_issues VARCHAR2(1) DEFAULT ''N'',
           validation_comments CLOB
       )';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -1430 THEN  -- Column already exists
               RAISE;
           END IF;
   END;
   
   -- Update name issues based on per_names_clean
   BEGIN
       EXECUTE IMMEDIATE '
           UPDATE dq_review dr
           SET has_name_issues = ''Y'',
               validation_comments = CASE 
                   WHEN validation_comments IS NULL THEN ''Name issues detected''
                   ELSE validation_comments || ''; Name issues detected'' 
               END
           WHERE EXISTS (
               SELECT 1 FROM per_names_clean pnc
               WHERE pnc.person_id = dr.person_id
               AND (pnc.has_name_inconsistency = ''Y'' OR pnc.has_missing_required = ''Y'')
           )'
           RETURNING COUNT(*) INTO v_records_updated;
       
       DBMS_OUTPUT.PUT_LINE('Updated ' || v_records_updated || ' records with name issues');
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -942 THEN  -- Table doesn't exist
               DBMS_OUTPUT.PUT_LINE('Name validation table not found, skipping...');
           ELSE
               RAISE;
           END IF;
   END;
   
   -- Update email issues based on per_emails_clean
   BEGIN
       EXECUTE IMMEDIATE '
           UPDATE dq_review dr
           SET has_email_issues = ''Y'',
               validation_comments = CASE 
                   WHEN validation_comments IS NULL THEN ''Email issues detected''
                   ELSE validation_comments || ''; Email issues detected'' 
               END
           WHERE EXISTS (
               SELECT 1 FROM per_emails_clean pec
               WHERE pec.person_id = dr.person_id
               AND pec.email_validation_status IN (''INVALID'', ''DUPLICATE'')
           )'
           RETURNING COUNT(*) INTO v_records_updated;
       
       DBMS_OUTPUT.PUT_LINE('Updated ' || v_records_updated || ' records with email issues');
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -942 THEN  -- Table doesn't exist
               DBMS_OUTPUT.PUT_LINE('Email validation table not found, skipping...');
           ELSE
               RAISE;
           END IF;
   END;
   
   -- Update address issues based on per_addresses_clean
   BEGIN
       EXECUTE IMMEDIATE '
           UPDATE dq_review dr
           SET has_address_issues = ''Y'',
               validation_comments = CASE 
                   WHEN validation_comments IS NULL THEN ''Address issues detected''
                   ELSE validation_comments || ''; Address issues detected'' 
               END
           WHERE EXISTS (
               SELECT 1 FROM per_addresses_clean pac
               WHERE pac.person_id = dr.person_id
               AND (pac.is_incomplete_address = ''Y'' OR pac.is_valid_uk_postcode = ''N'')
           )'
           RETURNING COUNT(*) INTO v_records_updated;
       
       DBMS_OUTPUT.PUT_LINE('Updated ' || v_records_updated || ' records with address issues');
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -942 THEN  -- Table doesn't exist
               DBMS_OUTPUT.PUT_LINE('Address validation table not found, skipping...');
           ELSE
               RAISE;
           END IF;
   END;
   
   -- Update DOB issues based on per_dob_clean
   BEGIN
       EXECUTE IMMEDIATE '
           UPDATE dq_review dr
           SET has_dob_issues = ''Y'',
               validation_comments = CASE 
                   WHEN validation_comments IS NULL THEN ''DOB issues detected''
                   ELSE validation_comments || ''; DOB issues detected'' 
               END
           WHERE EXISTS (
               SELECT 1 FROM per_dob_clean pdc
               WHERE pdc.person_id = dr.person_id
               AND pdc.dob_validation_status NOT IN (''VALID'', ''SENIOR'')
           )'
           RETURNING COUNT(*) INTO v_records_updated;
       
       DBMS_OUTPUT.PUT_LINE('Updated ' || v_records_updated || ' records with DOB issues');
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -942 THEN  -- Table doesn't exist
               DBMS_OUTPUT.PUT_LINE('DOB validation table not found, skipping...');
           ELSE
               RAISE;
           END IF;
   END;
   
   -- Update NINO issues based on per_nino_clean
   BEGIN
       EXECUTE IMMEDIATE '
           UPDATE dq_review dr
           SET has_nino_issues = ''Y'',
               validation_comments = CASE 
                   WHEN validation_comments IS NULL THEN ''NINO issues detected''
                   ELSE validation_comments || ''; NINO issues detected'' 
               END
           WHERE EXISTS (
               SELECT 1 FROM per_nino_clean pnc
               WHERE pnc.person_id = dr.person_id
               AND pnc.nino_validation_status NOT IN (''VALID'')
           )'
           RETURNING COUNT(*) INTO v_records_updated;
       
       DBMS_OUTPUT.PUT_LINE('Updated ' || v_records_updated || ' records with NINO issues');
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -942 THEN  -- Table doesn't exist
               DBMS_OUTPUT.PUT_LINE('NINO validation table not found, skipping...');
           ELSE
               RAISE;
           END IF;
   END;
   
   COMMIT;
   
   -- Log the update
   INSERT INTO dq_review_log
       (operation, operation_date, records_affected)
   VALUES
       ('Add validation flags', SYSTIMESTAMP, v_records_updated);
   
   DBMS_OUTPUT.PUT_LINE('Added validation flags to DQ Review table');

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error adding validation flags: ' || SQLERRM);
       RAISE;
END add_dq_validation_flags;
/

-- Procedure to generate summary statistics
CREATE OR REPLACE PROCEDURE generate_dq_summary
IS
BEGIN
   -- Create a summary table if it doesn't exist
   BEGIN
       EXECUTE IMMEDIATE 'DROP TABLE dq_summary';
   EXCEPTION
       WHEN OTHERS THEN
           IF SQLCODE != -942 THEN RAISE; END IF;
   END;
   
   -- Create summary table with counts and percentages
   EXECUTE IMMEDIATE '
       CREATE TABLE dq_summary AS
       SELECT
           ''Total records'' as metric,
           COUNT(*) as count,
           100 as percentage
       FROM dq_review
       UNION ALL
       SELECT
           ''Records with name issues'' as metric,
           COUNT(*) as count,
           ROUND(COUNT(*) * 100 / (SELECT COUNT(*) FROM dq_review), 2) as percentage
       FROM dq_review
       WHERE has_name_issues = ''Y''
       UNION ALL
       SELECT
           ''Records with email issues'' as metric,
           COUNT(*) as count,
           ROUND(COUNT(*) * 100 / (SELECT COUNT(*) FROM dq_review), 2) as percentage
       FROM dq_review
       WHERE has_email_issues = ''Y''
       UNION ALL
       SELECT
           ''Records with address issues'' as metric,
           COUNT(*) as count,
           ROUND(COUNT(*) * 100 / (SELECT COUNT(*) FROM dq_review), 2) as percentage
       FROM dq_review
       WHERE has_address_issues = ''Y''
       UNION ALL
       SELECT
           ''Records with DOB issues'' as metric,
           COUNT(*) as count,
           ROUND(COUNT(*) * 100 / (SELECT COUNT(*) FROM dq_review), 2) as percentage
       FROM dq_review
       WHERE has_dob_issues = ''Y''
       UNION ALL
       SELECT
           ''Records with NINO issues'' as metric,
           COUNT(*) as count,
           ROUND(COUNT(*) * 100 / (SELECT COUNT(*) FROM dq_review), 2) as percentage
       FROM dq_review
       WHERE has_nino_issues = ''Y''
       UNION ALL
       SELECT
           ''Records with multiple issues'' as metric,
           COUNT(*) as count,
           ROUND(COUNT(*) * 100 / (SELECT COUNT(*) FROM dq_review), 2) as percentage
       FROM dq_review
       WHERE (CASE WHEN has_name_issues = ''Y'' THEN 1 ELSE 0 END +
              CASE WHEN has_email_issues = ''Y'' THEN 1 ELSE 0 END +
              CASE WHEN has_address_issues = ''Y'' THEN 1 ELSE 0 END +
              CASE WHEN has_dob_issues = ''Y'' THEN 1 ELSE 0 END +
              CASE WHEN has_nino_issues = ''Y'' THEN 1 ELSE 0 END) > 1
       UNION ALL
       SELECT
           ''Records with no issues'' as metric,
           COUNT(*) as count,
           ROUND(COUNT(*) * 100 / (SELECT COUNT(*) FROM dq_review), 2) as percentage
       FROM dq_review
       WHERE has_name_issues = ''N''
         AND has_email_issues = ''N''
         AND has_address_issues = ''N''
         AND has_dob_issues = ''N''
         AND has_nino_issues = ''N''
       ORDER BY 
           CASE 
               WHEN metric = ''Total records'' THEN 1
               WHEN metric = ''Records with no issues'' THEN 9
               WHEN metric = ''Records with multiple issues'' THEN 8
               ELSE 2
           END';
   
   -- Log the creation
   INSERT INTO dq_review_log
       (operation, operation_date, records_affected)
   VALUES
       ('Create DQ Summary', SYSTIMESTAMP, 0);
   
   COMMIT;
   
   DBMS_OUTPUT.PUT_LINE('DQ Summary table created successfully');

EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE('Error creating DQ summary: ' || SQLERRM);
       RAISE;
END generate_dq_summary;
/

-- Execution syntax
/*
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
*/
