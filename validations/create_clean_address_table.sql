-- Create transformation log table
CREATE OR REPLACE PROCEDURE create_transformation_log IS
    -- Local variables for error handling
    v_error_message VARCHAR2(4000);
    v_error_code NUMBER;
BEGIN
    -- Drop existing objects if they exist
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE transformation_log';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN RAISE; END IF;
    END;

    BEGIN
        EXECUTE IMMEDIATE 'DROP SEQUENCE transformation_log_seq';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -2289 THEN RAISE; END IF;
    END;

    -- Create the transformation log table
    EXECUTE IMMEDIATE 'CREATE TABLE transformation_log (
        log_id NUMBER,
        transformation_name VARCHAR2(100),
        operation_type VARCHAR2(50),
        field_name VARCHAR2(100),
        original_value VARCHAR2(4000),
        transformed_value VARCHAR2(4000),
        record_id NUMBER,
        record_type VARCHAR2(50),
        status VARCHAR2(20) DEFAULT ''SUCCESS'',
        error_message VARCHAR2(4000),
        created_date TIMESTAMP DEFAULT SYSTIMESTAMP,
        created_by VARCHAR2(100) DEFAULT USER
    )';

    -- Create sequence for log_id
    EXECUTE IMMEDIATE 'CREATE SEQUENCE transformation_log_seq
        START WITH 1
        INCREMENT BY 1
        NOCACHE
        NOCYCLE';

    -- Create trigger for log_id
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TRIGGER trg_transformation_log_bir
        BEFORE INSERT ON transformation_log
        FOR EACH ROW
    BEGIN
        IF :new.log_id IS NULL THEN
            :new.log_id := transformation_log_seq.NEXTVAL;
        END IF;
    END;';

    -- Create index for common queries
    EXECUTE IMMEDIATE 'CREATE INDEX idx_transformation_log_trans_name ON transformation_log(transformation_name)';
    EXECUTE IMMEDIATE 'CREATE INDEX idx_transformation_log_record_id ON transformation_log(record_id)';
    EXECUTE IMMEDIATE 'CREATE INDEX idx_transformation_log_status ON transformation_log(status)';
    EXECUTE IMMEDIATE 'CREATE INDEX idx_transformation_log_created_date ON transformation_log(created_date)';

    -- Create procedure to log transformations
    EXECUTE IMMEDIATE 'CREATE OR REPLACE PROCEDURE log_transformation(
        p_transformation_name IN VARCHAR2,
        p_operation_type IN VARCHAR2,
        p_field_name IN VARCHAR2,
        p_original_value IN VARCHAR2,
        p_transformed_value IN VARCHAR2,
        p_record_id IN NUMBER,
        p_record_type IN VARCHAR2,
        p_status IN VARCHAR2 DEFAULT ''SUCCESS'',
        p_error_message IN VARCHAR2 DEFAULT NULL
    ) IS
    BEGIN
        INSERT INTO transformation_log (
            transformation_name,
            operation_type,
            field_name,
            original_value,
            transformed_value,
            record_id,
            record_type,
            status,
            error_message
        ) VALUES (
            p_transformation_name,
            p_operation_type,
            p_field_name,
            p_original_value,
            p_transformed_value,
            p_record_id,
            p_record_type,
            p_status,
            p_error_message
        );
    END;';

EXCEPTION
    WHEN OTHERS THEN
        v_error_message := SQLERRM;
        v_error_code := SQLCODE;
        RAISE_APPLICATION_ERROR(-20001, 'Error creating transformation log: ' || v_error_message);
END create_transformation_log;
/

-- Create clean address table
CREATE OR REPLACE PROCEDURE create_clean_address_table IS
    -- Local variables for error handling
    v_error_message VARCHAR2(4000);
    v_error_code NUMBER;
    v_sql VARCHAR2(4000);
BEGIN
    -- Ensure transformation log exists
    BEGIN
        create_transformation_log;
    EXCEPTION
        WHEN OTHERS THEN
            v_error_message := SQLERRM;
            v_error_code := SQLCODE;
            RAISE_APPLICATION_ERROR(-20001, 'Error creating transformation log: ' || v_error_message);
    END;

    -- Drop table if exists
    BEGIN
        v_sql := 'DROP TABLE clean_addresses';
        EXECUTE IMMEDIATE v_sql;
    EXCEPTION
        WHEN OTHERS THEN
            NULL; -- Table doesn't exist, continue
    END;

    -- Create the clean addresses table
    v_sql := 'CREATE TABLE clean_addresses (
        address_id NUMBER GENERATED ALWAYS AS IDENTITY,
        employee_id NUMBER,
        address_type VARCHAR2(20),
        -- Original address lines
        address_line1 VARCHAR2(100),
        address_line2 VARCHAR2(100),
        address_line3 VARCHAR2(100),
        -- Standardized components
        main_building_number VARCHAR2(20),
        sub_building_number VARCHAR2(50),
        town_and_city VARCHAR2(50),
        post_code VARCHAR2(20),
        country VARCHAR2(50),
        -- Full address string
        full_address VARCHAR2(500),
        -- Data quality flags
        is_valid CHAR(1) DEFAULT ''Y'',
        validation_errors VARCHAR2(4000),
        -- Audit columns
        is_active CHAR(1) DEFAULT ''Y'',
        created_date TIMESTAMP DEFAULT SYSTIMESTAMP,
        modified_date TIMESTAMP DEFAULT SYSTIMESTAMP
    )';
    EXECUTE IMMEDIATE v_sql;

    -- Create indexes
    v_sql := 'CREATE INDEX idx_clean_addresses_employee_id ON clean_addresses(employee_id)';
    EXECUTE IMMEDIATE v_sql;
    
    v_sql := 'CREATE INDEX idx_clean_addresses_post_code ON clean_addresses(post_code)';
    EXECUTE IMMEDIATE v_sql;
    
    v_sql := 'CREATE INDEX idx_clean_addresses_country ON clean_addresses(country)';
    EXECUTE IMMEDIATE v_sql;

    -- Create trigger for modified_date
    v_sql := 'CREATE OR REPLACE TRIGGER trg_clean_addresses_modify
        BEFORE UPDATE ON clean_addresses
        FOR EACH ROW
    BEGIN
        :NEW.modified_date := SYSTIMESTAMP;
    END;';
    EXECUTE IMMEDIATE v_sql;

EXCEPTION
    WHEN OTHERS THEN
        v_error_message := SQLERRM;
        v_error_code := SQLCODE;
        RAISE_APPLICATION_ERROR(-20001, 'Error creating clean address table: ' || v_error_message);
END create_clean_address_table;
/

-- Create procedure to populate clean addresses
CREATE OR REPLACE PROCEDURE populate_clean_addresses IS
    v_error_message VARCHAR2(4000);
    v_error_code NUMBER;
    v_main_building_number VARCHAR2(20);
    v_sub_building_number VARCHAR2(50);
    v_full_address VARCHAR2(500);
BEGIN
    -- Insert data from locations table
    FOR addr_rec IN (
        SELECT 
            e.employee_id,
            l.street_address,
            l.city,
            l.postal_code,
            c.country_name
        FROM 
            employees e
            JOIN departments d ON e.department_id = d.department_id
            JOIN locations l ON d.location_id = l.location_id
            JOIN countries c ON l.country_id = c.country_id
        WHERE 
            e.employee_id NOT IN (SELECT employee_id FROM clean_addresses)
    ) LOOP
        -- Extract address components
        v_main_building_number := extract_main_building_number(addr_rec.street_address);
        v_sub_building_number := extract_sub_building_number(addr_rec.street_address);

        -- Build full address string handling nulls
        v_full_address := NULL;
        IF v_main_building_number IS NOT NULL THEN
            v_full_address := v_main_building_number;
        END IF;
        
        IF addr_rec.street_address IS NOT NULL THEN
            v_full_address := v_full_address || ' ' || addr_rec.street_address;
        END IF;
        
        IF v_sub_building_number IS NOT NULL THEN
            v_full_address := v_full_address || ', ' || v_sub_building_number;
        END IF;
        
        IF addr_rec.city IS NOT NULL THEN
            v_full_address := v_full_address || ', ' || addr_rec.city;
        END IF;
        
        IF addr_rec.postal_code IS NOT NULL THEN
            v_full_address := v_full_address || ' ' || addr_rec.postal_code;
        END IF;
        
        IF addr_rec.country_name IS NOT NULL THEN
            v_full_address := v_full_address || ', ' || addr_rec.country_name;
        END IF;

        -- Insert the address
        INSERT INTO clean_addresses (
            employee_id,
            address_type,
            address_line1,
            main_building_number,
            sub_building_number,
            town_and_city,
            post_code,
            country,
            full_address
        ) VALUES (
            addr_rec.employee_id,
            'WORK',
            addr_rec.street_address,
            v_main_building_number,
            v_sub_building_number,
            addr_rec.city,
            addr_rec.postal_code,
            addr_rec.country_name,
            v_full_address
        );

        -- Log the transformations
        log_transformation(
            p_transformation_name => 'ADDRESS_STANDARDIZATION',
            p_operation_type => 'TRANSFORM',
            p_field_name => 'ADDRESS_LINE1',
            p_original_value => addr_rec.street_address,
            p_transformed_value => v_main_building_number || ' ' || addr_rec.street_address,
            p_record_id => addr_rec.employee_id,
            p_record_type => 'EMPLOYEE_ADDRESS'
        );

        IF v_sub_building_number IS NOT NULL THEN
            log_transformation(
                p_transformation_name => 'ADDRESS_STANDARDIZATION',
                p_operation_type => 'TRANSFORM',
                p_field_name => 'SUB_BUILDING',
                p_original_value => addr_rec.street_address,
                p_transformed_value => v_sub_building_number,
                p_record_id => addr_rec.employee_id,
                p_record_type => 'EMPLOYEE_ADDRESS'
            );
        END IF;
    END LOOP;
        
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        v_error_message := SQLERRM;
        v_error_code := SQLCODE;
        ROLLBACK;
        RAISE_APPLICATION_ERROR(-20001, 'Error populating clean addresses: ' || v_error_message);
END populate_clean_addresses;
/

-- Create procedure to validate addresses
CREATE OR REPLACE PROCEDURE validate_addresses IS
    v_error_message VARCHAR2(4000);
    v_error_code NUMBER;
    v_sql CLOB;  -- Changed from VARCHAR2(4000) to CLOB to handle large SQL queries
BEGIN
    -- Drop validation results table if exists
    BEGIN
        v_sql := 'DROP TABLE address_validation_results';
        EXECUTE IMMEDIATE v_sql;
    EXCEPTION
        WHEN OTHERS THEN
            NULL; -- Table doesn't exist, continue
    END;

    -- Create and populate validation results table
    v_sql := 'CREATE TABLE address_validation_results AS
    WITH addressdata AS (
        SELECT
            pa.address_line1,
            pa.town_or_city,
            pa.postal_code,
            CASE
                WHEN regexp_substr(pa.address_line1, ''^[0-9]+'') IS NULL THEN
                    ''Yes''
                ELSE
                    ''No''
            END AS missing_house_number,
            CASE
                WHEN not REGEXP_LIKE ( pa.postal_code,
                                   ''^([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([AZa-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9]?[A-Za-z])))) [0-9][A-Za-z]{2})$''
                                   ) THEN
                    ''Invalid''
                ELSE
                    ''Valid''
            END AS postal_code_status,
            CASE
                WHEN pa.primary_flag = ''Y'' THEN
                    ''Primary''
                ELSE
                    ''Secondary''
            END AS address_status
        FROM
                 hr.per_addresses pa
            JOIN hr.per_all_people_f pp ON pa.person_id = pp.person_id
        WHERE
            trunc(sysdate) BETWEEN pp.effective_start_date AND pp.effective_end_date
            AND ( pa.date_to IS NULL
                  OR pa.date_to > sysdate )
            AND pp.current_employee_flag = ''Y''
    )
    SELECT
        ROW_NUMBER() OVER(ORDER BY pa.last_update_date DESC) AS row_number,
        TRIM(TRIM(pa.address_line1)
             || '' ''
             ||
             CASE
                 WHEN pa.address_line2 IS NOT NULL THEN
                     TRIM(pa.address_line2)
                     || '' ''
                 ELSE
                     ''''
             END
             ||
             CASE
                 WHEN pa.address_line3 IS NOT NULL THEN
                     TRIM(pa.address_line3)
                     || '' ''
                 ELSE
                     ''''
             END
             ||
             CASE
                 WHEN pa.town_or_city IS NOT NULL THEN
                     TRIM(pa.town_or_city)
                     || '' ''
                 ELSE
                     ''''
             END
             ||
             CASE
                 WHEN pa.postal_code IS NOT NULL THEN
                     TRIM(pa.postal_code)
                 ELSE
                     ''''
             END
        ) AS full_address,
        pa.address_line1,
        pa.address_line2,
        pa.address_line3,
        pa.address_type,
        pa.town_or_city,
        pa.country,
        pa.postal_code,
        regexp_substr(pa.address_line1, ''^[0-9]+'') AS house_number,
        CASE
            WHEN regexp_substr(pa.address_line1, ''^[0-9]+'') IS NULL THEN
                ''Yes''
            ELSE
                ''No''
        END AS missing_house_number,
        CASE
            WHEN not REGEXP_LIKE ( pa.postal_code,
                               ''^([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([AZa-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9]?[A-Za-z])))) [0-9][A-Za-z]{2})$''
                               ) THEN
                ''Invalid''
            ELSE
                ''Valid''
        END AS postal_code_analysis,
        CASE
            WHEN pa.primary_flag = ''Y'' THEN
                ''Primary''
            ELSE
                ''Secondary''
        END AS address_status,
        pa.last_update_date,
        trunc(pa.date_from) AS valid_from_date,
        trunc(pa.date_to) AS valid_to_date,
        ad.postal_code_status,
        SUM(CASE WHEN ad.postal_code_status = ''Valid'' THEN 1 ELSE 0 END) OVER (PARTITION BY pa.town_or_city) AS valid_count,
        SUM(CASE WHEN ad.postal_code_status = ''Invalid'' THEN 1 ELSE 0 END) OVER (PARTITION BY pa.town_or_city) AS invalid_count,
        SUM(CASE WHEN ad.missing_house_number = ''Yes'' THEN 1 ELSE 0 END) OVER (PARTITION BY pa.town_or_city) AS missing_number_count,
        SUM(CASE WHEN ad.missing_house_number = ''No'' THEN 1 ELSE 0 END) OVER (PARTITION BY pa.town_or_city) AS has_number_count,
        ROUND((SUM(CASE WHEN ad.postal_code_status = ''Valid'' THEN 1 ELSE 0 END) OVER (PARTITION BY pa.town_or_city) * 100.0 / 
               COUNT(*) OVER (PARTITION BY pa.town_or_city)), 0) AS valid_percentage,
        ROUND((SUM(CASE WHEN ad.postal_code_status = ''Invalid'' THEN 1 ELSE 0 END) OVER (PARTITION BY pa.town_or_city) * 100.0 / 
               COUNT(*) OVER (PARTITION BY pa.town_or_city)), 0) AS invalid_percentage,
        ROUND((SUM(CASE WHEN ad.missing_house_number = ''Yes'' THEN 1 ELSE 0 END) OVER (PARTITION BY pa.town_or_city) * 100.0 / 
               COUNT(*) OVER (PARTITION BY pa.town_or_city)), 0) AS missing_number_percentage,
        ROUND((SUM(CASE WHEN ad.missing_house_number = ''No'' THEN 1 ELSE 0 END) OVER (PARTITION BY pa.town_or_city) * 100.0 / 
               COUNT(*) OVER (PARTITION BY pa.town_or_city)), 0) AS has_number_percentage,
        COUNT(*) OVER (PARTITION BY pa.town_or_city) AS assessed_count
    FROM
        hr.per_addresses pa
        JOIN hr.per_all_people_f pp ON pa.person_id = pp.person_id
        JOIN addressdata ad ON pa.address_line1 = ad.address_line1
    WHERE
        trunc(sysdate) BETWEEN pp.effective_start_date AND pp.effective_end_date
        AND ( pa.date_to IS NULL
              OR pa.date_to > sysdate )
        AND pp.current_employee_flag = ''Y''';
    EXECUTE IMMEDIATE v_sql;

    -- Log validation results
    v_sql := 'INSERT INTO transformation_log (
        transformation_name,
        operation_type,
        field_name,
        original_value,
        transformed_value,
        record_id,
        record_type,
        created_date
    )
    SELECT
        ''ADDRESS_VALIDATION'',
        ''VALIDATE'',
        ''POSTAL_CODE'',
        postal_code,
        postal_code_status,
        row_number,
        ''ADDRESS'',
        SYSTIMESTAMP
    FROM
        address_validation_results
    WHERE
        postal_code_status = ''Invalid''';
    EXECUTE IMMEDIATE v_sql;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        v_error_message := SQLERRM;
        v_error_code := SQLCODE;
        ROLLBACK;
        RAISE_APPLICATION_ERROR(-20001, 'Error validating addresses: ' || v_error_message);
END validate_addresses;
/

-- Add table and column comments after all objects are created
-- Transformation Log Table Comments
COMMENT ON TABLE transformation_log IS 'Logs all data transformations with detailed tracking';
COMMENT ON COLUMN transformation_log.log_id IS 'Unique identifier for the log entry';
COMMENT ON COLUMN transformation_log.transformation_name IS 'Name of the transformation being performed';
COMMENT ON COLUMN transformation_log.operation_type IS 'Type of operation (INSERT, UPDATE, DELETE, TRANSFORM)';
COMMENT ON COLUMN transformation_log.field_name IS 'Name of the field being transformed';
COMMENT ON COLUMN transformation_log.original_value IS 'Original value before transformation';
COMMENT ON COLUMN transformation_log.transformed_value IS 'Value after transformation';
COMMENT ON COLUMN transformation_log.record_id IS 'ID of the record being transformed';
COMMENT ON COLUMN transformation_log.record_type IS 'Type of record being transformed';
COMMENT ON COLUMN transformation_log.status IS 'Status of the transformation (SUCCESS, ERROR, WARNING)';
COMMENT ON COLUMN transformation_log.error_message IS 'Error message if transformation failed';
COMMENT ON COLUMN transformation_log.created_date IS 'Timestamp of the log entry';
COMMENT ON COLUMN transformation_log.created_by IS 'User who performed the transformation';

-- Clean Addresses Table Comments
COMMENT ON TABLE clean_addresses IS 'Clean and standardized address information for employees with data quality tracking';
COMMENT ON COLUMN clean_addresses.address_id IS 'Unique identifier for the address';
COMMENT ON COLUMN clean_addresses.employee_id IS 'Reference to the employee';
COMMENT ON COLUMN clean_addresses.address_type IS 'Type of address (HOME, WORK, etc.)';
COMMENT ON COLUMN clean_addresses.address_line1 IS 'Original address line 1';
COMMENT ON COLUMN clean_addresses.address_line2 IS 'Original address line 2';
COMMENT ON COLUMN clean_addresses.address_line3 IS 'Original address line 3';
COMMENT ON COLUMN clean_addresses.main_building_number IS 'Extracted main building number';
COMMENT ON COLUMN clean_addresses.sub_building_number IS 'Extracted sub-building number (apt, floor, etc.)';
COMMENT ON COLUMN clean_addresses.town_and_city IS 'Town and city name';
COMMENT ON COLUMN clean_addresses.post_code IS 'Postal or ZIP code';
COMMENT ON COLUMN clean_addresses.country IS 'Country name';
COMMENT ON COLUMN clean_addresses.full_address IS 'Complete formatted address string';
COMMENT ON COLUMN clean_addresses.is_valid IS 'Data quality validation flag';
COMMENT ON COLUMN clean_addresses.validation_errors IS 'Description of any validation errors';
COMMENT ON COLUMN clean_addresses.is_active IS 'Active status of the address';
COMMENT ON COLUMN clean_addresses.created_date IS 'Record creation timestamp';
COMMENT ON COLUMN clean_addresses.modified_date IS 'Record last modification timestamp';

-- Procedure Comments
COMMENT ON PROCEDURE create_transformation_log IS 'Creates the transformation logging structure with proper error handling';
COMMENT ON PROCEDURE create_clean_address_table IS 'Creates the clean_addresses table and related objects with proper error handling';
COMMENT ON PROCEDURE populate_clean_addresses IS 'Populates the clean_addresses table with standardized address information from the locations table';
COMMENT ON PROCEDURE validate_addresses IS 'Validates addresses and creates a temporary table with validation results including postal code analysis and house number validation';

-- Execution syntax
/*
-- Create transformation log and clean address table
EXEC create_transformation_log;
EXEC create_clean_address_table;

-- Populate and validate addresses
EXEC populate_clean_addresses;
EXEC validate_addresses;

-- View the transformation log by operation:
SELECT 
    transformation_name,
    operation_type,
    field_name,
    record_id,
    status,
    created_date
FROM transformation_log
ORDER BY created_date;

-- View summary of address validation results:
SELECT 
    postal_code_status,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM address_validation_results
GROUP BY postal_code_status
ORDER BY COUNT(*) DESC;

-- View addresses with missing house numbers:
SELECT 
    row_number,
    full_address,
    address_line1,
    postal_code,
    missing_house_number,
    postal_code_status
FROM address_validation_results
WHERE missing_house_number = 'Yes'
ORDER BY row_number;

-- View postal code validation by town/city:
SELECT 
    town_or_city,
    valid_count,
    invalid_count,
    valid_percentage,
    invalid_percentage,
    assessed_count
FROM address_validation_results
GROUP BY 
    town_or_city,
    valid_count,
    invalid_count,
    valid_percentage,
    invalid_percentage,
    assessed_count
ORDER BY invalid_percentage DESC;

-- View addresses with validation issues:
SELECT 
    row_number,
    full_address,
    postal_code,
    postal_code_status,
    missing_house_number,
    address_status
FROM address_validation_results
WHERE postal_code_status = 'Invalid'
   OR missing_house_number = 'Yes'
ORDER BY row_number;

-- View clean addresses with validation flags:
SELECT 
    ca.employee_id,
    ca.full_address,
    ca.post_code,
    ca.is_valid,
    ca.validation_errors,
    ca.created_date
FROM clean_addresses ca
WHERE ca.is_valid = 'N'
ORDER BY ca.employee_id;
*/ 