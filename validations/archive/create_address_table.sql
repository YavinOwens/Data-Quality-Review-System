-- Create address table
CREATE OR REPLACE PROCEDURE create_address_table IS
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

    -- Drop existing table if it exists
    BEGIN
        v_sql := 'DROP TABLE addresses';
        EXECUTE IMMEDIATE v_sql;
    EXCEPTION
        WHEN OTHERS THEN
            NULL; -- Table doesn't exist, continue
    END;

    -- Create the addresses table
    v_sql := 'CREATE TABLE addresses (
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
        street_name VARCHAR2(100),
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
    v_sql := 'CREATE INDEX idx_addresses_employee_id ON addresses(employee_id)';
    EXECUTE IMMEDIATE v_sql;
    
    v_sql := 'CREATE INDEX idx_addresses_post_code ON addresses(post_code)';
    EXECUTE IMMEDIATE v_sql;
    
    v_sql := 'CREATE INDEX idx_addresses_country ON addresses(country)';
    EXECUTE IMMEDIATE v_sql;

    -- Create trigger for modified_date
    v_sql := 'CREATE OR REPLACE TRIGGER trg_addresses_modify
        BEFORE UPDATE ON addresses
        FOR EACH ROW
    BEGIN
        :NEW.modified_date := SYSTIMESTAMP;
    END;';
    EXECUTE IMMEDIATE v_sql;

    -- Create procedure to populate addresses
    v_sql := 'CREATE OR REPLACE PROCEDURE populate_addresses IS
        v_error_message VARCHAR2(4000);
        v_error_code NUMBER;
        v_main_building_number VARCHAR2(20);
        v_sub_building_number VARCHAR2(50);
        v_street_name VARCHAR2(100);
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
                e.employee_id NOT IN (SELECT employee_id FROM addresses)
        ) LOOP
            -- Extract address components
            v_main_building_number := extract_main_building_number(addr_rec.street_address);
            v_sub_building_number := extract_sub_building_number(addr_rec.street_address);
            v_street_name := extract_street_name(addr_rec.street_address);

            -- Build full address string handling nulls
            v_full_address := NULL;
            IF v_main_building_number IS NOT NULL THEN
                v_full_address := v_main_building_number;
            END IF;
            
            IF v_street_name IS NOT NULL THEN
                v_full_address := v_full_address || '' '' || v_street_name;
            END IF;
            
            IF v_sub_building_number IS NOT NULL THEN
                v_full_address := v_full_address || '', '' || v_sub_building_number;
            END IF;
            
            IF addr_rec.city IS NOT NULL THEN
                v_full_address := v_full_address || '', '' || addr_rec.city;
            END IF;
            
            IF addr_rec.postal_code IS NOT NULL THEN
                v_full_address := v_full_address || '' '' || addr_rec.postal_code;
            END IF;
            
            IF addr_rec.country_name IS NOT NULL THEN
                v_full_address := v_full_address || '', '' || addr_rec.country_name;
            END IF;

            -- Insert the address
            INSERT INTO addresses (
                employee_id,
                address_type,
                address_line1,
                main_building_number,
                sub_building_number,
                street_name,
                town_and_city,
                post_code,
                country,
                full_address
            ) VALUES (
                addr_rec.employee_id,
                ''WORK'',
                addr_rec.street_address,
                v_main_building_number,
                v_sub_building_number,
                v_street_name,
                addr_rec.city,
                addr_rec.postal_code,
                addr_rec.country_name,
                v_full_address
            );

            -- Log the transformations
            log_transformation(
                p_transformation_name => ''ADDRESS_STANDARDIZATION'',
                p_operation_type => ''TRANSFORM'',
                p_field_name => ''ADDRESS_LINE1'',
                p_original_value => addr_rec.street_address,
                p_transformed_value => v_main_building_number || '' '' || v_street_name,
                p_record_id => addr_rec.employee_id,
                p_record_type => ''EMPLOYEE_ADDRESS''
            );

            IF v_sub_building_number IS NOT NULL THEN
                log_transformation(
                    p_transformation_name => ''ADDRESS_STANDARDIZATION'',
                    p_operation_type => ''TRANSFORM'',
                    p_field_name => ''SUB_BUILDING'',
                    p_original_value => addr_rec.street_address,
                    p_transformed_value => v_sub_building_number,
                    p_record_id => addr_rec.employee_id,
                    p_record_type => ''EMPLOYEE_ADDRESS''
                );
            END IF;
        END LOOP;
            
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            v_error_message := SQLERRM;
            v_error_code := SQLCODE;
            ROLLBACK;
            RAISE_APPLICATION_ERROR(-20001, ''Error populating addresses: '' || v_error_message);
    END;';
    EXECUTE IMMEDIATE v_sql;

EXCEPTION
    WHEN OTHERS THEN
        v_error_message := SQLERRM;
        v_error_code := SQLCODE;
        RAISE_APPLICATION_ERROR(-20001, 'Error creating address table: ' || v_error_message);
END create_address_table;
/

-- Add table and column comments after all objects are created
COMMENT ON TABLE addresses IS 'Standardized address information for employees with data quality tracking';
COMMENT ON COLUMN addresses.address_id IS 'Unique identifier for the address';
COMMENT ON COLUMN addresses.employee_id IS 'Reference to the employee';
COMMENT ON COLUMN addresses.address_type IS 'Type of address (HOME, WORK, etc.)';
COMMENT ON COLUMN addresses.address_line1 IS 'Original address line 1';
COMMENT ON COLUMN addresses.address_line2 IS 'Original address line 2';
COMMENT ON COLUMN addresses.address_line3 IS 'Original address line 3';
COMMENT ON COLUMN addresses.main_building_number IS 'Extracted main building number';
COMMENT ON COLUMN addresses.sub_building_number IS 'Extracted sub-building number (apt, floor, etc.)';
COMMENT ON COLUMN addresses.street_name IS 'Extracted street name';
COMMENT ON COLUMN addresses.town_and_city IS 'Town and city name';
COMMENT ON COLUMN addresses.post_code IS 'Postal or ZIP code';
COMMENT ON COLUMN addresses.country IS 'Country name';
COMMENT ON COLUMN addresses.full_address IS 'Complete formatted address string';
COMMENT ON COLUMN addresses.is_valid IS 'Data quality validation flag';
COMMENT ON COLUMN addresses.validation_errors IS 'Description of any validation errors';
COMMENT ON COLUMN addresses.is_active IS 'Active status of the address';
COMMENT ON COLUMN addresses.created_date IS 'Record creation timestamp';
COMMENT ON COLUMN addresses.modified_date IS 'Record last modification timestamp';

COMMENT ON PROCEDURE create_address_table IS 'Creates the addresses table and related objects with proper error handling';
COMMENT ON PROCEDURE populate_addresses IS 'Populates the addresses table with standardized address information from the locations table'; 