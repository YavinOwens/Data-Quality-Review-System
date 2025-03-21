-- Create clean address table
CREATE OR REPLACE PROCEDURE create_clean_address_table IS
BEGIN
    -- Drop table if exists
    EXECUTE IMMEDIATE 'DROP TABLE clean_addresses';
EXCEPTION
    WHEN OTHERS THEN
        NULL; -- Table doesn't exist, continue
END;
/

-- Create the clean addresses table
CREATE TABLE clean_addresses (
    address_id NUMBER GENERATED ALWAYS AS IDENTITY,
    employee_id NUMBER,
    address_type VARCHAR2(20),
    building_number VARCHAR2(20),
    street_name VARCHAR2(100),
    city VARCHAR2(50),
    state VARCHAR2(50),
    postal_code VARCHAR2(20),
    country VARCHAR2(50),
    is_active CHAR(1) DEFAULT 'Y',
    created_date TIMESTAMP DEFAULT SYSTIMESTAMP,
    modified_date TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Create index on employee_id
CREATE INDEX idx_clean_addresses_employee_id ON clean_addresses(employee_id);

-- Add comments
COMMENT ON TABLE clean_addresses IS 'Clean and standardized address information for employees';
COMMENT ON COLUMN clean_addresses.address_id IS 'Unique identifier for the address';
COMMENT ON COLUMN clean_addresses.employee_id IS 'Reference to the employee';
COMMENT ON COLUMN clean_addresses.address_type IS 'Type of address (HOME, WORK, etc.)';
COMMENT ON COLUMN clean_addresses.building_number IS 'Extracted building number from address line 1';
COMMENT ON COLUMN clean_addresses.street_name IS 'Street name without building number';
COMMENT ON COLUMN clean_addresses.city IS 'City name';
COMMENT ON COLUMN clean_addresses.state IS 'State or province';
COMMENT ON COLUMN clean_addresses.postal_code IS 'Postal or ZIP code';
COMMENT ON COLUMN clean_addresses.country IS 'Country name';
COMMENT ON COLUMN clean_addresses.is_active IS 'Active status of the address';
COMMENT ON COLUMN clean_addresses.created_date IS 'Record creation timestamp';
COMMENT ON COLUMN clean_addresses.modified_date IS 'Record last modification timestamp';

-- Create trigger for modified_date
CREATE OR REPLACE TRIGGER trg_clean_addresses_modify
    BEFORE UPDATE ON clean_addresses
    FOR EACH ROW
BEGIN
    :NEW.modified_date := SYSTIMESTAMP;
END;
/

-- Create procedure to populate clean addresses
CREATE OR REPLACE PROCEDURE populate_clean_addresses IS
BEGIN
    -- Insert data from locations table
    INSERT INTO clean_addresses (
        employee_id,
        address_type,
        building_number,
        street_name,
        city,
        state,
        postal_code,
        country
    )
    SELECT 
        e.employee_id,
        'WORK' as address_type,
        extract_building_number(l.street_address) as building_number,
        REGEXP_REPLACE(l.street_address, '^[0-9]+[A-Za-z]*\s*', '') as street_name,
        l.city,
        l.state_province,
        l.postal_code,
        c.country_name
    FROM 
        employees e
        JOIN departments d ON e.department_id = d.department_id
        JOIN locations l ON d.location_id = l.location_id
        JOIN countries c ON l.country_id = c.country_id
    WHERE 
        e.employee_id NOT IN (SELECT employee_id FROM clean_addresses);
        
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END populate_clean_addresses;
/

-- Add comment
COMMENT ON PROCEDURE populate_clean_addresses IS 'Populates the clean_addresses table with standardized address information from the locations table'; 