-- Execution script for address transformations
-- This script runs all components in the correct order

-- Set server output on for better visibility
SET SERVEROUTPUT ON;

-- Step 1: Create transformation log
BEGIN
    create_transformation_log;
    DBMS_OUTPUT.PUT_LINE('Transformation log created successfully');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error creating transformation log: ' || SQLERRM);
        RAISE;
END;
/

-- Step 2: Create address extraction functions
@functions/extract_address_components.sql
DBMS_OUTPUT.PUT_LINE('Address extraction functions created successfully');
/

-- Step 3: Create address table and related objects
BEGIN
    create_address_table;
    DBMS_OUTPUT.PUT_LINE('Address table and related objects created successfully');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error creating address table: ' || SQLERRM);
        RAISE;
END;
/

-- Step 4: Create address summary view
@views/address_summary_view.sql
DBMS_OUTPUT.PUT_LINE('Address summary view created successfully');
/

-- Step 5: Populate the addresses table
BEGIN
    populate_addresses;
    DBMS_OUTPUT.PUT_LINE('Addresses populated successfully');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error populating addresses: ' || SQLERRM);
        RAISE;
END;
/

-- Step 6: Verify the results
DECLARE
    v_count NUMBER;
BEGIN
    -- Check number of addresses
    SELECT COUNT(*) INTO v_count FROM addresses;
    DBMS_OUTPUT.PUT_LINE('Total addresses processed: ' || v_count);
    
    -- Check number of transformation log entries
    SELECT COUNT(*) INTO v_count FROM transformation_log;
    DBMS_OUTPUT.PUT_LINE('Total transformation log entries: ' || v_count);
    
    -- Check number of addresses in summary view
    SELECT COUNT(*) INTO v_count FROM address_summary_view;
    DBMS_OUTPUT.PUT_LINE('Total addresses in summary view: ' || v_count);
END;
/

-- Add comment
COMMENT ON TABLE execute_transformations IS 'Main execution script for address transformations that runs all components in the correct order'; 