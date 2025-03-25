-- Rollback script for clean supplier address transformations
CREATE OR REPLACE PROCEDURE rollback_clean_address IS
BEGIN
    -- Drop trigger
    EXECUTE IMMEDIATE 'DROP TRIGGER trg_clean_addresses_modify';
EXCEPTION
    WHEN OTHERS THEN
        NULL; -- Trigger doesn't exist, continue
END;
/

-- Drop function
EXECUTE IMMEDIATE 'DROP FUNCTION extract_building_number';
EXCEPTION
    WHEN OTHERS THEN
        NULL; -- Function doesn't exist, continue
END;
/

-- Drop procedure
EXECUTE IMMEDIATE 'DROP PROCEDURE populate_clean_addresses';
EXCEPTION
    WHEN OTHERS THEN
        NULL; -- Procedure doesn't exist, continue
END;
/

-- Drop table
EXECUTE IMMEDIATE 'DROP TABLE clean_addresses';
EXCEPTION
    WHEN OTHERS THEN
        NULL; -- Table doesn't exist, continue
END;
/

-- Add comment
COMMENT ON PROCEDURE rollback_clean_address IS 'Rolls back all clean supplier address transformations including table, function, trigger, and procedures'; 