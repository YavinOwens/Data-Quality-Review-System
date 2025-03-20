-- Rollback script for supplier address transformations
CREATE OR REPLACE PROCEDURE rollback_address IS
BEGIN
    -- Drop view
    EXECUTE IMMEDIATE 'DROP VIEW address_summary_view';
EXCEPTION
    WHEN OTHERS THEN
        NULL; -- View doesn't exist, continue
END;
/

-- Drop trigger
EXECUTE IMMEDIATE 'DROP TRIGGER trg_addresses_modify';
EXCEPTION
    WHEN OTHERS THEN
        NULL; -- Trigger doesn't exist, continue
END;
/

-- Drop functions
EXECUTE IMMEDIATE 'DROP FUNCTION extract_main_building_number';
EXCEPTION
    WHEN OTHERS THEN
        NULL; -- Function doesn't exist, continue
END;
/

EXECUTE IMMEDIATE 'DROP FUNCTION extract_sub_building_number';
EXCEPTION
    WHEN OTHERS THEN
        NULL; -- Function doesn't exist, continue
END;
/

EXECUTE IMMEDIATE 'DROP FUNCTION extract_street_name';
EXCEPTION
    WHEN OTHERS THEN
        NULL; -- Function doesn't exist, continue
END;
/

-- Drop procedure
EXECUTE IMMEDIATE 'DROP PROCEDURE populate_addresses';
EXCEPTION
    WHEN OTHERS THEN
        NULL; -- Procedure doesn't exist, continue
END;
/

-- Drop table
EXECUTE IMMEDIATE 'DROP TABLE addresses';
EXCEPTION
    WHEN OTHERS THEN
        NULL; -- Table doesn't exist, continue
END;
/

-- Add comment
COMMENT ON PROCEDURE rollback_address IS 'Rolls back all supplier address transformations including table, functions, trigger, procedures, and views'; 