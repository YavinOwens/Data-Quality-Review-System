-- Function to extract main building number from address line 1
CREATE OR REPLACE FUNCTION extract_main_building_number(
    p_address_line IN VARCHAR2
) RETURN VARCHAR2 IS
    v_building_number VARCHAR2(20);
BEGIN
    -- Extract the first number (and any following letters) from the start of the address
    SELECT REGEXP_SUBSTR(p_address_line, '^[0-9]+[A-Za-z]*')
    INTO v_building_number
    FROM DUAL;
    
    RETURN v_building_number;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        RETURN NULL;
END;
/

-- Function to extract sub-building number (apartment, floor, etc.)
CREATE OR REPLACE FUNCTION extract_sub_building_number(
    p_address_line IN VARCHAR2
) RETURN VARCHAR2 IS
    v_sub_building VARCHAR2(20);
BEGIN
    -- Extract apartment, floor, unit, suite, or flat numbers
    SELECT REGEXP_SUBSTR(p_address_line, '(?:APT|FL|UNIT|SUITE|FLAT)\s*[0-9]+[A-Za-z]*', 1, 1, 'i')
    INTO v_sub_building
    FROM DUAL;
    
    RETURN v_sub_building;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        RETURN NULL;
END;
/

-- Function to extract street name from address line 1
CREATE OR REPLACE FUNCTION extract_street_name(
    p_address_line IN VARCHAR2
) RETURN VARCHAR2 IS
    v_street_name VARCHAR2(200);
BEGIN
    -- Remove both main building number and sub-building number to get street name
    SELECT TRIM(REGEXP_REPLACE(
        REGEXP_REPLACE(p_address_line, '^[0-9]+[A-Za-z]*\s*', ''),
        '(?:APT|FL|UNIT|SUITE|FLAT)\s*[0-9]+[A-Za-z]*\s*', ''
    ))
    INTO v_street_name
    FROM DUAL;
    
    RETURN v_street_name;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        RETURN NULL;
END;
/

-- Add comments
COMMENT ON FUNCTION extract_main_building_number IS 'Extracts the main building number from the start of an address line';
COMMENT ON FUNCTION extract_sub_building_number IS 'Extracts apartment, floor, unit, suite, or flat numbers from an address line';
COMMENT ON FUNCTION extract_street_name IS 'Extracts the street name by removing building numbers from an address line'; 