-- Function to extract main building number
CREATE OR REPLACE FUNCTION extract_main_building_number(
    p_address IN VARCHAR2
) RETURN VARCHAR2 IS
    v_building_number VARCHAR2(20);
BEGIN
    -- Extract the first number (and any following letters) from the start of the address
    SELECT REGEXP_SUBSTR(p_address, '^[0-9]+[A-Za-z]*')
    INTO v_building_number
    FROM DUAL;
    
    RETURN v_building_number;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        RETURN NULL;
END extract_main_building_number;
/

-- Function to extract sub-building number
CREATE OR REPLACE FUNCTION extract_sub_building_number(
    p_address IN VARCHAR2
) RETURN VARCHAR2 IS
    v_sub_building VARCHAR2(50);
BEGIN
    -- Extract apartment, floor, unit, suite, or flat numbers
    SELECT REGEXP_SUBSTR(p_address, '(?:APT|FL|UNIT|SUITE|FLAT)\s*[0-9]+[A-Za-z]*', 1, 1)
    INTO v_sub_building
    FROM DUAL;
    
    RETURN v_sub_building;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        RETURN NULL;
END extract_sub_building_number;
/

-- Function to extract street name
CREATE OR REPLACE FUNCTION extract_street_name(
    p_address IN VARCHAR2
) RETURN VARCHAR2 IS
    v_street_name VARCHAR2(100);
BEGIN
    -- Remove both main building number and sub-building number to get street name
    SELECT TRIM(REGEXP_REPLACE(
        REGEXP_REPLACE(p_address, '^[0-9]+[A-Za-z]*\s*', ''),
        '(?:APT|FL|UNIT|SUITE|FLAT)\s*[0-9]+[A-Za-z]*\s*',
        ''
    ))
    INTO v_street_name
    FROM DUAL;
    
    RETURN v_street_name;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        RETURN NULL;
END extract_street_name;
/

-- Grant execute permissions
GRANT EXECUTE ON hr.extract_main_building_number TO hr;
GRANT EXECUTE ON hr.extract_sub_building_number TO hr;
GRANT EXECUTE ON hr.extract_street_name TO hr;

-- Add comments
COMMENT ON FUNCTION hr.extract_main_building_number IS 'Extracts main building number from the beginning of an address line';
COMMENT ON FUNCTION hr.extract_sub_building_number IS 'Extracts sub-building numbers (apartment, floor, unit, suite, flat) from address line';
COMMENT ON FUNCTION hr.extract_street_name IS 'Extracts street name after removing building numbers'; 