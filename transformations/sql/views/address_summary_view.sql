-- Create view for address summary
CREATE OR REPLACE VIEW address_summary_view AS
SELECT 
    a.address_id,
    a.employee_id as person_id,
    a.address_type,
    a.full_address,
    a.main_building_number,
    a.sub_building_number,
    a.street_name,
    a.town_and_city,
    a.post_code,
    a.country,
    a.is_valid,
    a.validation_errors,
    a.is_active,
    a.created_date,
    a.modified_date
FROM 
    addresses a
WHERE 
    a.is_active = 'Y';

-- Add comment
COMMENT ON TABLE address_summary_view IS 'Provides a summary view of address information with standardized components, using person_id for identification'; 