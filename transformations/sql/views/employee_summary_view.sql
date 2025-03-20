-- Employee Summary View
-- This view provides a comprehensive summary of employee information
-- including current position, department, and salary information

CREATE OR REPLACE VIEW hr.employee_summary AS
SELECT 
    e.employee_id,
    e.first_name || ' ' || e.last_name as full_name,
    e.email,
    e.phone_number,
    e.hire_date,
    j.job_title,
    d.department_name,
    e.salary,
    e.commission_pct,
    m.first_name || ' ' || m.last_name as manager_name
FROM 
    hr.employees e
    LEFT JOIN hr.jobs j ON e.job_id = j.job_id
    LEFT JOIN hr.departments d ON e.department_id = d.department_id
    LEFT JOIN hr.employees m ON e.manager_id = m.employee_id;

-- Grant access to the view
GRANT SELECT ON hr.employee_summary TO hr;

-- Add comment to the view
COMMENT ON TABLE hr.employee_summary IS 'Provides a comprehensive summary of employee information including current position, department, and salary information'; 