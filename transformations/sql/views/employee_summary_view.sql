-- Employee Summary View
-- This view provides a comprehensive summary of employee information
-- including current position, department, and salary information

CREATE OR REPLACE VIEW employee_summary AS
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
    employees e
    LEFT JOIN jobs j ON e.job_id = j.job_id
    LEFT JOIN departments d ON e.department_id = d.department_id
    LEFT JOIN employees m ON e.manager_id = m.employee_id;

-- Add comment to the view
COMMENT ON TABLE employee_summary IS 'Provides a comprehensive summary of employee information including current position, department, and salary information'; 