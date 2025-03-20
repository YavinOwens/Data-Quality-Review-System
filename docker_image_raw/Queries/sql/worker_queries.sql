-- Worker Queries
-- This file contains various queries related to worker information

-- Basic Worker Information
SELECT 
    w.first_name,
    w.last_name,
    w.employee_number,
    w.birth_date,
    w.nationality
FROM workers w
ORDER BY w.last_name, w.first_name;

-- Worker Contact Information
SELECT 
    w.first_name,
    w.last_name,
    c.contact_type,
    c.contact_value,
    c.primary_flag
FROM workers w
JOIN communications c ON w.unique_id = c.worker_unique_id
ORDER BY w.last_name, w.first_name, c.contact_type;

-- Worker Addresses
SELECT 
    w.first_name,
    w.last_name,
    a.address_type,
    a.address_line1,
    a.city,
    a.state,
    a.country
FROM workers w
JOIN addresses a ON w.unique_id = a.worker_unique_id
ORDER BY w.last_name, w.first_name, a.address_type;

-- Worker Assignments
SELECT 
    w.first_name,
    w.last_name,
    a.assignment_number,
    a.position_id,
    a.department_id,
    a.location_id,
    a.effective_from,
    a.effective_to
FROM workers w
JOIN assignments a ON w.unique_id = a.worker_unique_id
ORDER BY w.last_name, w.first_name, a.effective_from;

-- Worker Complete Profile
SELECT 
    w.first_name,
    w.last_name,
    w.employee_number,
    w.birth_date,
    w.nationality,
    w.sex,
    w.marital_status,
    a.address_line1,
    a.city,
    a.state,
    a.country,
    c.contact_type,
    c.contact_value,
    asg.assignment_number,
    asg.position_id,
    asg.department_id
FROM workers w
LEFT JOIN addresses a ON w.unique_id = a.worker_unique_id
LEFT JOIN communications c ON w.unique_id = c.worker_unique_id
LEFT JOIN assignments asg ON w.unique_id = asg.worker_unique_id
WHERE a.effective_to > CURRENT_DATE
  AND c.effective_to > CURRENT_DATE
  AND asg.effective_to > CURRENT_DATE
ORDER BY w.last_name, w.first_name; 