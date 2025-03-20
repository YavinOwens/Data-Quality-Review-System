-- Analytics Queries
-- This file contains various analytical queries for workforce data

-- Worker Demographics
SELECT 
    nationality,
    COUNT(*) as worker_count,
    AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, birth_date))) as avg_age,
    sex,
    marital_status
FROM workers
GROUP BY nationality, sex, marital_status
ORDER BY worker_count DESC;

-- Assignment Statistics
SELECT 
    department_id,
    COUNT(DISTINCT worker_unique_id) as worker_count,
    COUNT(*) as total_assignments,
    AVG(EXTRACT(DAYS FROM (effective_to - effective_from))) as avg_assignment_duration_days
FROM assignments
GROUP BY department_id
ORDER BY worker_count DESC;

-- Contact Method Analysis
SELECT 
    contact_type,
    COUNT(*) as total_contacts,
    SUM(CASE WHEN primary_flag = 'Y' THEN 1 ELSE 0 END) as primary_contacts,
    ROUND(AVG(CASE WHEN primary_flag = 'Y' THEN 100.0 ELSE 0 END), 2) as primary_percentage
FROM communications
GROUP BY contact_type
ORDER BY total_contacts DESC;

-- Geographic Distribution
SELECT 
    a.country,
    a.state,
    COUNT(DISTINCT a.worker_unique_id) as worker_count,
    COUNT(*) as address_count
FROM addresses a
GROUP BY a.country, a.state
ORDER BY worker_count DESC;

-- Age Distribution by Department
SELECT 
    a.department_id,
    COUNT(DISTINCT w.unique_id) as worker_count,
    AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, w.birth_date))) as avg_age,
    MIN(EXTRACT(YEAR FROM AGE(CURRENT_DATE, w.birth_date))) as min_age,
    MAX(EXTRACT(YEAR FROM AGE(CURRENT_DATE, w.birth_date))) as max_age
FROM workers w
JOIN assignments a ON w.unique_id = a.worker_unique_id
WHERE a.effective_to > CURRENT_DATE
GROUP BY a.department_id
ORDER BY worker_count DESC;

-- Assignment Duration Analysis
SELECT 
    department_id,
    AVG(EXTRACT(DAYS FROM (effective_to - effective_from))) as avg_duration,
    MIN(EXTRACT(DAYS FROM (effective_to - effective_from))) as min_duration,
    MAX(EXTRACT(DAYS FROM (effective_to - effective_from))) as max_duration,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(DAYS FROM (effective_to - effective_from))) as median_duration
FROM assignments
GROUP BY department_id
ORDER BY avg_duration DESC;

-- Worker Nationality Distribution by Department
SELECT 
    a.department_id,
    w.nationality,
    COUNT(*) as worker_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY a.department_id), 2) as percentage
FROM workers w
JOIN assignments a ON w.unique_id = a.worker_unique_id
WHERE a.effective_to > CURRENT_DATE
GROUP BY a.department_id, w.nationality
ORDER BY a.department_id, worker_count DESC;

-- Contact Type Distribution by Country
SELECT 
    a.country,
    c.contact_type,
    COUNT(*) as contact_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY a.country), 2) as percentage
FROM addresses a
JOIN communications c ON a.worker_unique_id = c.worker_unique_id
WHERE a.effective_to > CURRENT_DATE
  AND c.effective_to > CURRENT_DATE
GROUP BY a.country, c.contact_type
ORDER BY a.country, contact_count DESC; 