-- Data Validation Queries
-- This file contains queries for validating data quality and consistency

-- Check for Missing Required Fields
SELECT 'workers' as table_name,
       COUNT(*) as total_records,
       SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) as missing_first_name,
       SUM(CASE WHEN last_name IS NULL THEN 1 ELSE 0 END) as missing_last_name,
       SUM(CASE WHEN employee_number IS NULL THEN 1 ELSE 0 END) as missing_employee_number
FROM workers
UNION ALL
SELECT 'addresses' as table_name,
       COUNT(*) as total_records,
       SUM(CASE WHEN address_line1 IS NULL THEN 1 ELSE 0 END) as missing_address_line1,
       SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END) as missing_city,
       SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) as missing_country
FROM addresses;

-- Check for Date Range Validity
SELECT 'workers' as table_name,
       COUNT(*) as total_records,
       SUM(CASE WHEN effective_from > effective_to THEN 1 ELSE 0 END) as invalid_date_range,
       SUM(CASE WHEN effective_from > CURRENT_DATE THEN 1 ELSE 0 END) as future_start_date,
       SUM(CASE WHEN effective_to < CURRENT_DATE THEN 1 ELSE 0 END) as expired_records
FROM workers
UNION ALL
SELECT 'addresses',
       COUNT(*),
       SUM(CASE WHEN effective_from > effective_to THEN 1 ELSE 0 END),
       SUM(CASE WHEN effective_from > CURRENT_DATE THEN 1 ELSE 0 END),
       SUM(CASE WHEN effective_to < CURRENT_DATE THEN 1 ELSE 0 END)
FROM addresses
UNION ALL
SELECT 'assignments',
       COUNT(*),
       SUM(CASE WHEN effective_from > effective_to THEN 1 ELSE 0 END),
       SUM(CASE WHEN effective_from > CURRENT_DATE THEN 1 ELSE 0 END),
       SUM(CASE WHEN effective_to < CURRENT_DATE THEN 1 ELSE 0 END)
FROM assignments
UNION ALL
SELECT 'communications',
       COUNT(*),
       SUM(CASE WHEN effective_from > effective_to THEN 1 ELSE 0 END),
       SUM(CASE WHEN effective_from > CURRENT_DATE THEN 1 ELSE 0 END),
       SUM(CASE WHEN effective_to < CURRENT_DATE THEN 1 ELSE 0 END)
FROM communications;

-- Check for Duplicate Records
SELECT 'duplicate_workers' as check_type,
       employee_number,
       COUNT(*) as record_count
FROM workers
GROUP BY employee_number
HAVING COUNT(*) > 1
UNION ALL
SELECT 'duplicate_assignments',
       assignment_number,
       COUNT(*)
FROM assignments
GROUP BY assignment_number
HAVING COUNT(*) > 1;

-- Check for Orphaned Records
SELECT 'orphaned_addresses' as check_type,
       COUNT(*) as orphaned_count
FROM addresses a
LEFT JOIN workers w ON a.worker_unique_id = w.unique_id
WHERE w.unique_id IS NULL
UNION ALL
SELECT 'orphaned_assignments',
       COUNT(*)
FROM assignments a
LEFT JOIN workers w ON a.worker_unique_id = w.unique_id
WHERE w.unique_id IS NULL
UNION ALL
SELECT 'orphaned_communications',
       COUNT(*)
FROM communications c
LEFT JOIN workers w ON c.worker_unique_id = w.unique_id
WHERE w.unique_id IS NULL;

-- Check for Data Consistency
SELECT w.unique_id,
       w.first_name,
       w.last_name,
       COUNT(DISTINCT a.address_type) as address_count,
       COUNT(DISTINCT c.contact_type) as contact_count,
       COUNT(DISTINCT asg.assignment_number) as assignment_count
FROM workers w
LEFT JOIN addresses a ON w.unique_id = a.worker_unique_id
LEFT JOIN communications c ON w.unique_id = c.worker_unique_id
LEFT JOIN assignments asg ON w.unique_id = asg.worker_unique_id
GROUP BY w.unique_id, w.first_name, w.last_name
HAVING COUNT(DISTINCT a.address_type) = 0
    OR COUNT(DISTINCT c.contact_type) = 0
    OR COUNT(DISTINCT asg.assignment_number) = 0;

-- Check for Valid Reference Data
SELECT DISTINCT nationality
FROM workers
WHERE nationality NOT IN ('US', 'UK', 'CA', 'AU', 'FR', 'DE', 'IT', 'ES', 'PT', 'NL', 'BE', 'CH', 'AT', 'SE', 'NO', 'DK', 'FI', 'IE', 'NZ');

-- Check Primary Contact Flags
SELECT worker_unique_id,
       contact_type,
       COUNT(*) as total_contacts,
       SUM(CASE WHEN primary_flag = 'Y' THEN 1 ELSE 0 END) as primary_contacts
FROM communications
GROUP BY worker_unique_id, contact_type
HAVING SUM(CASE WHEN primary_flag = 'Y' THEN 1 ELSE 0 END) > 1; 