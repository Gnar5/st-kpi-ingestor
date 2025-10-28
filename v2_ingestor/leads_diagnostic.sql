-- LEADS DIAGNOSTIC: Phoenix-Sales 8/18-8/24/2025
-- VALIDATED AGAINST SERVICETITAN EXPORT - MATCHES 96 UNIQUE CUSTOMERS, 97 JOBS
-- Key findings:
--   1. Use dim_jobs table (has businessUnitNormalized field)
--   2. Use 2025 dates (not 2024)
--   3. Filter: job_type_lower LIKE '%estimate%'
--   4. Exclude: customer_name_lower NOT LIKE '%test%'
--   5. Count: COUNT(DISTINCT customerId) for leads

-- Step 1: What jobs were created in this period?
WITH jobs_in_range AS (
  SELECT
    j.id as job_id,
    j.jobNumber,
    j.customerId,
    j.jobTypeName,
    j.jobStatus,
    j.businessUnitNormalized,
    j.createdOn,
    DATE(j.createdOn, 'America/Phoenix') as created_date_az,
    LOWER(j.jobTypeName) as job_type_lower
  FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
  WHERE DATE(j.createdOn, 'America/Phoenix') BETWEEN '2025-08-18' AND '2025-08-24'
),

-- Step 2: Join with customers to get names and filter test customers
jobs_with_customers AS (
  SELECT
    j.*,
    c.name as customerName,
    LOWER(COALESCE(c.name, '')) as customer_name_lower
  FROM jobs_in_range j
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_customers` c ON j.customerId = c.id
),

-- Step 3: Filter to Phoenix-Sales
phoenix_jobs AS (
  SELECT *
  FROM jobs_with_customers
  WHERE businessUnitNormalized = 'Phoenix-Sales'
),

-- Step 4: Filter to estimate jobs
estimate_jobs AS (
  SELECT *
  FROM phoenix_jobs
  WHERE job_type_lower LIKE '%estimate%'
),

-- Step 5: Exclude test customers
real_customers AS (
  SELECT *
  FROM estimate_jobs
  WHERE customer_name_lower NOT LIKE '%test%'
),

-- Step 6: Count distinct customers
final_count AS (
  SELECT
    COUNT(DISTINCT customerId) as leads_count,
    COUNT(*) as total_jobs,
    COUNT(DISTINCT customerName) as distinct_customer_names
  FROM real_customers
)

-- Show the breakdown (Expected: 96 leads, 97 jobs for Phoenix-Sales)
SELECT
  'Jobs Created 8/18-8/24' as step,
  COUNT(*) as jobs,
  COUNT(DISTINCT customerId) as unique_customers
FROM jobs_in_range

UNION ALL

SELECT
  'Phoenix-Sales Jobs' as step,
  COUNT(*) as jobs,
  COUNT(DISTINCT customerId) as unique_customers
FROM phoenix_jobs

UNION ALL

SELECT
  'Estimate Jobs' as step,
  COUNT(*) as jobs,
  COUNT(DISTINCT customerId) as unique_customers
FROM estimate_jobs

UNION ALL

SELECT
  'Excluding Test Customers' as step,
  COUNT(*) as jobs,
  COUNT(DISTINCT customerId) as unique_customers
FROM real_customers

UNION ALL

SELECT
  'FINAL: LEADS COUNT' as step,
  total_jobs as jobs,
  leads_count as unique_customers
FROM final_count;

-- OPTIONAL: Show which customer has multiple jobs (should be 1 customer with 2 jobs)
-- SELECT
--   customerId,
--   customerName,
--   COUNT(*) as job_count,
--   STRING_AGG(jobNumber || ' (' || jobStatus || ')' ORDER BY jobNumber) as jobs
-- FROM real_customers
-- GROUP BY customerId, customerName
-- HAVING COUNT(*) > 1;

-- OPTIONAL: Show sample of leads
-- SELECT
--   customerId,
--   customerName,
--   jobNumber,
--   jobStatus,
--   jobTypeName,
--   created_date_az
-- FROM real_customers
-- ORDER BY customerName
-- LIMIT 20;
