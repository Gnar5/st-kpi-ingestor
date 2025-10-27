-- LEADS DIAGNOSTIC: Phoenix-Sales 8/18-8/24/2024
-- Let's see what's in the raw data vs what we're counting

-- Step 1: What jobs were created in this period?
WITH jobs_in_range AS (
  SELECT
    j.id as job_id,
    j.jobNumber,
    j.customerId,
    j.customerName,
    j.jobTypeName,
    j.businessUnitId,
    j.businessUnitName,
    j.createdOn,
    DATE(j.createdOn, 'America/Phoenix') as created_date_az,
    LOWER(j.jobTypeName) as job_type_lower,
    LOWER(COALESCE(j.customerName, '')) as customer_name_lower
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
  WHERE DATE(j.createdOn, 'America/Phoenix') BETWEEN '2024-08-18' AND '2024-08-24'
),

-- Step 2: Filter to Phoenix-Sales
phoenix_jobs AS (
  SELECT *
  FROM jobs_in_range
  WHERE businessUnitName = 'Phoenix-Sales'
    OR businessUnitName LIKE 'Phoenix%Sales%'
),

-- Step 3: Filter to estimate jobs
estimate_jobs AS (
  SELECT *
  FROM phoenix_jobs
  WHERE job_type_lower LIKE '%estimate%'
),

-- Step 4: Exclude test customers
real_customers AS (
  SELECT *
  FROM estimate_jobs
  WHERE customer_name_lower NOT LIKE '%test%'
),

-- Step 5: Count distinct customers
final_count AS (
  SELECT
    COUNT(DISTINCT customerId) as leads_count,
    COUNT(*) as total_jobs,
    COUNT(DISTINCT customerName) as distinct_customer_names
  FROM real_customers
)

-- Show the breakdown
SELECT
  'Jobs Created 8/18-8/24' as step,
  COUNT(*) as count
FROM jobs_in_range

UNION ALL

SELECT
  'Phoenix-Sales Jobs' as step,
  COUNT(*) as count
FROM phoenix_jobs

UNION ALL

SELECT
  'Estimate Jobs' as step,
  COUNT(*) as count
FROM estimate_jobs

UNION ALL

SELECT
  'Excluding Test Customers' as step,
  COUNT(*) as count
FROM real_customers

UNION ALL

SELECT
  'FINAL: Distinct Customers (LEADS)' as step,
  leads_count as count
FROM final_count

UNION ALL

SELECT
  'Distinct Customer Names' as step,
  distinct_customer_names as count
FROM final_count;

-- Also show what business unit names we actually have
-- SELECT DISTINCT businessUnitName
-- FROM `kpi-auto-471020.st_raw_v2.raw_jobs`
-- WHERE businessUnitName LIKE '%Phoenix%'
-- ORDER BY businessUnitName;

-- Show sample of what we're counting as leads
-- SELECT
--   customerId,
--   customerName,
--   COUNT(*) as estimate_jobs,
--   MIN(jobNumber) as first_job,
--   MIN(DATE(createdOn, 'America/Phoenix')) as first_date
-- FROM real_customers
-- GROUP BY customerId, customerName
-- ORDER BY estimate_jobs DESC
-- LIMIT 20;
