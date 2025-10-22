-- ============================================================================
-- LEADS VALIDATION QUERIES
-- ============================================================================
-- Testing new Leads definition (case-insensitive 'estimate', no COMM exclusion, exclude test customers)
-- Date: 2025-08-18
-- ============================================================================

-- ----------------------------------------------------------------------------
-- (a) DAILY LEADS BY BU - NEW LOGIC
-- ----------------------------------------------------------------------------
-- Shows lead counts for Aug 18 by BU using the new definition
WITH sales_units AS (
  SELECT businessUnit FROM UNNEST([
    'Andy\'s Painting Sales',
    'Commercial-AZ-Sales',
    'Guaranteed Painting-Sales',
    'Nevada-Sales',
    'Phoenix-Sales',
    'Tucson-Sales'
  ]) AS businessUnit
)
SELECT
  DATE(j.createdOn) as event_date,
  j.businessUnitNormalized as business_unit,
  COUNT(DISTINCT j.customerId) as lead_count_new,
  COUNT(*) as total_jobs
FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
INNER JOIN sales_units s ON j.businessUnitNormalized = s.businessUnit
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_customers` c ON j.customerId = c.id
WHERE LOWER(j.jobTypeName) LIKE '%estimate%'
  AND (c.name IS NULL OR LOWER(c.name) NOT LIKE '%test%')
  AND DATE(j.createdOn) = '2025-08-18'
GROUP BY event_date, business_unit
ORDER BY business_unit;

-- Expected results for 2025-08-18:
-- Phoenix-Sales: ~16-19 leads (includes COMM estimates now)
-- Tucson-Sales: ~7-8 leads
-- Nevada-Sales: ~4 leads


-- ----------------------------------------------------------------------------
-- (b) RAW BREAKDOWN - 10 SAMPLE ROWS
-- ----------------------------------------------------------------------------
-- Shows specific jobs/customers that qualify under new logic
WITH sales_units AS (
  SELECT businessUnit FROM UNNEST([
    'Andy\'s Painting Sales',
    'Commercial-AZ-Sales',
    'Guaranteed Painting-Sales',
    'Nevada-Sales',
    'Phoenix-Sales',
    'Tucson-Sales'
  ]) AS businessUnit
)
SELECT
  DATE(j.createdOn) as created_date,
  j.businessUnitNormalized as business_unit,
  j.customerId,
  c.name as customer_name,
  j.jobTypeName,
  j.jobStatus,
  j.jobNumber
FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
INNER JOIN sales_units s ON j.businessUnitNormalized = s.businessUnit
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_customers` c ON j.customerId = c.id
WHERE LOWER(j.jobTypeName) LIKE '%estimate%'
  AND (c.name IS NULL OR LOWER(c.name) NOT LIKE '%test%')
  AND DATE(j.createdOn) = '2025-08-18'
  AND j.businessUnitNormalized = 'Phoenix-Sales'
ORDER BY j.createdOn
LIMIT 10;


-- ----------------------------------------------------------------------------
-- (c) DELTA QUERY - OLD VS NEW LOGIC
-- ----------------------------------------------------------------------------
-- Compares old logic (no COMM, case-sensitive) vs new logic (includes COMM, case-insensitive, exclude test)
WITH sales_units AS (
  SELECT businessUnit FROM UNNEST([
    'Andy\'s Painting Sales',
    'Commercial-AZ-Sales',
    'Guaranteed Painting-Sales',
    'Nevada-Sales',
    'Phoenix-Sales',
    'Tucson-Sales'
  ]) AS businessUnit
),

old_logic AS (
  SELECT
    DATE(j.createdOn) as event_date,
    j.businessUnitNormalized as business_unit,
    COUNT(DISTINCT j.customerId) as lead_count
  FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
  INNER JOIN sales_units s ON j.businessUnitNormalized = s.businessUnit
  WHERE j.jobTypeName NOT LIKE '%COMM.%'
    AND DATE(j.createdOn) = '2025-08-18'
  GROUP BY event_date, business_unit
),

new_logic AS (
  SELECT
    DATE(j.createdOn) as event_date,
    j.businessUnitNormalized as business_unit,
    COUNT(DISTINCT j.customerId) as lead_count
  FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
  INNER JOIN sales_units s ON j.businessUnitNormalized = s.businessUnit
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_customers` c ON j.customerId = c.id
  WHERE LOWER(j.jobTypeName) LIKE '%estimate%'
    AND (c.name IS NULL OR LOWER(c.name) NOT LIKE '%test%')
    AND DATE(j.createdOn) = '2025-08-18'
  GROUP BY event_date, business_unit
)

SELECT
  COALESCE(o.business_unit, n.business_unit) as business_unit,
  COALESCE(o.lead_count, 0) as old_lead_count,
  COALESCE(n.lead_count, 0) as new_lead_count,
  COALESCE(n.lead_count, 0) - COALESCE(o.lead_count, 0) as delta,
  ROUND(SAFE_DIVIDE(COALESCE(n.lead_count, 0) - COALESCE(o.lead_count, 0), COALESCE(o.lead_count, 1)) * 100, 2) as pct_change
FROM old_logic o
FULL OUTER JOIN new_logic n ON o.event_date = n.event_date AND o.business_unit = n.business_unit
ORDER BY business_unit;

-- Expected delta:
-- Old logic counted ALL job types (not just estimates), excluded COMM
-- New logic counts ONLY estimates (case-insensitive), INCLUDES COMM, excludes test customers
-- Delta should show fewer leads for new logic since we're now filtering to estimates only


-- ----------------------------------------------------------------------------
-- (d) JOB TYPE BREAKDOWN - What's included now
-- ----------------------------------------------------------------------------
-- Shows all job types matching new estimate filter
SELECT
  j.jobTypeName,
  COUNT(DISTINCT j.customerId) as unique_customers,
  COUNT(*) as total_jobs,
  CASE
    WHEN LOWER(j.jobTypeName) LIKE '%comm%' THEN 'COMMERCIAL'
    WHEN LOWER(j.jobTypeName) LIKE '%res%' THEN 'RESIDENTIAL'
    ELSE 'OTHER'
  END as job_category
FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_customers` c ON j.customerId = c.id
WHERE LOWER(j.jobTypeName) LIKE '%estimate%'
  AND (c.name IS NULL OR LOWER(c.name) NOT LIKE '%test%')
  AND DATE(j.createdOn) = '2025-08-18'
  AND j.businessUnitNormalized IN ('Phoenix-Sales', 'Tucson-Sales', 'Nevada-Sales')
GROUP BY j.jobTypeName, job_category
ORDER BY job_category, unique_customers DESC;
