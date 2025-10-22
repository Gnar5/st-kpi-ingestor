-- ============================================================================
-- TOTAL BOOKED DIAGNOSTICS - Multi-Basis Comparison Matrix
-- ============================================================================
-- Purpose: Determine authoritative definition for Total Booked that matches ST UI
-- Date tested: 2025-08-18
-- ST UI values for reference:
--   Phoenix-Sales: $30,241.51
--   Tucson-Sales: $4,844.58
--   Nevada-Sales: $27,150.00
-- ============================================================================

WITH
-- Sales business units (with Phoenix merge)
sales_units AS (
  SELECT businessUnit FROM UNNEST([
    'Andy\'s Painting Sales',
    'Commercial-AZ-Sales',
    'Guaranteed Painting-Sales',
    'Nevada-Sales',
    'Phoenix-Sales',
    'Tucson-Sales'
  ]) AS businessUnit
),

-- Test date and BUs
test_params AS (
  SELECT '2025-08-18' as test_date, 'Phoenix-Sales' as test_bu
  UNION ALL SELECT '2025-08-18', 'Tucson-Sales'
  UNION ALL SELECT '2025-08-18', 'Nevada-Sales'
),

-- ============================================================================
-- BASIS 1: Invoice Date (invoice.invoiceDate = target_date)
-- ============================================================================
invoice_date_all AS (
  SELECT
    DATE(i.invoiceDate) as event_date,
    j.businessUnitNormalized as business_unit,
    'invoice_date' as basis,
    'all_jobs' as filter_desc,
    SUM(i.total) as total_booked,
    COUNT(DISTINCT i.id) as num_invoices,
    COUNT(DISTINCT j.id) as num_jobs
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
  INNER JOIN sales_units s ON j.businessUnitNormalized = s.businessUnit
  CROSS JOIN test_params p
  WHERE DATE(i.invoiceDate) = p.test_date
    AND j.businessUnitNormalized = p.test_bu
  GROUP BY event_date, business_unit
),

invoice_date_no_comm AS (
  SELECT
    DATE(i.invoiceDate) as event_date,
    j.businessUnitNormalized as business_unit,
    'invoice_date' as basis,
    'exclude_comm' as filter_desc,
    SUM(i.total) as total_booked,
    COUNT(DISTINCT i.id) as num_invoices,
    COUNT(DISTINCT j.id) as num_jobs
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
  INNER JOIN sales_units s ON j.businessUnitNormalized = s.businessUnit
  CROSS JOIN test_params p
  WHERE DATE(i.invoiceDate) = p.test_date
    AND j.businessUnitNormalized = p.test_bu
    AND j.jobTypeName NOT LIKE '%COMM%'
  GROUP BY event_date, business_unit
),

-- ============================================================================
-- BASIS 2: Estimate Created Date (estimate.createdOn = target_date, status=Sold)
-- ============================================================================
estimate_created_all AS (
  SELECT
    DATE(e.createdOn) as event_date,
    j.businessUnitNormalized as business_unit,
    'estimate_created' as basis,
    'all_sold' as filter_desc,
    SUM(e.subtotal) as total_booked,
    COUNT(DISTINCT e.id) as num_estimates,
    COUNT(DISTINCT j.id) as num_jobs
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id
  INNER JOIN sales_units s ON j.businessUnitNormalized = s.businessUnit
  CROSS JOIN test_params p
  WHERE DATE(e.createdOn) = p.test_date
    AND j.businessUnitNormalized = p.test_bu
    AND e.status = 'Sold'
  GROUP BY event_date, business_unit
),

estimate_created_no_comm AS (
  SELECT
    DATE(e.createdOn) as event_date,
    j.businessUnitNormalized as business_unit,
    'estimate_created' as basis,
    'sold_no_comm' as filter_desc,
    SUM(e.subtotal) as total_booked,
    COUNT(DISTINCT e.id) as num_estimates,
    COUNT(DISTINCT j.id) as num_jobs
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id
  INNER JOIN sales_units s ON j.businessUnitNormalized = s.businessUnit
  CROSS JOIN test_params p
  WHERE DATE(e.createdOn) = p.test_date
    AND j.businessUnitNormalized = p.test_bu
    AND e.status = 'Sold'
    AND j.jobTypeName NOT LIKE '%COMM%'
  GROUP BY event_date, business_unit
),

estimate_created_estimate_jobs AS (
  SELECT
    DATE(e.createdOn) as event_date,
    j.businessUnitNormalized as business_unit,
    'estimate_created' as basis,
    'sold_estimate_jobs' as filter_desc,
    SUM(e.subtotal) as total_booked,
    COUNT(DISTINCT e.id) as num_estimates,
    COUNT(DISTINCT j.id) as num_jobs
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id
  INNER JOIN sales_units s ON j.businessUnitNormalized = s.businessUnit
  CROSS JOIN test_params p
  WHERE DATE(e.createdOn) = p.test_date
    AND j.businessUnitNormalized = p.test_bu
    AND e.status = 'Sold'
    AND LOWER(j.jobTypeName) LIKE '%estimate%'
  GROUP BY event_date, business_unit
),

-- ============================================================================
-- BASIS 3: Job Completed Date (job.completedOn = target_date)
-- ============================================================================
job_completed_all AS (
  SELECT
    DATE(j.completedOn) as event_date,
    j.businessUnitNormalized as business_unit,
    'job_completed' as basis,
    'all_jobs' as filter_desc,
    SUM(COALESCE(i.total, 0)) as total_booked,
    COUNT(DISTINCT i.id) as num_invoices,
    COUNT(DISTINCT j.id) as num_jobs
  FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
  INNER JOIN sales_units s ON j.businessUnitNormalized = s.businessUnit
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON j.id = i.jobId
  CROSS JOIN test_params p
  WHERE DATE(j.completedOn) = p.test_date
    AND j.businessUnitNormalized = p.test_bu
    AND j.jobStatus = 'Completed'
  GROUP BY event_date, business_unit
),

job_completed_estimate_jobs AS (
  SELECT
    DATE(j.completedOn) as event_date,
    j.businessUnitNormalized as business_unit,
    'job_completed' as basis,
    'estimate_jobs' as filter_desc,
    SUM(COALESCE(i.total, 0)) as total_booked,
    COUNT(DISTINCT i.id) as num_invoices,
    COUNT(DISTINCT j.id) as num_jobs
  FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
  INNER JOIN sales_units s ON j.businessUnitNormalized = s.businessUnit
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON j.id = i.jobId
  CROSS JOIN test_params p
  WHERE DATE(j.completedOn) = p.test_date
    AND j.businessUnitNormalized = p.test_bu
    AND j.jobStatus = 'Completed'
    AND LOWER(j.jobTypeName) LIKE '%estimate%'
  GROUP BY event_date, business_unit
),

-- ============================================================================
-- BASIS 4: Check if soldOn field exists in estimates
-- ============================================================================
-- Note: If raw_estimates has soldOn/soldDate field, add queries here
-- For now, using estimate created date as proxy

-- ============================================================================
-- Combine all interpretations
-- ============================================================================
all_interpretations AS (
  SELECT * FROM invoice_date_all
  UNION ALL SELECT * FROM invoice_date_no_comm
  UNION ALL SELECT * FROM estimate_created_all
  UNION ALL SELECT * FROM estimate_created_no_comm
  UNION ALL SELECT * FROM estimate_created_estimate_jobs
  UNION ALL SELECT * FROM job_completed_all
  UNION ALL SELECT * FROM job_completed_estimate_jobs
)

-- ============================================================================
-- Final output matrix
-- ============================================================================
SELECT
  business_unit as bu,
  event_date as date,
  basis,
  filter_desc,
  ROUND(total_booked, 2) as total_booked,
  num_invoices,
  num_estimates,
  num_jobs,
  -- Show delta from ST UI targets
  CASE business_unit
    WHEN 'Phoenix-Sales' THEN ROUND(total_booked - 30241.51, 2)
    WHEN 'Tucson-Sales' THEN ROUND(total_booked - 4844.58, 2)
    WHEN 'Nevada-Sales' THEN ROUND(total_booked - 27150.00, 2)
  END as delta_from_st_ui
FROM all_interpretations
ORDER BY business_unit, basis, filter_desc;

-- ============================================================================
-- Expected output format:
-- ============================================================================
-- bu            | date       | basis             | filter_desc        | total_booked | delta_from_st_ui
-- Phoenix-Sales | 2025-08-18 | invoice_date      | all_jobs           | $XXX         | $XXX
-- Phoenix-Sales | 2025-08-18 | invoice_date      | exclude_comm       | $XXX         | $XXX
-- Phoenix-Sales | 2025-08-18 | estimate_created  | all_sold           | $XXX         | $XXX
-- Phoenix-Sales | 2025-08-18 | estimate_created  | sold_no_comm       | $XXX         | $XXX
-- Phoenix-Sales | 2025-08-18 | estimate_created  | sold_estimate_jobs | $XXX         | $XXX
-- Phoenix-Sales | 2025-08-18 | job_completed     | all_jobs           | $XXX         | $XXX
-- Phoenix-Sales | 2025-08-18 | job_completed     | estimate_jobs      | $XXX         | $XXX
-- ... (repeat for Tucson-Sales and Nevada-Sales)

-- ============================================================================
-- RECOMMENDATION:
-- After running this query, identify the basis + filter_desc combination
-- with delta closest to $0 for all three BUs. That becomes our authoritative
-- Total Booked definition.
-- ============================================================================
