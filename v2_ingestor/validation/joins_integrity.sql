-- Critical Joins Integrity Validation
-- Verifies that entities can be properly joined for KPI calculation

WITH join_coverage AS (
  -- Invoice → Job join coverage
  SELECT
    'invoices_to_jobs' as join_type,
    COUNT(*) as total_records,
    COUNT(jobId) as records_with_join,
    COUNT(*) - COUNT(jobId) as records_missing_join,
    ROUND(COUNT(jobId) / COUNT(*) * 100, 2) as coverage_percent
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices`
  WHERE DATE(createdOn) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)

  UNION ALL

  -- Invoice → Business Unit coverage
  SELECT
    'invoices_to_business_units' as join_type,
    COUNT(*) as total_records,
    COUNT(businessUnitId) as records_with_join,
    COUNT(*) - COUNT(businessUnitId) as records_missing_join,
    ROUND(COUNT(businessUnitId) / COUNT(*) * 100, 2) as coverage_percent
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices`
  WHERE DATE(createdOn) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)

  UNION ALL

  -- Payments → Invoice join coverage
  SELECT
    'payments_to_invoices' as join_type,
    COUNT(*) as total_records,
    COUNT(invoiceId) as records_with_join,
    COUNT(*) - COUNT(invoiceId) as records_missing_join,
    ROUND(COUNT(invoiceId) / COUNT(*) * 100, 2) as coverage_percent
  FROM `kpi-auto-471020.st_raw_v2.raw_payments`
  WHERE DATE(createdOn) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)

  UNION ALL

  -- Payroll → Job join coverage
  SELECT
    'payroll_to_jobs' as join_type,
    COUNT(*) as total_records,
    COUNT(jobId) as records_with_join,
    COUNT(*) - COUNT(jobId) as records_missing_join,
    ROUND(COUNT(jobId) / COUNT(*) * 100, 2) as coverage_percent
  FROM `kpi-auto-471020.st_raw_v2.raw_payroll`
  WHERE DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)

  UNION ALL

  -- Estimates → Job join coverage
  SELECT
    'estimates_to_jobs' as join_type,
    COUNT(*) as total_records,
    COUNT(jobId) as records_with_join,
    COUNT(*) - COUNT(jobId) as records_missing_join,
    ROUND(COUNT(jobId) / COUNT(*) * 100, 2) as coverage_percent
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates`
  WHERE DATE(createdOn) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)

  UNION ALL

  -- Estimates → Business Unit coverage
  SELECT
    'estimates_to_business_units' as join_type,
    COUNT(*) as total_records,
    COUNT(businessUnitId) as records_with_join,
    COUNT(*) - COUNT(businessUnitId) as records_missing_join,
    ROUND(COUNT(businessUnitId) / COUNT(*) * 100, 2) as coverage_percent
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates`
  WHERE DATE(createdOn) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)

  UNION ALL

  -- Jobs → Customer join coverage
  SELECT
    'jobs_to_customers' as join_type,
    COUNT(*) as total_records,
    COUNT(customerId) as records_with_join,
    COUNT(*) - COUNT(customerId) as records_missing_join,
    ROUND(COUNT(customerId) / COUNT(*) * 100, 2) as coverage_percent
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs`
  WHERE DATE(createdOn) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)

  UNION ALL

  -- Locations → Customer join coverage
  SELECT
    'locations_to_customers' as join_type,
    COUNT(*) as total_records,
    COUNT(customerId) as records_with_join,
    COUNT(*) - COUNT(customerId) as records_missing_join,
    ROUND(COUNT(customerId) / COUNT(*) * 100, 2) as coverage_percent
  FROM `kpi-auto-471020.st_raw_v2.raw_locations`
  WHERE DATE(createdOn) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
)

SELECT
  join_type,
  total_records,
  records_with_join,
  records_missing_join,
  coverage_percent,
  CASE
    WHEN join_type IN ('invoices_to_jobs', 'payments_to_invoices', 'estimates_to_jobs')
         AND coverage_percent < 85 THEN '🔴 CRITICAL'
    WHEN coverage_percent < 70 THEN '🟡 WARNING'
    WHEN coverage_percent >= 95 THEN '🟢 EXCELLENT'
    ELSE '🟢 OK'
  END as status,
  CASE
    WHEN join_type = 'invoices_to_jobs' THEN 90.0
    WHEN join_type = 'payments_to_invoices' THEN 95.0
    WHEN join_type = 'estimates_to_jobs' THEN 90.0
    WHEN join_type LIKE '%_to_business_units' THEN 85.0
    ELSE 75.0
  END as threshold_percent
FROM join_coverage
ORDER BY
  CASE
    WHEN join_type LIKE 'invoices%' THEN 1
    WHEN join_type LIKE 'payments%' THEN 2
    WHEN join_type LIKE 'estimates%' THEN 3
    WHEN join_type LIKE 'payroll%' THEN 4
    ELSE 5
  END,
  join_type;