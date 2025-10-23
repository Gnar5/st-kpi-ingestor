-- Entity Coverage by Month Validation
-- Shows record counts by month for each entity to identify gaps

WITH monthly_coverage AS (
  -- Jobs
  SELECT
    'jobs' as entity,
    DATE_TRUNC(DATE(createdOn), MONTH) as month,
    COUNT(*) as record_count,
    COUNT(DISTINCT businessUnitId) as business_units,
    MIN(createdOn) as earliest,
    MAX(createdOn) as latest
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs`
  WHERE createdOn >= '2020-01-01'
  GROUP BY month

  UNION ALL

  -- Invoices
  SELECT
    'invoices' as entity,
    DATE_TRUNC(DATE(createdOn), MONTH) as month,
    COUNT(*) as record_count,
    COUNT(DISTINCT businessUnitId) as business_units,
    MIN(createdOn) as earliest,
    MAX(createdOn) as latest
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices`
  WHERE createdOn >= '2020-01-01'
  GROUP BY month

  UNION ALL

  -- Estimates
  SELECT
    'estimates' as entity,
    DATE_TRUNC(DATE(createdOn), MONTH) as month,
    COUNT(*) as record_count,
    COUNT(DISTINCT businessUnitId) as business_units,
    MIN(createdOn) as earliest,
    MAX(createdOn) as latest
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates`
  WHERE createdOn >= '2020-01-01'
  GROUP BY month

  UNION ALL

  -- Payments
  SELECT
    'payments' as entity,
    DATE_TRUNC(DATE(createdOn), MONTH) as month,
    COUNT(*) as record_count,
    COUNT(DISTINCT customerId) as unique_customers,
    MIN(createdOn) as earliest,
    MAX(createdOn) as latest
  FROM `kpi-auto-471020.st_raw_v2.raw_payments`
  WHERE createdOn >= '2020-01-01'
  GROUP BY month

  UNION ALL

  -- Payroll
  SELECT
    'payroll' as entity,
    DATE_TRUNC(DATE(date), MONTH) as month,
    COUNT(*) as record_count,
    COUNT(DISTINCT employeeId) as unique_employees,
    MIN(date) as earliest,
    MAX(date) as latest
  FROM `kpi-auto-471020.st_raw_v2.raw_payroll`
  WHERE date >= '2020-01-01'
  GROUP BY month

  UNION ALL

  -- Customers
  SELECT
    'customers' as entity,
    DATE_TRUNC(DATE(createdOn), MONTH) as month,
    COUNT(*) as record_count,
    COUNT(DISTINCT type) as customer_types,
    MIN(createdOn) as earliest,
    MAX(createdOn) as latest
  FROM `kpi-auto-471020.st_raw_v2.raw_customers`
  WHERE createdOn >= '2020-01-01'
  GROUP BY month

  UNION ALL

  -- Locations
  SELECT
    'locations' as entity,
    DATE_TRUNC(DATE(createdOn), MONTH) as month,
    COUNT(*) as record_count,
    COUNT(DISTINCT customerId) as unique_customers,
    MIN(createdOn) as earliest,
    MAX(createdOn) as latest
  FROM `kpi-auto-471020.st_raw_v2.raw_locations`
  WHERE createdOn >= '2020-01-01'
  GROUP BY month

  UNION ALL

  -- Campaigns
  SELECT
    'campaigns' as entity,
    DATE_TRUNC(DATE(createdOn), MONTH) as month,
    COUNT(*) as record_count,
    COUNT(DISTINCT categoryId) as categories,
    MIN(createdOn) as earliest,
    MAX(createdOn) as latest
  FROM `kpi-auto-471020.st_raw_v2.raw_campaigns`
  WHERE createdOn >= '2020-01-01'
  GROUP BY month
)

SELECT
  entity,
  month,
  record_count,
  business_units,
  CASE
    WHEN LAG(record_count) OVER (PARTITION BY entity ORDER BY month) > 0 THEN
      ROUND((record_count - LAG(record_count) OVER (PARTITION BY entity ORDER BY month)) /
            LAG(record_count) OVER (PARTITION BY entity ORDER BY month) * 100, 1)
    ELSE NULL
  END as month_over_month_change_pct,
  CASE
    WHEN record_count = 0 THEN '🔴 NO DATA'
    WHEN record_count < 100 THEN '🟡 LOW'
    ELSE '🟢 OK'
  END as status
FROM monthly_coverage
ORDER BY entity, month DESC;