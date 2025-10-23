-- BigQuery Data Quality Stoplight Report
-- RED/YELLOW/GREEN status for critical thresholds

WITH metrics AS (
  -- Invoice job linkage
  SELECT
    'Invoice Job Coverage' as metric,
    ROUND(COUNT(jobId) / COUNT(*) * 100, 2) as value,
    90.0 as red_threshold,
    95.0 as green_threshold,
    '%' as unit
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices`
  WHERE DATE(createdOn) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)

  UNION ALL

  -- Invoice business unit coverage
  SELECT
    'Invoice BU Coverage' as metric,
    ROUND(COUNT(businessUnitId) / COUNT(*) * 100, 2) as value,
    85.0 as red_threshold,
    95.0 as green_threshold,
    '%' as unit
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices`
  WHERE DATE(createdOn) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)

  UNION ALL

  -- Payment to invoice linkage
  SELECT
    'Payment Invoice Coverage' as metric,
    ROUND(COUNT(invoiceId) / COUNT(*) * 100, 2) as value,
    95.0 as red_threshold,
    98.0 as green_threshold,
    '%' as unit
  FROM `kpi-auto-471020.st_raw_v2.raw_payments`
  WHERE DATE(createdOn) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)

  UNION ALL

  -- Estimate job linkage
  SELECT
    'Estimate Job Coverage' as metric,
    ROUND(COUNT(jobId) / COUNT(*) * 100, 2) as value,
    90.0 as red_threshold,
    95.0 as green_threshold,
    '%' as unit
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates`
  WHERE DATE(createdOn) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)

  UNION ALL

  -- Data freshness - Jobs
  SELECT
    'Jobs Data Freshness' as metric,
    DATETIME_DIFF(CURRENT_DATETIME(), MAX(modifiedOn), HOUR) as value,
    48.0 as red_threshold,  -- Red if > 48 hours old
    4.0 as green_threshold,  -- Green if < 4 hours old
    'hours' as unit
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs`

  UNION ALL

  -- Data freshness - Invoices
  SELECT
    'Invoices Data Freshness' as metric,
    DATETIME_DIFF(CURRENT_DATETIME(), MAX(modifiedOn), HOUR) as value,
    48.0 as red_threshold,
    4.0 as green_threshold,
    'hours' as unit
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices`

  UNION ALL

  -- Data freshness - Payroll
  SELECT
    'Payroll Data Freshness' as metric,
    DATETIME_DIFF(CURRENT_DATETIME(), MAX(modifiedOn), HOUR) as value,
    72.0 as red_threshold,  -- Payroll can be less frequent
    24.0 as green_threshold,
    'hours' as unit
  FROM `kpi-auto-471020.st_raw_v2.raw_payroll`

  UNION ALL

  -- Daily volume check - Jobs
  SELECT
    'Jobs Daily Volume' as metric,
    COUNT(*) as value,
    50.0 as red_threshold,  -- Red if < 50 jobs/day
    200.0 as green_threshold,  -- Green if > 200 jobs/day
    'records' as unit
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs`
  WHERE DATE(createdOn) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)

  UNION ALL

  -- Daily volume check - Invoices
  SELECT
    'Invoices Daily Volume' as metric,
    COUNT(*) as value,
    30.0 as red_threshold,  -- Red if < 30 invoices/day
    100.0 as green_threshold,  -- Green if > 100 invoices/day
    'records' as unit
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices`
  WHERE DATE(createdOn) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)

  UNION ALL

  -- Average close rate (should be reasonable)
  SELECT
    'Average Close Rate' as metric,
    AVG(CASE WHEN e.status = 'Sold' THEN 100.0 ELSE 0.0 END) as value,
    15.0 as red_threshold,  -- Red if < 15%
    25.0 as green_threshold,  -- Green if > 25%
    '%' as unit
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
  WHERE DATE(e.createdOn) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)

  UNION ALL

  -- Duplicate check
  SELECT
    'Invoice Duplicates' as metric,
    COUNT(*) - COUNT(DISTINCT id) as value,
    10.0 as red_threshold,  -- Red if > 10 duplicates
    1.0 as green_threshold,  -- Green if <= 1 duplicate
    'records' as unit
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices`
  WHERE DATE(createdOn) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
)

SELECT
  metric,
  value,
  unit,
  red_threshold,
  green_threshold,
  CASE
    -- For coverage metrics, higher is better
    WHEN unit = '%' AND metric LIKE '%Coverage%' THEN
      CASE
        WHEN value < red_threshold THEN '🔴 RED'
        WHEN value >= green_threshold THEN '🟢 GREEN'
        ELSE '🟡 YELLOW'
      END
    -- For close rate, mid-range is good
    WHEN metric = 'Average Close Rate' THEN
      CASE
        WHEN value < red_threshold THEN '🔴 RED'
        WHEN value >= green_threshold THEN '🟢 GREEN'
        ELSE '🟡 YELLOW'
      END
    -- For freshness metrics, lower is better
    WHEN unit = 'hours' THEN
      CASE
        WHEN value > red_threshold THEN '🔴 RED'
        WHEN value <= green_threshold THEN '🟢 GREEN'
        ELSE '🟡 YELLOW'
      END
    -- For volume metrics, higher is better
    WHEN metric LIKE '%Volume%' THEN
      CASE
        WHEN value < red_threshold THEN '🔴 RED'
        WHEN value >= green_threshold THEN '🟢 GREEN'
        ELSE '🟡 YELLOW'
      END
    -- For duplicates, lower is better
    WHEN metric LIKE '%Duplicates%' THEN
      CASE
        WHEN value > red_threshold THEN '🔴 RED'
        WHEN value <= green_threshold THEN '🟢 GREEN'
        ELSE '🟡 YELLOW'
      END
    ELSE '⚪ UNKNOWN'
  END as status,
  CURRENT_TIMESTAMP() as checked_at
FROM metrics
ORDER BY
  CASE
    WHEN metric LIKE 'Invoice%' THEN 1
    WHEN metric LIKE 'Payment%' THEN 2
    WHEN metric LIKE 'Estimate%' THEN 3
    WHEN metric LIKE '%Freshness%' THEN 4
    WHEN metric LIKE '%Volume%' THEN 5
    ELSE 6
  END,
  metric;