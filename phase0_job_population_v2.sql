-- Test more combinations to find 245 jobs
WITH combinations AS (
  -- Jobs with appointments OR completed in week
  SELECT
    'appointment_or_completed' as variant,
    COUNT(DISTINCT j.id) as job_count
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_appointments` a ON j.id = a.jobId
  WHERE (DATE(a.scheduledStart) BETWEEN '2025-10-20' AND '2025-10-26'
         OR DATE(j.completedOn) BETWEEN '2025-10-20' AND '2025-10-26')
    AND j.jobStatus IN ('Completed', 'Hold', 'Scheduled', 'InProgress')

  UNION ALL

  -- Invoice date with broader status filter
  SELECT
    'invoiceDate_all_statuses' as variant,
    COUNT(DISTINCT j.id) as job_count
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
  JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON j.id = i.jobId
  WHERE DATE(i.invoiceDate) BETWEEN '2025-10-20' AND '2025-10-26'

  UNION ALL

  -- Production jobs view count
  SELECT
    'production_jobs_view' as variant,
    COUNT(DISTINCT job_id) as job_count
  FROM `kpi-auto-471020.st_stage.production_jobs` p
  WHERE DATE(p.start_date) BETWEEN '2025-10-20' AND '2025-10-26'

  UNION ALL

  -- Jobs with any financial activity
  SELECT
    'any_financial_activity' as variant,
    COUNT(DISTINCT j.id) as job_count
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
  WHERE j.id IN (
    SELECT DISTINCT jobId FROM `kpi-auto-471020.st_raw_v2.raw_invoices`
    WHERE DATE(invoiceDate) BETWEEN '2025-10-20' AND '2025-10-26'
    UNION DISTINCT
    SELECT DISTINCT jobId FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders`
    WHERE DATE(requiredOn) BETWEEN '2025-10-20' AND '2025-10-26' AND jobId IS NOT NULL
    UNION DISTINCT
    SELECT DISTINCT jobId FROM `kpi-auto-471020.st_raw_v2.raw_payroll`
    WHERE DATE(date) BETWEEN '2025-10-20' AND '2025-10-26' AND jobId IS NOT NULL
  )
)
SELECT * FROM combinations
ORDER BY ABS(job_count - 245) ASC