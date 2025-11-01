-- Phase 0: Find the correct job population matching ServiceTitan's 245 jobs
-- Testing different date fields and filters

WITH date_variants AS (
  SELECT
    'completedOn' as variant,
    COUNT(DISTINCT j.id) as job_count,
    COUNT(DISTINCT j.businessUnitId) as bu_count
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
  WHERE DATE(j.completedOn) BETWEEN '2025-10-20' AND '2025-10-26'
    AND j.jobStatus IN ('Completed', 'Hold', 'Scheduled', 'InProgress')

  UNION ALL

  SELECT
    'createdOn' as variant,
    COUNT(DISTINCT j.id) as job_count,
    COUNT(DISTINCT j.businessUnitId) as bu_count
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
  WHERE DATE(j.createdOn) BETWEEN '2025-10-20' AND '2025-10-26'
    AND j.jobStatus IN ('Completed', 'Hold', 'Scheduled', 'InProgress')

  UNION ALL

  -- Jobs with appointments scheduled in the week
  SELECT
    'appointment_scheduledStart' as variant,
    COUNT(DISTINCT j.id) as job_count,
    COUNT(DISTINCT j.businessUnitId) as bu_count
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
  JOIN `kpi-auto-471020.st_raw_v2.raw_appointments` a ON j.id = a.jobId
  WHERE DATE(a.scheduledStart) BETWEEN '2025-10-20' AND '2025-10-26'
    AND j.jobStatus IN ('Completed', 'Hold', 'Scheduled', 'InProgress')

  UNION ALL

  -- Jobs with invoices in the week
  SELECT
    'invoiceDate' as variant,
    COUNT(DISTINCT j.id) as job_count,
    COUNT(DISTINCT j.businessUnitId) as bu_count
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
  JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON j.id = i.jobId
  WHERE DATE(i.invoiceDate) BETWEEN '2025-10-20' AND '2025-10-26'
    AND j.jobStatus IN ('Completed', 'Hold', 'Scheduled', 'InProgress')

  UNION ALL

  -- Jobs with invoices created in the week
  SELECT
    'invoice_createdOn' as variant,
    COUNT(DISTINCT j.id) as job_count,
    COUNT(DISTINCT j.businessUnitId) as bu_count
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
  JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON j.id = i.jobId
  WHERE DATE(i.createdOn) BETWEEN '2025-10-20' AND '2025-10-26'
    AND j.jobStatus IN ('Completed', 'Hold', 'Scheduled', 'InProgress')

  UNION ALL

  -- Jobs with invoices modified in the week
  SELECT
    'invoice_modifiedOn' as variant,
    COUNT(DISTINCT j.id) as job_count,
    COUNT(DISTINCT j.businessUnitId) as bu_count
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
  JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON j.id = i.jobId
  WHERE DATE(i.modifiedOn) BETWEEN '2025-10-20' AND '2025-10-26'
    AND j.jobStatus IN ('Completed', 'Hold', 'Scheduled', 'InProgress')
)
SELECT * FROM date_variants
ORDER BY ABS(job_count - 245) ASC