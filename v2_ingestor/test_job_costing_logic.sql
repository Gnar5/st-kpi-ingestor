-- Test Job Costing Logic
-- Theory: "Dollars Produced" = Sum of invoice totals for jobs where:
--   1. Job has appointment with start date in range (8/18-8/24)
--   2. Job status = 'Completed'
--   3. Group by business unit

-- First, we need to ingest appointments to get scheduled dates
-- For now, let's test with what we have: completedOn as proxy

WITH
phoenix_jobs_completed AS (
  SELECT
    j.id as job_id,
    j.businessUnitNormalized,
    j.jobStatus,
    DATE(j.completedOn) as completed_date,
    i.id as invoice_id,
    i.total as invoice_total
  FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON j.id = i.jobId
  WHERE j.businessUnitNormalized = 'Phoenix-Production'
    AND j.jobStatus = 'Completed'
    AND DATE(j.completedOn) >= '2025-08-18'
    AND DATE(j.completedOn) <= '2025-08-24'
)

SELECT
  businessUnitNormalized,
  COUNT(DISTINCT job_id) as job_count,
  COUNT(DISTINCT invoice_id) as invoice_count,
  ROUND(SUM(invoice_total), 2) as total_dollars_produced
FROM phoenix_jobs_completed
GROUP BY businessUnitNormalized;

-- ST UI shows: 119 jobs, $232,891.98 for Phoenix
-- Let's see what we get with completedOn date
