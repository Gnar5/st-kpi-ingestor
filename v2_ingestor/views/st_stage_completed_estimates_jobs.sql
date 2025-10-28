-- st_stage.completed_estimates_jobs
-- Jobs filtered for completed estimates counting
--
-- Business Logic:
--   - Completed Estimate = job with estimate in job type name AND status = Completed
--   - Filter: jobTypeName contains "estimate" (case-insensitive)
--   - Filter: jobStatus = "Completed"
--   - Date: job completedOn in America/Phoenix timezone
--   - Count: total jobs (no deduplication by customer)
--   - Note: Does NOT exclude test customers (unlike leads)
--
-- Grain: One row per completed estimate job

CREATE OR REPLACE VIEW `kpi-auto-471020.st_stage.completed_estimates_jobs` AS

SELECT
  j.id as job_id,
  j.customerId as customer_id,
  j.businessUnitNormalized as business_unit,
  j.jobTypeName as job_type_name,
  j.jobStatus as job_status,

  -- Original UTC timestamps
  j.createdOn as job_created_on_utc,
  j.completedOn as job_completed_on_utc,

  -- Completed date: completedOn in Phoenix timezone
  DATE(j.completedOn, 'America/Phoenix') as completed_date,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j

WHERE
  -- Filter: estimate jobs
  LOWER(j.jobTypeName) LIKE '%estimate%'

  -- Filter: completed status
  AND j.jobStatus = 'Completed'

  -- Basic nullability checks
  AND j.id IS NOT NULL
  AND j.completedOn IS NOT NULL
;
