-- st_stage.leads_jobs
-- Jobs filtered for lead counting - VALIDATED AGAINST SERVICETITAN 2025-08-18 to 2025-08-24
-- Phoenix-Sales: 96 unique customers, 97 jobs ✓
--
-- Business Logic (validated):
--   - Lead = unique customer (customerId) with estimate job(s) created on date
--   - Filter: jobTypeName contains "estimate" (case-insensitive)
--   - Exclude: customer name contains "test" (case-insensitive)
--   - Date: job createdOn in America/Phoenix timezone
--
-- Grain: One row per job that qualifies as a lead

CREATE OR REPLACE VIEW `kpi-auto-471020.st_stage.leads_jobs` AS

SELECT
  j.id as job_id,
  j.customerId as customer_id,
  j.businessUnitNormalized as business_unit,
  j.jobTypeName as job_type_name,
  j.jobStatus as job_status,

  -- Original UTC timestamps
  j.createdOn as job_created_on_utc,
  j.completedOn as job_completed_on_utc,

  -- Lead date: createdOn in Phoenix timezone
  DATE(j.createdOn, 'America/Phoenix') as lead_date,

  -- Customer info for filtering
  c.name as customer_name,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_customers` c
  ON j.customerId = c.id

WHERE
  -- Filter: estimate jobs (validated logic)
  LOWER(j.jobTypeName) LIKE '%estimate%'

  -- Exclude test customers (validated logic)
  AND LOWER(COALESCE(c.name, '')) NOT LIKE '%test%'

  -- Basic nullability checks
  AND j.id IS NOT NULL
  AND j.customerId IS NOT NULL
  AND j.createdOn IS NOT NULL
;
