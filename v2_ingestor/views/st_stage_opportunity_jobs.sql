-- st_stage.opportunity_jobs
-- Per-job opportunity rollup with close rate flags
--
-- Business Logic:
--   - Sales Opportunity = any job with >= 1 estimate
--   - Closed Opportunity = any job with >= 1 sold estimate
--   - Opportunity Date = earliest soldOn if exists, else job completedOn (America/Phoenix timezone)
--
-- Grain: One row per job (jobId)

CREATE OR REPLACE VIEW `kpi-auto-471020.st_stage.opportunity_jobs` AS

WITH estimate_rollup AS (
  -- Roll up estimates by job to get counts and earliest sold date
  SELECT
    jobId,
    COUNT(*) as estimate_count,
    COUNT(CASE WHEN status = 'Sold' THEN 1 END) as sold_estimate_count,
    MIN(CASE WHEN status = 'Sold' THEN soldOn END) as earliest_sold_on_utc
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates`
  WHERE jobId IS NOT NULL
  GROUP BY jobId
)

SELECT
  j.id as job_id,
  j.customerId as customer_id,
  j.businessUnitId as business_unit_id,
  j.jobStatus as job_status,
  j.createdOn as job_created_on_utc,
  j.completedOn as job_completed_on_utc,

  -- Estimate rollup fields
  COALESCE(e.estimate_count, 0) as estimate_count,
  COALESCE(e.sold_estimate_count, 0) as sold_estimate_count,
  e.earliest_sold_on_utc,

  -- Opportunity date: soldOn if exists, else completedOn, converted to Phoenix timezone
  DATE(
    COALESCE(e.earliest_sold_on_utc, j.completedOn), 'America/Phoenix'
  ) as opportunity_date,

  -- Opportunity flags
  CASE
    WHEN COALESCE(e.estimate_count, 0) >= 1 THEN TRUE
    ELSE FALSE
  END as is_sales_opportunity,

  CASE
    WHEN COALESCE(e.sold_estimate_count, 0) >= 1 THEN TRUE
    ELSE FALSE
  END as is_closed_opportunity,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
LEFT JOIN estimate_rollup e ON j.id = e.jobId
WHERE j.id IS NOT NULL
  AND COALESCE(e.estimate_count, 0) >= 1  -- Only include jobs with at least 1 estimate
;
