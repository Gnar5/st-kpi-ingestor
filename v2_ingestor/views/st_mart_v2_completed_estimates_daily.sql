-- st_mart_v2.completed_estimates_daily
-- Daily count of completed estimates by business unit
--
-- Business Logic:
--   - Completed Estimate = job with estimate in type name AND status = Completed
--   - Count: total completed estimate jobs (no customer deduplication)
--   - Date based on job completedOn in America/Phoenix timezone
--   - Does NOT exclude test customers
--
-- Grain: One row per date per business unit

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.completed_estimates_daily` AS

SELECT
  c.completed_date as kpi_date,
  c.business_unit,

  -- COMPLETED ESTIMATES COUNT: Total completed estimate jobs
  COUNT(c.job_id) as completed_estimates_count,

  -- Additional metrics for analysis
  COUNT(DISTINCT c.customer_id) as unique_customers,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_stage.completed_estimates_jobs` c

WHERE c.completed_date IS NOT NULL
  AND c.business_unit IS NOT NULL

GROUP BY
  c.completed_date,
  c.business_unit

ORDER BY
  c.completed_date DESC,
  c.business_unit
;
