-- st_mart_v2.completed_estimates_daily
-- Daily count of completed estimates by business unit
--
-- Business Logic:
--   - Completed Estimate = one completed job from allowlist
--   - Counts distinct jobs per business unit per day
--   - Date based on job completedOn in America/Phoenix timezone
--
-- Grain: One row per date per business unit

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.completed_estimates_daily` AS

SELECT
  c.completed_date as kpi_date,
  c.business_unit_id,
  bu.businessUnitName as business_unit_name,

  -- Count of completed jobs (estimates)
  COUNT(DISTINCT c.job_id) as completed_estimates,

  -- Additional metrics for analysis
  COUNT(DISTINCT c.customer_id) as unique_customers,
  COUNT(DISTINCT c.job_type_id) as job_types_completed,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_stage.completed_estimates_jobs` c
LEFT JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` bu
  ON c.job_id = bu.id

WHERE c.completed_date IS NOT NULL

GROUP BY
  c.completed_date,
  c.business_unit_id,
  bu.businessUnitName

ORDER BY
  c.completed_date DESC,
  c.business_unit_id
;
