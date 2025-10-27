-- st_mart_v2.dollars_produced_daily
-- Daily dollars produced (revenue) by business unit
-- VALIDATED: Matches daily_kpis.dollars_produced calculation
--
-- Business Logic:
--   - Dollars Produced = sum of revenue_subtotal from completed/hold production jobs
--   - Date based on job_start_date (from first appointment)
--   - Includes both Completed and Hold status jobs
--
-- Grain: One row per date per business unit

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.dollars_produced_daily` AS

SELECT
  p.start_date as kpi_date,
  p.business_unit,

  -- DOLLARS PRODUCED: Sum of revenue from production jobs
  SUM(p.revenue_subtotal) as dollars_produced,

  -- Additional production metrics for analysis
  SUM(p.gross_profit) as total_gross_profit,
  SUM(p.total_cost) as total_cost,
  SUM(p.labor_cost) as total_labor_cost,
  SUM(p.material_cost_net) as total_material_cost,

  -- Job counts
  COUNT(p.job_id) as total_job_count,
  COUNT(CASE WHEN p.is_warranty THEN 1 END) as warranty_job_count,
  COUNT(CASE WHEN p.job_status = 'Completed' THEN 1 END) as completed_job_count,
  COUNT(CASE WHEN p.job_status = 'Hold' THEN 1 END) as hold_job_count,

  -- Calculated metrics (for reference)
  SAFE_DIVIDE(SUM(p.gross_profit), NULLIF(SUM(p.revenue_subtotal), 0)) * 100 as gpm_percent,
  SAFE_DIVIDE(SUM(p.revenue_subtotal), NULLIF(SUM(p.labor_cost), 0)) as labor_efficiency,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_stage.production_jobs` p

WHERE p.start_date IS NOT NULL
  AND p.business_unit IS NOT NULL

GROUP BY
  p.start_date,
  p.business_unit

ORDER BY
  p.start_date DESC,
  p.business_unit
;
