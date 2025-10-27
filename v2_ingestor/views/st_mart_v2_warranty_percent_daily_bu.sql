-- st_mart_v2.warranty_percent_daily_bu
-- Daily warranty percentage by business unit for production work
--
-- Business Logic:
--   - Warranty % = (warranty jobs / total jobs) * 100
--   - Warranty jobs = jobs with jobTypeName IN ('Warranty', 'Touchup')
--   - Includes both Completed and Hold status production jobs
--   - Date based on job start_date in America/Phoenix timezone
--
-- Grain: One row per date per business unit

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.warranty_percent_daily_bu` AS

SELECT
  DATE(p.start_date) as kpi_date,
  p.business_unit,

  -- JOB COUNTS
  COUNT(*) as total_jobs,
  COUNT(CASE WHEN p.is_warranty THEN 1 END) as warranty_jobs,
  COUNT(CASE WHEN NOT p.is_warranty THEN 1 END) as non_warranty_jobs,

  -- WARRANTY %: Percentage of jobs that are warranty
  ROUND(
    SAFE_DIVIDE(
      COUNT(CASE WHEN p.is_warranty THEN 1 END),
      NULLIF(COUNT(*), 0)
    ) * 100,
    2
  ) as warranty_percent,

  -- Additional metrics for analysis
  SUM(CASE WHEN p.is_warranty THEN p.revenue_subtotal ELSE 0 END) as warranty_revenue,
  SUM(CASE WHEN NOT p.is_warranty THEN p.revenue_subtotal ELSE 0 END) as non_warranty_revenue,
  SUM(p.revenue_subtotal) as total_revenue,

  -- Warranty revenue percentage
  ROUND(
    SAFE_DIVIDE(
      SUM(CASE WHEN p.is_warranty THEN p.revenue_subtotal ELSE 0 END),
      NULLIF(SUM(p.revenue_subtotal), 0)
    ) * 100,
    2
  ) as warranty_revenue_percent,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_stage.production_jobs` p

WHERE DATE(p.start_date) IS NOT NULL
  AND p.business_unit IS NOT NULL

GROUP BY
  DATE(p.start_date),
  p.business_unit

ORDER BY
  kpi_date DESC,
  p.business_unit
;
