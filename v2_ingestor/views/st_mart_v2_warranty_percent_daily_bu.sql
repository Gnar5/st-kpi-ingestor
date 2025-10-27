-- st_mart_v2.warranty_percent_daily_bu
-- Daily warranty percentage by business unit for production work
-- VALIDATED AGAINST SERVICETITAN: Week 08/18-08/24/2025 matches exactly ✓
--
-- Business Logic (validated):
--   - Warranty % = (Total Cost of Warranty/Touchup Jobs) / (Total Revenue) * 100
--   - Warranty jobs = jobs with jobTypeName IN ('Warranty', 'Touchup')
--   - Uses COST (labor + materials) not revenue (warranty jobs typically have $0 revenue)
--   - Revenue = Dollars Produced (sum of all production job revenue)
--   - Includes both Completed and Hold status production jobs
--   - Date based on job start_date
--
-- Grain: One row per date per business unit

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.warranty_percent_daily_bu` AS

SELECT
  DATE(p.start_date) as kpi_date,
  p.business_unit,

  -- COSTS: Warranty/Touchup costs (labor + materials)
  ROUND(SUM(CASE WHEN p.is_warranty THEN p.total_cost ELSE 0 END), 2) as warranty_cost,
  ROUND(SUM(CASE WHEN NOT p.is_warranty THEN p.total_cost ELSE 0 END), 2) as non_warranty_cost,
  ROUND(SUM(p.total_cost), 2) as total_cost,

  -- REVENUE: Dollars Produced (all production revenue)
  ROUND(SUM(p.revenue_subtotal), 2) as dollars_produced,

  -- WARRANTY %: (Warranty Cost / Revenue) * 100 (validated metric)
  ROUND(
    SAFE_DIVIDE(
      SUM(CASE WHEN p.is_warranty THEN p.total_cost ELSE 0 END),
      NULLIF(SUM(p.revenue_subtotal), 0)
    ) * 100,
    2
  ) as warranty_percent,

  -- JOB COUNTS (for reference)
  COUNT(*) as total_jobs,
  COUNT(CASE WHEN p.is_warranty THEN 1 END) as warranty_jobs,
  COUNT(CASE WHEN NOT p.is_warranty THEN 1 END) as non_warranty_jobs,

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
