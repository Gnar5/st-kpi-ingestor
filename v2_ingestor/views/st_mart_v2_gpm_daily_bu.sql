-- st_mart_v2.gpm_daily_bu
-- Daily Gross Profit Margin (GPM) by business unit for production work
-- UPDATED: 2025-10-29 to include ALL production job labor costs (even zero-revenue jobs)
--
-- Business Logic:
--   - GPM = (Revenue - Labor - Materials) / Revenue * 100
--   - Revenue from completed invoices (subtotal) - only jobs with revenue
--   - Labor from payroll for ALL production jobs (includes jobs with zero revenue)
--   - Materials from purchase orders (Exported, Received, Sent, PartiallyReceived)
--   - Production jobs only (excludes service/warranty)
--   - Date based on job start_date in America/Phoenix timezone
--
-- Grain: One row per date per business unit
--
-- Key Change: Labor costs now include ALL production jobs, even if they have $0 revenue.
-- This matches ServiceTitan FOREMAN methodology where labor is tracked for all work done,
-- but revenue only counts completed/invoiced jobs.

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.gpm_daily_bu` AS

SELECT
  DATE(p.start_date) as kpi_date,
  p.business_unit,

  -- REVENUE: Total invoiced revenue for production work
  ROUND(SUM(p.revenue_subtotal), 2) as total_revenue,

  -- COSTS: Labor and materials
  ROUND(SUM(p.labor_cost), 2) as total_labor_cost,
  ROUND(SUM(p.material_cost_net), 2) as total_material_cost,
  ROUND(SUM(p.total_cost), 2) as total_cost,

  -- GROSS PROFIT: Revenue minus all costs
  ROUND(SUM(p.gross_profit), 2) as gross_profit,

  -- GPM %: Weighted average (sum of profits / sum of revenue)
  ROUND(
    SAFE_DIVIDE(SUM(p.gross_profit), NULLIF(SUM(p.revenue_subtotal), 0)) * 100,
    2
  ) as gpm_percent,

  -- Additional metrics for analysis
  COUNT(DISTINCT p.job_id) as job_count,

  -- Cost percentages
  ROUND(
    SAFE_DIVIDE(SUM(p.labor_cost), NULLIF(SUM(p.revenue_subtotal), 0)) * 100,
    2
  ) as labor_percent_of_revenue,
  ROUND(
    SAFE_DIVIDE(SUM(p.material_cost_net), NULLIF(SUM(p.revenue_subtotal), 0)) * 100,
    2
  ) as material_percent_of_revenue,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_stage.production_jobs` p

WHERE DATE(p.start_date) IS NOT NULL
  AND p.business_unit IS NOT NULL
  -- NOTE: No revenue filter here! We include ALL production jobs for labor/materials,
  -- but revenue will naturally be $0 for jobs without invoices

GROUP BY
  DATE(p.start_date),
  p.business_unit

ORDER BY
  kpi_date DESC,
  p.business_unit
;
