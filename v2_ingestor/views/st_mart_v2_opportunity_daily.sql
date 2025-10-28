-- st_mart_v2.opportunity_daily
-- Daily business unit summary of sales opportunities and close rate
--
-- Business Logic:
--   - Aggregates opportunity_jobs by opportunity_date and business unit
--   - Calculates close_rate = closed_opportunities / sales_opportunities * 100
--
-- Grain: One row per date per business unit

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.opportunity_daily` AS

SELECT
  o.opportunity_date as kpi_date,
  o.business_unit,

  -- Opportunity counts
  COUNT(CASE WHEN o.is_sales_opportunity THEN 1 END) as sales_opportunities,
  COUNT(CASE WHEN o.is_closed_opportunity THEN 1 END) as closed_opportunities,

  -- Close rate percentage
  ROUND(
    SAFE_DIVIDE(
      COUNT(CASE WHEN o.is_closed_opportunity THEN 1 END),
      NULLIF(COUNT(CASE WHEN o.is_sales_opportunity THEN 1 END), 0)
    ) * 100,
    2
  ) as close_rate_percent,

  -- Additional metrics for analysis
  COUNT(DISTINCT o.customer_id) as unique_customers,
  SUM(o.estimate_count) as total_estimates,
  SUM(o.sold_estimate_count) as total_sold_estimates,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_stage.opportunity_jobs` o

WHERE o.opportunity_date IS NOT NULL
  AND o.business_unit IS NOT NULL
GROUP BY
  o.opportunity_date,
  o.business_unit

ORDER BY
  o.opportunity_date DESC,
  o.business_unit
;
