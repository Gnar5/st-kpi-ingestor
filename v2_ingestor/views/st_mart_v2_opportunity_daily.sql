-- st_mart_v2.opportunity_daily
-- Daily business unit summary of sales opportunities and close rate
--
-- Business Logic:
--   - Sales opportunities counted by opportunity_date (when job had activity)
--   - Closed opportunities counted by first_sale_date (when job FIRST closed)
--   - This matches ServiceTitan's counting methodology
--
-- Grain: One row per date per business unit

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.opportunity_daily` AS

WITH sales_opps AS (
  -- Count sales opportunities by opportunity_date (LATEST soldOn logic)
  SELECT
    opportunity_date as kpi_date,
    business_unit,
    COUNT(*) as sales_opportunities,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(estimate_count) as total_estimates
  FROM `kpi-auto-471020.st_stage.opportunity_jobs`
  WHERE opportunity_date IS NOT NULL
    AND business_unit IS NOT NULL
  GROUP BY opportunity_date, business_unit
),

closed_opps AS (
  -- Count closed opportunities by first_sale_date (EARLIEST soldOn logic)
  -- A job counts as "closed" on the date it FIRST sold
  SELECT
    first_sale_date as kpi_date,
    business_unit,
    COUNT(*) as closed_opportunities,
    SUM(sold_estimate_count) as total_sold_estimates
  FROM `kpi-auto-471020.st_stage.opportunity_jobs`
  WHERE first_sale_date IS NOT NULL
    AND business_unit IS NOT NULL
  GROUP BY first_sale_date, business_unit
)

SELECT
  COALESCE(s.kpi_date, c.kpi_date) as kpi_date,
  COALESCE(s.business_unit, c.business_unit) as business_unit,

  -- Opportunity counts from separate logic
  COALESCE(s.sales_opportunities, 0) as sales_opportunities,
  COALESCE(c.closed_opportunities, 0) as closed_opportunities,

  -- Close rate percentage
  ROUND(
    SAFE_DIVIDE(
      COALESCE(c.closed_opportunities, 0),
      NULLIF(COALESCE(s.sales_opportunities, 0), 0)
    ) * 100,
    2
  ) as close_rate_percent,

  -- Additional metrics
  COALESCE(s.unique_customers, 0) as unique_customers,
  COALESCE(s.total_estimates, 0) as total_estimates,
  COALESCE(c.total_sold_estimates, 0) as total_sold_estimates,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM sales_opps s
FULL OUTER JOIN closed_opps c
  ON s.kpi_date = c.kpi_date AND s.business_unit = c.business_unit

WHERE COALESCE(s.kpi_date, c.kpi_date) IS NOT NULL

ORDER BY
  kpi_date DESC,
  business_unit
;
