-- st_mart_v2.total_booked_daily
-- Daily total booked revenue by business unit
-- VALIDATED: Matches daily_kpis.total_booked calculation
--
-- Business Logic:
--   - Total Booked = sum of sold estimate revenue on sold date
--   - Date based on estimate soldOn in America/Phoenix timezone
--   - Amount = SUM(COALESCE(total, subtotal))
--
-- Grain: One row per date per business unit

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.total_booked_daily` AS

SELECT
  s.sold_date as kpi_date,
  s.business_unit,

  -- TOTAL BOOKED: Sum of sold estimate revenue
  SUM(s.revenue_amount) as total_booked,

  -- Additional metrics for analysis
  COUNT(s.estimate_id) as sold_estimate_count,
  COUNT(DISTINCT s.job_id) as unique_jobs_sold,
  COUNT(DISTINCT s.customer_id) as unique_customers,

  -- Average sale amount
  AVG(s.revenue_amount) as avg_sale_amount,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_stage.sold_estimates` s

WHERE s.sold_date IS NOT NULL
  AND s.business_unit IS NOT NULL

GROUP BY
  s.sold_date,
  s.business_unit

ORDER BY
  s.sold_date DESC,
  s.business_unit
;
