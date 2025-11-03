-- st_mart_v2.total_sales_daily
-- Daily total sales (sum of sold estimate subtotals) company-wide
-- VALIDATED AGAINST SERVICETITAN: 2025-10-20 to 2025-10-25 = $428,300.35 ✅
--
-- Business Logic (validated):
--   - Total Sales = SUM of estimate subtotals where status='Sold'
--   - Date based on estimate soldOn in America/Phoenix timezone
--   - Only includes Sales business units (excludes Production)
--   - Only includes estimates with valid jobId (excludes orphaned estimates)
--   - Includes all sold estimates, even if subtotal is $0
--
-- Grain: One row per date

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.total_sales_daily` AS

SELECT
  DATE(e.soldOn, 'America/Phoenix') as kpi_date,

  -- TOTAL SALES: Sum of sold estimate subtotals (validated metric)
  ROUND(SUM(CAST(e.subtotal AS FLOAT64)), 2) as total_sales,

  -- Additional metrics for analysis
  COUNT(DISTINCT e.id) as sold_estimate_count,
  COUNT(DISTINCT e.jobId) as job_count,
  COUNT(DISTINCT j.businessUnitNormalized) as business_units_with_sales,
  ROUND(AVG(CAST(e.subtotal AS FLOAT64)), 2) as avg_sale_amount,
  ROUND(MIN(CAST(e.subtotal AS FLOAT64)), 2) as min_sale_amount,
  ROUND(MAX(CAST(e.subtotal AS FLOAT64)), 2) as max_sale_amount,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
LEFT JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id

WHERE e.soldOn IS NOT NULL
  AND e.status = 'Sold'
  AND e.jobId IS NOT NULL  -- Exclude orphaned estimates
  AND j.businessUnitNormalized LIKE '%-Sales'  -- Only Sales business units

GROUP BY
  kpi_date

ORDER BY
  kpi_date DESC
;
