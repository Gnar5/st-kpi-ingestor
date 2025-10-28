-- st_mart_v2.leads_daily_bu
-- Daily count of unique leads (customers) by business unit
-- VALIDATED AGAINST SERVICETITAN: Phoenix-Sales 2025-08-18 to 2025-08-24 = 96 leads ✓
--
-- Business Logic (validated):
--   - Lead = unique customer (COUNT DISTINCT customerId) with estimate job(s) on date
--   - Excludes test customers (filtered in st_stage.leads_jobs)
--   - Date based on job createdOn in America/Phoenix timezone
--
-- Grain: One row per date per business unit

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.leads_daily_bu` AS

SELECT
  l.lead_date as kpi_date,
  l.business_unit,

  -- LEADS COUNT: Unique customers (validated metric)
  COUNT(DISTINCT l.customer_id) as leads_count,

  -- Additional metrics for analysis
  COUNT(DISTINCT l.job_id) as total_estimate_jobs,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_stage.leads_jobs` l

WHERE l.lead_date IS NOT NULL
  AND l.business_unit IS NOT NULL

GROUP BY
  l.lead_date,
  l.business_unit

ORDER BY
  l.lead_date DESC,
  l.business_unit
;
