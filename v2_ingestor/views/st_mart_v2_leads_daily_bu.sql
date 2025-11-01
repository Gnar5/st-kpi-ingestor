-- st_mart_v2.leads_daily_bu
-- Daily count of leads (estimate jobs) by business unit
-- VALIDATED AGAINST SERVICETITAN: 2025-10-20 to 2025-10-26 = 241 leads ✅
--
-- Business Logic (validated):
--   - Lead = COUNT of estimate jobs created on date (not unique customers)
--   - Includes all estimate jobs for Sales business units
--   - Excludes test customers (filtered in st_stage.leads_jobs)
--   - Date based on job createdOn in America/Phoenix timezone
--
-- Grain: One row per date per business unit

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.leads_daily_bu` AS

SELECT
  l.lead_date as kpi_date,
  l.business_unit,

  -- LEADS COUNT: Total estimate jobs (validated metric)
  COUNT(l.job_id) as leads_count,

  -- Additional metrics for analysis
  COUNT(DISTINCT l.customer_id) as unique_customers,

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
