-- st_mart_v2.leads_daily
-- Daily count of leads (estimate jobs) company-wide
-- VALIDATED AGAINST SERVICETITAN: 2025-10-20 to 2025-10-26 = 241 leads ✅
--
-- Business Logic (validated):
--   - Lead = COUNT of estimate jobs created on date (not unique customers)
--   - Includes all estimate jobs for Sales business units
--   - Date based on job createdOn in America/Phoenix timezone
--
-- Grain: One row per date (company-wide aggregate)

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.leads_daily` AS

SELECT
  l.lead_date as kpi_date,

  -- LEADS COUNT: Total estimate jobs (validated metric)
  COUNT(l.job_id) as leads_count,

  -- Additional metrics for analysis
  COUNT(DISTINCT l.customer_id) as unique_customers,
  COUNT(DISTINCT l.business_unit) as business_units_with_leads,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_stage.leads_jobs` l

WHERE l.lead_date IS NOT NULL

GROUP BY l.lead_date

ORDER BY l.lead_date DESC
;
