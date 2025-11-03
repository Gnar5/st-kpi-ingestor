-- st_mart_v2.leads_daily_bu
-- Daily count of leads (estimate jobs) by business unit
-- VALIDATED AGAINST SERVICETITAN: 2025-10-27 to 2025-11-02 (5 of 6 BUs = 100% match)
--
-- Business Logic (validated):
--   - Lead = COUNT(DISTINCT customer_id) - unique customers with estimate jobs
--   - ServiceTitan counts unique customer names, NOT total jobs
--   - Includes all estimate jobs for Sales business units (including Canceled status)
--   - Excludes test customers (filtered in st_stage.leads_jobs)
--   - Date based on job createdOn in America/Phoenix timezone
--
-- Grain: One row per date per business unit

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.leads_daily_bu` AS

SELECT
  l.lead_date as kpi_date,
  l.business_unit,

  -- LEADS COUNT: Unique customers (validated metric - matches ST definition)
  COUNT(DISTINCT l.customer_id) as leads_count,

  -- Additional metrics for analysis
  COUNT(l.job_id) as total_jobs,

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
