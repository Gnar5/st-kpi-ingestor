-- st_mart_v2.leads_daily
-- Daily count of unique leads (customers) company-wide
--
-- Business Logic:
--   - Lead = unique customer with at least one eligible job created on date
--   - Counts distinct customers across all business units
--   - Date based on job createdOn in America/Phoenix timezone
--
-- Grain: One row per date (company-wide aggregate)

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.leads_daily` AS

SELECT
  l.lead_date as kpi_date,

  -- Unique customer count (company-wide)
  COUNT(DISTINCT l.customer_id) as unique_leads,

  -- Additional metrics for analysis
  COUNT(DISTINCT l.job_id) as total_lead_jobs,
  COUNT(DISTINCT l.business_unit_id) as business_units_with_leads,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_stage.leads_jobs` l

WHERE l.lead_date IS NOT NULL

GROUP BY l.lead_date

ORDER BY l.lead_date DESC
;
