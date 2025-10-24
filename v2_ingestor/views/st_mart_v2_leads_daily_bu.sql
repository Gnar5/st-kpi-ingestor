-- st_mart_v2.leads_daily_bu
-- Daily count of unique leads (customers) by business unit
--
-- Business Logic:
--   - Lead = unique customer with at least one eligible job created on date
--   - Counts distinct customers per business unit per day
--   - Date based on job createdOn in America/Phoenix timezone
--
-- Grain: One row per date per business unit

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.leads_daily_bu` AS

SELECT
  l.lead_date as kpi_date,
  l.business_unit_id,
  bu.businessUnitName as business_unit_name,

  -- Unique customer count per BU
  COUNT(DISTINCT l.customer_id) as unique_leads,

  -- Additional metrics for analysis
  COUNT(DISTINCT l.job_id) as total_lead_jobs,
  COUNT(l.job_id) as total_jobs_including_duplicates,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_stage.leads_jobs` l
LEFT JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` bu
  ON l.job_id = bu.id

WHERE l.lead_date IS NOT NULL

GROUP BY
  l.lead_date,
  l.business_unit_id,
  bu.businessUnitName

ORDER BY
  l.lead_date DESC,
  l.business_unit_id
;
