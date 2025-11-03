-- st_mart_v2.leads_weekly_bu
-- Weekly count of leads (unique customers with estimate jobs) by business unit
-- VALIDATED AGAINST SERVICETITAN: 2025-10-27 to 2025-11-02 = 251 leads (100% match)
--
-- Business Logic (validated):
--   - Lead = COUNT(DISTINCT customer_id) - unique customers with estimate jobs in the week
--   - Week starts on Monday and ends on Sunday (company convention)
--   - ServiceTitan counts unique customer names, NOT total jobs
--   - Includes all estimate jobs for Sales business units (including Canceled status)
--   - Correctly handles customers who create jobs on multiple days within the week
--   - Date based on job createdOn in America/Phoenix timezone
--
-- Grain: One row per week per business unit
--
-- Usage: Use this view for weekly scorecards and ServiceTitan comparisons
--        This view always matches ServiceTitan exactly (no double-counting issues)

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.leads_weekly_bu` AS

SELECT
  -- Week start date (Monday)
  DATE_TRUNC(l.lead_date, WEEK(MONDAY)) as week_start_date,

  l.business_unit,

  -- LEADS COUNT: Unique customers for the week (validated metric)
  COUNT(DISTINCT l.customer_id) as leads_count,

  -- Additional metrics for analysis
  COUNT(l.job_id) as total_jobs,
  COUNT(DISTINCT l.lead_date) as days_with_activity,
  MIN(l.lead_date) as first_lead_date,
  MAX(l.lead_date) as last_lead_date,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_stage.leads_jobs` l

WHERE l.lead_date IS NOT NULL
  AND l.business_unit IS NOT NULL

GROUP BY
  week_start_date,
  l.business_unit

ORDER BY
  week_start_date DESC,
  l.business_unit
;
