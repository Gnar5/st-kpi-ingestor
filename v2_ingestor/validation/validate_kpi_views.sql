-- Validation Queries for Standard KPI Views
-- Run these queries after deploying views to verify correctness

-- ================================================
-- 1. ALLOWLIST VERIFICATION
-- ================================================
-- Check that allowlist jobTypeIds exist in raw data

SELECT
  'Allowlist Verification' as test_name,
  COUNT(DISTINCT jobTypeId) as unique_job_types_found,
  COUNT(*) as total_jobs_found,
  'Expected: 10-15 jobTypeIds with ~70K+ jobs' as expectation
FROM `kpi-auto-471020.st_raw_v2.raw_jobs`
WHERE jobTypeId IN (
  705557, 705812, 727444, 727572, 7761171,
  25643501, 25640548, 40084045, 40091077, 40528050,
  52632595, 53425776, 53419951, 53417012, 66527167,
  80656917, 142931404, 144397449, 365792375
)
;

-- ================================================
-- 2. OPPORTUNITIES VIEWS VALIDATION
-- ================================================

-- Check opportunity_jobs row count
SELECT
  'Opportunity Jobs Count' as test_name,
  COUNT(*) as total_opportunity_jobs,
  COUNT(CASE WHEN is_sales_opportunity THEN 1 END) as sales_opportunities,
  COUNT(CASE WHEN is_closed_opportunity THEN 1 END) as closed_opportunities,
  ROUND(AVG(estimate_count), 2) as avg_estimates_per_job,
  'Expected: 35K-40K jobs with estimates' as expectation
FROM `kpi-auto-471020.st_stage.opportunity_jobs`
;

-- Check opportunity_daily aggregation
SELECT
  'Opportunity Daily Summary' as test_name,
  COUNT(DISTINCT kpi_date) as unique_dates,
  COUNT(DISTINCT business_unit_id) as unique_business_units,
  SUM(sales_opportunities) as total_sales_opps,
  SUM(closed_opportunities) as total_closed_opps,
  ROUND(AVG(close_rate_percent), 2) as avg_close_rate,
  'Expected: Hundreds of dates, 10+ BUs, 30-50% close rate' as expectation
FROM `kpi-auto-471020.st_mart_v2.opportunity_daily`
;

-- Sample recent opportunity data
SELECT
  kpi_date,
  business_unit_name,
  sales_opportunities,
  closed_opportunities,
  close_rate_percent
FROM `kpi-auto-471020.st_mart_v2.opportunity_daily`
WHERE kpi_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAYS)
ORDER BY kpi_date DESC, business_unit_name
LIMIT 20
;

-- ================================================
-- 3. LEADS VIEWS VALIDATION
-- ================================================

-- Check leads_jobs row count
SELECT
  'Leads Jobs Count' as test_name,
  COUNT(*) as total_lead_jobs,
  COUNT(DISTINCT customer_id) as unique_customers,
  COUNT(DISTINCT business_unit_id) as unique_business_units,
  MIN(lead_date) as earliest_lead_date,
  MAX(lead_date) as latest_lead_date,
  'Expected: 70K+ jobs, 40K+ customers' as expectation
FROM `kpi-auto-471020.st_stage.leads_jobs`
;

-- Check leads_daily company-wide aggregation
SELECT
  'Leads Daily (Company-Wide)' as test_name,
  COUNT(DISTINCT kpi_date) as unique_dates,
  SUM(unique_leads) as total_unique_leads,
  ROUND(AVG(unique_leads), 2) as avg_leads_per_day,
  'Expected: Matches leads_jobs unique customers' as expectation
FROM `kpi-auto-471020.st_mart_v2.leads_daily`
;

-- Check leads_daily_bu per-BU aggregation
SELECT
  'Leads Daily by BU' as test_name,
  COUNT(DISTINCT kpi_date) as unique_dates,
  COUNT(DISTINCT business_unit_id) as unique_business_units,
  SUM(unique_leads) as total_unique_leads,
  'Expected: Sum should match or exceed company-wide (customers may appear in multiple BUs)' as expectation
FROM `kpi-auto-471020.st_mart_v2.leads_daily_bu`
;

-- Sample recent leads data by BU
SELECT
  kpi_date,
  business_unit_name,
  unique_leads,
  total_lead_jobs
FROM `kpi-auto-471020.st_mart_v2.leads_daily_bu`
WHERE kpi_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAYS)
ORDER BY kpi_date DESC, unique_leads DESC
LIMIT 20
;

-- ================================================
-- 4. COMPLETED ESTIMATES VIEWS VALIDATION
-- ================================================

-- Check completed_estimates_jobs row count
SELECT
  'Completed Estimates Jobs Count' as test_name,
  COUNT(*) as total_completed_jobs,
  COUNT(DISTINCT customer_id) as unique_customers,
  COUNT(DISTINCT business_unit_id) as unique_business_units,
  MIN(completed_date) as earliest_completed_date,
  MAX(completed_date) as latest_completed_date,
  'Expected: Subset of leads_jobs (only completed)' as expectation
FROM `kpi-auto-471020.st_stage.completed_estimates_jobs`
;

-- Check completed_estimates_daily aggregation
SELECT
  'Completed Estimates Daily' as test_name,
  COUNT(DISTINCT kpi_date) as unique_dates,
  COUNT(DISTINCT business_unit_id) as unique_business_units,
  SUM(completed_estimates) as total_completed_estimates,
  ROUND(AVG(completed_estimates), 2) as avg_completed_per_day,
  'Expected: Should match completed_estimates_jobs count' as expectation
FROM `kpi-auto-471020.st_mart_v2.completed_estimates_daily`
;

-- Sample recent completed estimates data
SELECT
  kpi_date,
  business_unit_name,
  completed_estimates,
  unique_customers
FROM `kpi-auto-471020.st_mart_v2.completed_estimates_daily`
WHERE kpi_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAYS)
ORDER BY kpi_date DESC, completed_estimates DESC
LIMIT 20
;

-- ================================================
-- 5. CROSS-VIEW CONSISTENCY CHECKS
-- ================================================

-- Check that leads >= completed estimates (logic check)
SELECT
  'Leads vs Completed Estimates' as test_name,
  l.kpi_date,
  l.business_unit_name,
  l.unique_leads as leads,
  COALESCE(c.completed_estimates, 0) as completed_estimates,
  CASE
    WHEN COALESCE(c.completed_estimates, 0) > l.unique_leads
      THEN 'WARNING: More completed than leads!'
    ELSE 'OK'
  END as validation_status
FROM `kpi-auto-471020.st_mart_v2.leads_daily_bu` l
LEFT JOIN `kpi-auto-471020.st_mart_v2.completed_estimates_daily` c
  ON l.kpi_date = c.kpi_date AND l.business_unit_id = c.business_unit_id
WHERE l.kpi_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAYS)
  AND COALESCE(c.completed_estimates, 0) > l.unique_leads
ORDER BY l.kpi_date DESC
LIMIT 10
;

-- Check opportunity jobs have estimates (logic check)
SELECT
  'Opportunities Have Estimates' as test_name,
  COUNT(*) as opportunity_jobs_with_no_estimates,
  'Expected: 0 (all opportunity jobs should have estimates)' as expectation
FROM `kpi-auto-471020.st_stage.opportunity_jobs`
WHERE estimate_count = 0
;

-- ================================================
-- 6. TIMEZONE CONVERSION VALIDATION
-- ================================================

-- Verify timezone conversion is working (dates should differ from UTC)
SELECT
  'Timezone Conversion Check' as test_name,
  COUNT(CASE
    WHEN DATE(job_created_on_utc) != lead_date THEN 1
  END) as dates_differ_from_utc,
  COUNT(*) as total_jobs,
  ROUND(COUNT(CASE WHEN DATE(job_created_on_utc) != lead_date THEN 1 END) / COUNT(*) * 100, 2) as pct_different,
  'Expected: 10-30% should differ (depends on time of day)' as expectation
FROM `kpi-auto-471020.st_stage.leads_jobs`
WHERE job_created_on_utc IS NOT NULL
LIMIT 1
;

-- ================================================
-- 7. BUSINESS UNIT DISTRIBUTION
-- ================================================

-- Check distribution across business units
SELECT
  'Business Unit Distribution' as test_name,
  bu.business_unit_name,
  COUNT(DISTINCT l.kpi_date) as days_with_data,
  SUM(l.unique_leads) as total_leads,
  SUM(COALESCE(c.completed_estimates, 0)) as total_completed,
  SUM(COALESCE(o.sales_opportunities, 0)) as total_opportunities
FROM `kpi-auto-471020.st_mart_v2.leads_daily_bu` l
LEFT JOIN `kpi-auto-471020.st_mart_v2.completed_estimates_daily` c
  ON l.kpi_date = c.kpi_date AND l.business_unit_id = c.business_unit_id
LEFT JOIN `kpi-auto-471020.st_mart_v2.opportunity_daily` o
  ON l.kpi_date = o.kpi_date AND l.business_unit_id = o.business_unit_id
LEFT JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` bu
  ON l.business_unit_id = bu.businessUnitId
WHERE l.kpi_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAYS)
GROUP BY bu.business_unit_name
ORDER BY total_leads DESC
LIMIT 20
;

-- ================================================
-- 8. DATA FRESHNESS CHECK
-- ================================================

-- Check how recent the data is
SELECT
  'Data Freshness' as test_name,
  MAX(kpi_date) as most_recent_lead_date,
  DATE_DIFF(CURRENT_DATE(), MAX(kpi_date), DAY) as days_since_last_data,
  'Expected: 0-2 days (should be very recent)' as expectation
FROM `kpi-auto-471020.st_mart_v2.leads_daily`
;

-- ================================================
-- SUMMARY
-- ================================================

SELECT
  '========================================' as summary,
  'VALIDATION COMPLETE' as status,
  'Review results above for any warnings or unexpected values' as note
;
