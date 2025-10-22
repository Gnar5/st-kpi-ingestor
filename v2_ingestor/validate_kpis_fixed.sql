-- KPI Validation Queries - FIXED VERSION
-- Use these to compare BigQuery KPIs against ServiceTitan UI

-- =============================================================================
-- QUERY 1: Single Day KPIs for Specific Business Unit
-- =============================================================================

-- Test for October 15, 2025 - Phoenix-Sales
SELECT
  event_date,
  business_unit,
  lead_count,
  total_booked,
  dollars_produced,
  gpm_percent,
  dollars_collected,
  num_estimates,
  close_rate_percent,
  future_bookings,
  warranty_percent,
  outstanding_ar
FROM `kpi-auto-471020.st_mart_v2.daily_kpis`
WHERE event_date = '2025-10-15'
  AND business_unit = 'Phoenix-Sales';

  ------------------------------------------------------------------------------
  --QUERY RESULTS
--Row	event_date	business_unit	lead_count	total_booked	dollars_produced	gpm_percent	dollars_collected	num_estimates	close_rate_percent	future_bookings	warranty_percent	outstanding_ar
1	2025-10-15	Phoenix-Sales	4	39384.0	0.0	0.0	0.0	26	28.205128205128204	0.0	0.0	0.0

-- SERVICE TITAN UI RESULTS
-- Lead Count: 27
-- Total Booked: $56,928.67
-- Dollars Produced: $0.00
-- GPM %: 0.00%
-- Dollars Collected: $0.00
-- Number of Estimates: 23
-- Close Rate %: 40.74%
-- Future Bookings: $0.00
-- Warranty %: 0.00%
-- Outstanding AR: $0.00
----------------------------------------------------------------------------

-- =============================================================================
-- QUERY 2: All Business Units for Single Day
-- =============================================================================

SELECT
  event_date,
  business_unit,
  lead_count,
  total_booked,
  dollars_produced,
  gpm_percent,
  dollars_collected,
  num_estimates,
  close_rate_percent
FROM `kpi-auto-471020.st_mart_v2.daily_kpis`
WHERE event_date = '2025-10-15'
ORDER BY business_unit;

-- =============================================================================
-- QUERY 3: Weekly Summary (Oct 14-20, 2025)
-- =============================================================================

SELECT
  business_unit,
  SUM(lead_count) as weekly_leads,
  ROUND(SUM(total_booked), 2) as weekly_booked,
  ROUND(SUM(dollars_produced), 2) as weekly_produced,
  ROUND(AVG(gpm_percent), 2) as avg_gpm,
  ROUND(SUM(dollars_collected), 2) as weekly_collected,
  SUM(num_estimates) as weekly_estimates,
  ROUND(AVG(close_rate_percent), 2) as avg_close_rate
FROM `kpi-auto-471020.st_mart_v2.daily_kpis`
WHERE event_date BETWEEN '2025-10-14' AND '2025-10-20'
GROUP BY business_unit
ORDER BY business_unit;

-- =============================================================================
-- QUERY 4: DRILL DOWN - Leads Detail for Phoenix-Sales on Oct 15
-- =============================================================================

SELECT
  DATE(createdOn) as job_date,
  businessUnitNormalized as business_unit,
  COUNT(DISTINCT customerId) as lead_count,
  COUNT(*) as total_jobs,
  STRING_AGG(DISTINCT jobTypeName ORDER BY jobTypeName LIMIT 10) as job_types_used
FROM `kpi-auto-471020.st_dim_v2.dim_jobs`
WHERE DATE(createdOn) = '2025-10-15'
  AND businessUnitNormalized = 'Phoenix-Sales'
  AND jobTypeName LIKE '%Estimate%'
  AND jobTypeName NOT LIKE '%COMM.%'
GROUP BY job_date, business_unit;

-- =============================================================================
-- QUERY 5: DRILL DOWN - Total Booked Detail for Phoenix-Sales on Oct 15
-- =============================================================================

SELECT
  DATE(e.createdOn) as estimate_date,
  j.businessUnitNormalized as business_unit,
  COUNT(*) as sold_estimates,
  ROUND(SUM(e.subtotal), 2) as total_booked,
  ROUND(AVG(e.subtotal), 2) as avg_estimate_value,
  STRING_AGG(DISTINCT j.jobTypeName ORDER BY j.jobTypeName LIMIT 5) as job_types
FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id
WHERE DATE(e.createdOn) = '2025-10-15'
  AND j.businessUnitNormalized = 'Phoenix-Sales'
  AND e.status = 'Sold'
GROUP BY estimate_date, business_unit;

-- =============================================================================
-- QUERY 6: DATA FRESHNESS CHECK
-- =============================================================================

SELECT
  'jobs' as entity,
  MAX(_ingested_at) as last_ingested,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_ingested_at), HOUR) as hours_old,
  COUNT(*) as total_records
FROM `kpi-auto-471020.st_raw_v2.raw_jobs`
UNION ALL
SELECT
  'invoices' as entity,
  MAX(_ingested_at) as last_ingested,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_ingested_at), HOUR) as hours_old,
  COUNT(*) as total_records
FROM `kpi-auto-471020.st_raw_v2.raw_invoices`
UNION ALL
SELECT
  'estimates' as entity,
  MAX(_ingested_at) as last_ingested,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_ingested_at), HOUR) as hours_old,
  COUNT(*) as total_records
FROM `kpi-auto-471020.st_raw_v2.raw_estimates`
UNION ALL
SELECT
  'payments' as entity,
  MAX(_ingested_at) as last_ingested,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_ingested_at), HOUR) as hours_old,
  COUNT(*) as total_records
FROM `kpi-auto-471020.st_raw_v2.raw_payments`
UNION ALL
SELECT
  'payroll' as entity,
  MAX(_ingested_at) as last_ingested,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_ingested_at), HOUR) as hours_old,
  COUNT(*) as total_records
FROM `kpi-auto-471020.st_raw_v2.raw_payroll`
UNION ALL
SELECT
  'purchase_orders' as entity,
  MAX(_ingested_at) as last_ingested,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_ingested_at), HOUR) as hours_old,
  COUNT(*) as total_records
FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders`
UNION ALL
SELECT
  'returns' as entity,
  MAX(_ingested_at) as last_ingested,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_ingested_at), HOUR) as hours_old,
  COUNT(*) as total_records
FROM `kpi-auto-471020.st_raw_v2.raw_returns`
ORDER BY entity;

-- =============================================================================
-- QUERY 7: Compare Specific Date to ServiceTitan UI
-- PASTE YOUR SERVICETITAN NUMBERS HERE AS COMMENTS
-- =============================================================================

/*
ServiceTitan UI Results for October 15, 2025 - Phoenix-Sales:
Leads: [PASTE HERE]
Total Booked: [PASTE HERE]
$ Produced: [PASTE HERE]
GPM %: [PASTE HERE]
$ Collected: [PASTE HERE]
# Estimates: [PASTE HERE]
Close Rate %: [PASTE HERE]
*/

-- Run this query and compare to ST UI above:
SELECT
  'BigQuery Result' as source,
  event_date,
  business_unit,
  lead_count,
  ROUND(total_booked, 2) as total_booked,
  ROUND(dollars_produced, 2) as dollars_produced,
  ROUND(gpm_percent, 2) as gpm_percent,
  ROUND(dollars_collected, 2) as dollars_collected,
  num_estimates,
  ROUND(close_rate_percent, 2) as close_rate_percent
FROM `kpi-auto-471020.st_mart_v2.daily_kpis`
WHERE event_date = '2025-10-15'
  AND business_unit = 'Phoenix-Sales';

-- =============================================================================
-- QUERY 8: Business Unit Coverage Check
-- =============================================================================

-- Make sure all business units are represented
SELECT
  business_unit,
  MIN(event_date) as first_date,
  MAX(event_date) as last_date,
  COUNT(DISTINCT event_date) as days_with_data
FROM `kpi-auto-471020.st_mart_v2.daily_kpis`
GROUP BY business_unit
ORDER BY business_unit;
