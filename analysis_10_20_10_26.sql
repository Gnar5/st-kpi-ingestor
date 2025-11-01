-- Analysis of KPI Discrepancies for 10/20/2025 - 10/26/2025
-- Created: 2025-10-30
-- Purpose: Identify root causes of Total Booked and Close Rate discrepancies

-- ============================================================================
-- 1. TOTAL BOOKED - TEST DIFFERENT DATE LOGIC
-- ============================================================================

-- Current Logic: Using estimate soldOn date
SELECT
  'Current Logic (estimate.soldOn)' as method,
  COUNT(DISTINCT e.id) as estimate_count,
  ROUND(SUM(COALESCE(e.total, e.subTotal)), 2) as total_booked
FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
WHERE e.status = 'Sold'
  AND DATE(e.soldOn, 'America/Phoenix') BETWEEN '2025-10-20' AND '2025-10-26'

UNION ALL

-- Test: Using job completedOn date for sold estimates
SELECT
  'Test: job.completedOn for Sold estimates' as method,
  COUNT(DISTINCT e.id) as estimate_count,
  ROUND(SUM(COALESCE(e.total, e.subTotal)), 2) as total_booked
FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id
WHERE e.status = 'Sold'
  AND DATE(j.completedOn, 'America/Phoenix') BETWEEN '2025-10-20' AND '2025-10-26'
  AND LOWER(j.jobTypeName) LIKE '%estimate%'

UNION ALL

-- Test: ALL estimates for completed estimate jobs (not just sold)
SELECT
  'Test: ALL estimates for completed jobs' as method,
  COUNT(DISTINCT e.id) as estimate_count,
  ROUND(SUM(COALESCE(e.total, e.subTotal)), 2) as total_booked
FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id
WHERE DATE(j.completedOn, 'America/Phoenix') BETWEEN '2025-10-20' AND '2025-10-26'
  AND LOWER(j.jobTypeName) LIKE '%estimate%'
  AND j.jobStatus = 'Completed'

UNION ALL

-- Test: Invoices for completed estimate jobs
SELECT
  'Test: Invoices for completed estimate jobs' as method,
  COUNT(DISTINCT i.id) as estimate_count,
  ROUND(SUM(i.total), 2) as total_booked
FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
WHERE DATE(j.completedOn, 'America/Phoenix') BETWEEN '2025-10-20' AND '2025-10-26'
  AND LOWER(j.jobTypeName) LIKE '%estimate%'
  AND j.jobStatus = 'Completed'

ORDER BY total_booked DESC;

-- ============================================================================
-- 2. CLOSE RATE - UNDERSTAND OPPORTUNITY DEFINITION
-- ============================================================================

-- Current Logic: Any job with estimate
SELECT
  'Current Logic' as method,
  COUNT(*) as total_jobs,
  SUM(CASE WHEN is_sales_opportunity THEN 1 END) as sales_opps,
  SUM(CASE WHEN is_closed_opportunity THEN 1 END) as closed_opps,
  ROUND(SUM(CASE WHEN is_closed_opportunity THEN 1 END) /
    NULLIF(SUM(CASE WHEN is_sales_opportunity THEN 1 END), 0) * 100, 2) as close_rate
FROM `kpi-auto-471020.st_stage.opportunity_jobs`
WHERE opportunity_date BETWEEN '2025-10-20' AND '2025-10-26'

UNION ALL

-- Test: Only COMPLETED jobs with estimates
SELECT
  'Test: Completed jobs only' as method,
  COUNT(DISTINCT j.id) as total_jobs,
  COUNT(DISTINCT j.id) as sales_opps,  -- All completed estimate jobs are opportunities
  COUNT(DISTINCT CASE WHEN e.sold_estimate_count > 0 THEN j.id END) as closed_opps,
  ROUND(COUNT(DISTINCT CASE WHEN e.sold_estimate_count > 0 THEN j.id END) /
    NULLIF(COUNT(DISTINCT j.id), 0) * 100, 2) as close_rate
FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
LEFT JOIN (
  SELECT jobId, COUNT(CASE WHEN status = 'Sold' THEN 1 END) as sold_estimate_count
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates`
  GROUP BY jobId
) e ON j.id = e.jobId
WHERE DATE(j.completedOn, 'America/Phoenix') BETWEEN '2025-10-20' AND '2025-10-26'
  AND LOWER(j.jobTypeName) LIKE '%estimate%'
  AND j.jobStatus = 'Completed'
  AND LOWER(j.jobTypeName) NOT LIKE '%commercial%';

-- ============================================================================
-- 3. DETAILED BREAKDOWN BY BUSINESS UNIT
-- ============================================================================

SELECT
  j.businessUnitNormalized as business_unit,

  -- Completed Estimates (this should = 190 total)
  COUNT(DISTINCT CASE WHEN j.jobStatus = 'Completed' AND LOWER(j.jobTypeName) LIKE '%estimate%'
    THEN j.id END) as completed_estimates,

  -- Opportunities (jobs with completed estimates)
  COUNT(DISTINCT CASE WHEN j.jobStatus = 'Completed' AND LOWER(j.jobTypeName) LIKE '%estimate%'
    THEN j.id END) as sales_opportunities,

  -- Closed Opportunities (completed estimates with sold estimate)
  COUNT(DISTINCT CASE WHEN j.jobStatus = 'Completed' AND LOWER(j.jobTypeName) LIKE '%estimate%'
    AND est.sold_count > 0 THEN j.id END) as closed_opportunities,

  -- Close Rate
  ROUND(
    COUNT(DISTINCT CASE WHEN j.jobStatus = 'Completed' AND LOWER(j.jobTypeName) LIKE '%estimate%'
      AND est.sold_count > 0 THEN j.id END) /
    NULLIF(COUNT(DISTINCT CASE WHEN j.jobStatus = 'Completed' AND LOWER(j.jobTypeName) LIKE '%estimate%'
      THEN j.id END), 0) * 100,
    2
  ) as close_rate_percent,

  -- Total Booked (sum of sold estimates)
  ROUND(SUM(CASE WHEN est.sold_count > 0 THEN est.total_sold_amount END), 2) as total_booked

FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
LEFT JOIN (
  SELECT
    jobId,
    COUNT(CASE WHEN status = 'Sold' THEN 1 END) as sold_count,
    SUM(CASE WHEN status = 'Sold' THEN COALESCE(total, subTotal) END) as total_sold_amount
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates`
  GROUP BY jobId
) est ON j.id = est.jobId

WHERE DATE(j.completedOn, 'America/Phoenix') BETWEEN '2025-10-20' AND '2025-10-26'

GROUP BY j.businessUnitNormalized
ORDER BY business_unit;

-- ============================================================================
-- 4. COMPARE TO SERVICE TITAN BASELINE
-- ============================================================================

WITH servicetitan_baseline AS (
  SELECT 'Total' as business_unit, 227 as st_leads, 190 as st_completed_est,
         40.34 as st_close_rate, 434600.00 as st_total_booked
),

bigquery_current AS (
  SELECT
    'Total' as business_unit,
    SUM(leads_count) as bq_leads,
    (SELECT SUM(completed_estimates_count)
     FROM `kpi-auto-471020.st_mart_v2.completed_estimates_daily`
     WHERE kpi_date BETWEEN '2025-10-20' AND '2025-10-26') as bq_completed_est,
    (SELECT ROUND(SUM(closed_opportunities) / NULLIF(SUM(sales_opportunities), 0) * 100, 2)
     FROM `kpi-auto-471020.st_mart_v2.opportunity_daily`
     WHERE kpi_date BETWEEN '2025-10-20' AND '2025-10-26') as bq_close_rate,
    (SELECT SUM(total_booked)
     FROM `kpi-auto-471020.st_mart_v2.total_booked_daily`
     WHERE kpi_date BETWEEN '2025-10-20' AND '2025-10-26') as bq_total_booked
  FROM `kpi-auto-471020.st_mart_v2.leads_daily_bu`
  WHERE kpi_date BETWEEN '2025-10-20' AND '2025-10-26'
)

SELECT
  'Leads' as kpi,
  st.st_leads as servicetitan,
  bq.bq_leads as bigquery,
  bq.bq_leads - st.st_leads as variance,
  ROUND((bq.bq_leads - st.st_leads) / st.st_leads * 100, 2) as variance_pct
FROM servicetitan_baseline st, bigquery_current bq

UNION ALL

SELECT
  'Completed Estimates',
  st.st_completed_est,
  bq.bq_completed_est,
  bq.bq_completed_est - st.st_completed_est,
  ROUND((bq.bq_completed_est - st.st_completed_est) / st.st_completed_est * 100, 2)
FROM servicetitan_baseline st, bigquery_current bq

UNION ALL

SELECT
  'Close Rate %',
  st.st_close_rate,
  bq.bq_close_rate,
  bq.bq_close_rate - st.st_close_rate,
  ROUND((bq.bq_close_rate - st.st_close_rate) / st.st_close_rate * 100, 2)
FROM servicetitan_baseline st, bigquery_current bq

UNION ALL

SELECT
  'Total Booked $',
  st.st_total_booked,
  bq.bq_total_booked,
  bq.bq_total_booked - st.st_total_booked,
  ROUND((bq.bq_total_booked - st.st_total_booked) / st.st_total_booked * 100, 2)
FROM servicetitan_baseline st, bigquery_current bq;
