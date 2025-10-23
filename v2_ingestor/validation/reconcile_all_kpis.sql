-- ================================================================================
-- COMPREHENSIVE KPI RECONCILIATION QUERY
-- Purpose: Validate ALL KPIs against ServiceTitan exports with 0% variance goal
-- Date: 2025-10-23
-- ================================================================================

DECLARE start_date DATE DEFAULT '2025-08-18';
DECLARE end_date DATE DEFAULT '2025-08-24';

-- ================================================================================
-- SECTION A: ENUMERATE ALL KPIs WITH CURRENT VALUES
-- ================================================================================

WITH
-- Sales KPIs from current mart
sales_metrics AS (
  SELECT
    business_unit,
    SUM(lead_count) as leads,
    SUM(estimate_count) as estimates,
    SUM(total_booked) as total_booked,
    SAFE_DIVIDE(SUM(total_booked), NULLIF(SUM(estimate_count), 0)) as avg_ticket_booked,
    AVG(close_rate) * 100 as close_rate_pct
  FROM `kpi-auto-471020.st_mart_v2.daily_kpis`
  WHERE business_unit LIKE '%Sales'
    AND event_date BETWEEN start_date AND end_date
  GROUP BY business_unit
),

-- Production KPIs from current mart
production_metrics AS (
  SELECT
    business_unit,
    SUM(dollars_produced) as dollars_produced,
    AVG(gpm_percent) as gpm_percent,
    AVG(warranty_percent) as warranty_percent,
    AVG(labor_efficiency) as labor_efficiency,
    AVG(material_cost_percent) as material_cost_percent,
    SUM(completed_job_count) as completed_jobs,
    SUM(warranty_job_count) as warranty_jobs
  FROM `kpi-auto-471020.st_mart_v2.daily_kpis`
  WHERE business_unit LIKE '%Production'
    AND event_date BETWEEN start_date AND end_date
  GROUP BY business_unit
),

-- Future Bookings (jobs scheduled beyond current period)
future_bookings AS (
  SELECT
    j.businessUnitNormalized as business_unit,
    COUNT(DISTINCT j.id) as future_job_count,
    SUM(COALESCE(e.total, e.subTotal)) as future_booking_value
  FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
  JOIN `kpi-auto-471020.st_raw_v2.raw_estimates` e ON j.id = e.jobId
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_appointments` a ON j.id = a.jobId
  WHERE e.status = 'Sold'
    AND DATE(a.scheduledStart) > end_date
    AND DATE(e.soldOn) BETWEEN start_date AND end_date
  GROUP BY business_unit
),

-- Dollars Collected (payments received in period)
dollars_collected AS (
  SELECT
    j.businessUnitNormalized as business_unit,
    SUM(p.amount) as collected_amount,
    COUNT(DISTINCT p.id) as payment_count
  FROM `kpi-auto-471020.st_raw_v2.raw_payments` p
  JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON p.invoiceId = i.id
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
  WHERE DATE(p.createdOn) BETWEEN start_date AND end_date
  GROUP BY business_unit
),

-- Outstanding A/R (unpaid invoice balances)
outstanding_ar AS (
  SELECT
    j.businessUnitNormalized as business_unit,
    SUM(i.total) as total_invoiced,
    SUM(i.balance) as outstanding_balance,
    COUNT(DISTINCT CASE WHEN i.balance > 0 THEN i.id END) as open_invoice_count
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
  WHERE DATE(i.createdOn) <= end_date
    AND i.balance > 0
  GROUP BY business_unit
),

-- Estimates Scheduled (estimates with appointments)
estimates_scheduled AS (
  SELECT
    j.businessUnitNormalized as business_unit,
    COUNT(DISTINCT e.id) as scheduled_estimate_count,
    SUM(COALESCE(e.total, e.subTotal)) as scheduled_estimate_value
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id
  JOIN `kpi-auto-471020.st_raw_v2.raw_appointments` a ON j.id = a.jobId
  WHERE DATE(e.createdOn) BETWEEN start_date AND end_date
    AND a.scheduledStart IS NOT NULL
  GROUP BY business_unit
),

-- Job Types Analysis (for warranty identification)
job_type_analysis AS (
  SELECT
    j.businessUnitNormalized as business_unit,
    j.jobTypeId,
    jt.name as job_type_name,
    COUNT(DISTINCT j.id) as job_count,
    SUM(i.subTotal) as revenue,
    CASE
      WHEN LOWER(jt.name) LIKE '%warranty%' THEN 'Warranty'
      WHEN LOWER(jt.name) LIKE '%touch%up%' THEN 'Warranty'
      WHEN LOWER(jt.name) LIKE '%callback%' THEN 'Warranty'
      WHEN LOWER(jt.name) LIKE '%repair%' THEN 'Warranty'
      ELSE 'Regular'
    END as job_category
  FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_job_types` jt ON j.jobTypeId = jt.id
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON j.id = i.jobId
  WHERE DATE(j.createdOn) BETWEEN start_date AND end_date
  GROUP BY business_unit, j.jobTypeId, jt.name
),

-- Lead Source Analysis (for lead count validation)
lead_analysis AS (
  SELECT
    j.businessUnitNormalized as business_unit,
    j.campaignId,
    c.name as campaign_name,
    COUNT(DISTINCT j.id) as lead_count,
    COUNT(DISTINCT CASE WHEN e.id IS NOT NULL THEN j.id END) as leads_with_estimates,
    COUNT(DISTINCT CASE WHEN e.status = 'Sold' THEN j.id END) as leads_sold
  FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_campaigns` c ON j.campaignId = c.id
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_estimates` e ON j.id = e.jobId
  WHERE DATE(j.createdOn) BETWEEN start_date AND end_date
    AND j.businessUnitNormalized LIKE '%Sales'
  GROUP BY business_unit, j.campaignId, c.name
)

-- ================================================================================
-- SECTION B: CONSOLIDATE ALL METRICS
-- ================================================================================
SELECT
  COALESCE(s.business_unit, p.business_unit, f.business_unit) as business_unit,

  -- Sales KPIs
  ROUND(s.leads, 0) as leads,
  ROUND(s.estimates, 0) as estimates,
  ROUND(s.total_booked, 2) as total_booked,
  ROUND(s.close_rate_pct, 2) as close_rate_pct,

  -- Production KPIs
  ROUND(p.dollars_produced, 2) as dollars_produced,
  ROUND(p.gpm_percent, 2) as gpm_percent,
  ROUND(p.warranty_percent, 2) as warranty_percent,

  -- Future Bookings
  ROUND(COALESCE(f.future_booking_value, 0), 2) as future_bookings,

  -- Collections
  ROUND(COALESCE(dc.collected_amount, 0), 2) as dollars_collected,

  -- Outstanding A/R
  ROUND(COALESCE(ar.outstanding_balance, 0), 2) as outstanding_ar,

  -- Estimates Scheduled
  ROUND(COALESCE(es.scheduled_estimate_count, 0), 0) as estimates_scheduled

FROM sales_metrics s
FULL OUTER JOIN production_metrics p
  ON s.business_unit = REPLACE(p.business_unit, 'Production', 'Sales')
FULL OUTER JOIN future_bookings f
  ON COALESCE(s.business_unit, REPLACE(p.business_unit, 'Production', 'Sales')) = f.business_unit
FULL OUTER JOIN dollars_collected dc
  ON COALESCE(s.business_unit, p.business_unit, f.business_unit) = dc.business_unit
FULL OUTER JOIN outstanding_ar ar
  ON COALESCE(s.business_unit, p.business_unit, f.business_unit) = ar.business_unit
FULL OUTER JOIN estimates_scheduled es
  ON COALESCE(s.business_unit, p.business_unit, f.business_unit) = es.business_unit

ORDER BY business_unit;

-- ================================================================================
-- DIAGNOSTIC QUERIES (Run separately for deep-dive analysis)
-- ================================================================================

-- 1. Close Rate Calculation Validation
/*
SELECT
  business_unit,
  COUNT(DISTINCT j.id) as total_opportunities,
  COUNT(DISTINCT CASE WHEN e.status = 'Sold' THEN j.id END) as sold_opportunities,
  ROUND(SAFE_DIVIDE(
    COUNT(DISTINCT CASE WHEN e.status = 'Sold' THEN j.id END),
    COUNT(DISTINCT j.id)
  ) * 100, 2) as calculated_close_rate
FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_estimates` e ON j.id = e.jobId
WHERE DATE(j.createdOn) BETWEEN start_date AND end_date
  AND j.businessUnitNormalized LIKE '%Sales'
GROUP BY business_unit;
*/

-- 2. GPM Calculation Validation
/*
SELECT
  businessUnitNormalized,
  ROUND(AVG(gross_profit_margin), 2) as avg_gpm,
  ROUND(SUM(gross_profit), 2) as total_gross_profit,
  ROUND(SUM(revenue_subtotal), 2) as total_revenue,
  ROUND(SAFE_DIVIDE(SUM(gross_profit), SUM(revenue_subtotal)) * 100, 2) as weighted_gpm
FROM `kpi-auto-471020.st_mart_v2.job_costing`
WHERE DATE(job_start_date) BETWEEN start_date AND end_date
  AND jobStatus IN ('Completed', 'Hold')
GROUP BY businessUnitNormalized;
*/

-- 3. Warranty Analysis
/*
SELECT
  businessUnitNormalized,
  COUNT(CASE WHEN is_warranty THEN 1 END) as warranty_jobs,
  COUNT(*) as total_jobs,
  ROUND(SUM(CASE WHEN is_warranty THEN total_cost END), 2) as warranty_cost,
  ROUND(SUM(revenue_subtotal), 2) as total_revenue,
  ROUND(SAFE_DIVIDE(
    SUM(CASE WHEN is_warranty THEN total_cost END),
    SUM(revenue_subtotal)
  ) * 100, 2) as warranty_pct_of_revenue
FROM `kpi-auto-471020.st_mart_v2.job_costing`
WHERE DATE(job_start_date) BETWEEN start_date AND end_date
  AND jobStatus IN ('Completed', 'Hold')
GROUP BY businessUnitNormalized;
*/

-- 4. Lead Source Effectiveness
/*
SELECT
  j.businessUnitNormalized,
  c.name as campaign_name,
  COUNT(DISTINCT j.id) as leads,
  COUNT(DISTINCT e.id) as estimates,
  COUNT(DISTINCT CASE WHEN e.status = 'Sold' THEN e.id END) as sold,
  ROUND(SUM(CASE WHEN e.status = 'Sold' THEN COALESCE(e.total, e.subTotal) END), 2) as revenue
FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_campaigns` c ON j.campaignId = c.id
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_estimates` e ON j.id = e.jobId
WHERE DATE(j.createdOn) BETWEEN start_date AND end_date
GROUP BY j.businessUnitNormalized, c.name
ORDER BY j.businessUnitNormalized, revenue DESC;
*/