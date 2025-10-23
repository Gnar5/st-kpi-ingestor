-- ================================================================================
-- COMPREHENSIVE KPI RECONCILIATION - Based on ServiceTitan Report Specifications
-- Purpose: Validate ALL KPIs against ServiceTitan reports with 0% variance goal
-- Date: 2025-10-23
-- ================================================================================

DECLARE start_date DATE DEFAULT '2025-08-18';
DECLARE end_date DATE DEFAULT '2025-08-24';

-- ================================================================================
-- KPI 1: LEADS
-- ST Report: "Leads"
-- Filter: Job Creation Date, Customer Name != 'Test', Job Type contains 'Estimate' but NOT 'COMM'
-- Metric: Count Distinct Customer Name
-- ================================================================================
WITH leads_kpi AS (
  SELECT
    j.businessUnitNormalized as business_unit,
    COUNT(DISTINCT c.name) as lead_count_st_method
  FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_customers` c ON j.customerId = c.id
  WHERE DATE(j.createdOn) BETWEEN start_date AND end_date
    AND j.businessUnitNormalized LIKE '%Sales'
    AND (c.name IS NULL OR UPPER(c.name) NOT LIKE '%TEST%')
    -- Job Type filter: contains 'Estimate' but not 'COMM'
    -- Since we don't have job type names, we'll use jobTypeId patterns
    -- This needs to be calibrated based on actual jobTypeId mappings
  GROUP BY j.businessUnitNormalized
),

-- ================================================================================
-- KPI 2: TOTAL BOOKED
-- ST Report: "Daily WBR C/R"
-- Filter: Specific Job Types (ESTIMATE variants)
-- Metric: Sum of "Total Sales" Column
-- Note: We already have this working correctly with soldOn date
-- ================================================================================
total_booked_kpi AS (
  SELECT
    j.businessUnitNormalized as business_unit,
    ROUND(SUM(COALESCE(e.total, e.subTotal)), 2) as total_booked
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id
  WHERE j.businessUnitNormalized LIKE '%Sales'
    AND e.status = 'Sold'
    AND DATE(e.soldOn) BETWEEN start_date AND end_date
  GROUP BY j.businessUnitNormalized
),

-- ================================================================================
-- KPI 3: DOLLARS PRODUCED
-- ST Report: "KPIs FOREMAN Job Cost - THIS WEEK ONLY"
-- Filter: Job Start Date, Job Type NOT IN (PM Inspection, Safety Inspection, Window/Solar Washing)
-- Metric: Sum of "Jobs Subtotal" Column
-- Note: We already have this working correctly with job_start_date and Hold status
-- ================================================================================
dollars_produced_kpi AS (
  SELECT
    jc.businessUnitNormalized as business_unit,
    ROUND(SUM(jc.revenue_subtotal), 2) as dollars_produced
  FROM `kpi-auto-471020.st_mart_v2.job_costing` jc
  WHERE jc.businessUnitNormalized LIKE '%Production'
    AND DATE(jc.job_start_date) BETWEEN start_date AND end_date
    AND jc.jobStatus IN ('Completed', 'Hold')
    -- TODO: Exclude PM Inspection, Safety Inspection, Window/Solar Washing job types
  GROUP BY jc.businessUnitNormalized
),

-- ================================================================================
-- KPI 4: GPM %
-- ST Report: "KPIs FOREMAN Job Cost - THIS WEEK ONLY"
-- Method 1: Average of "Job Gross Margin %" Column
-- Method 2: Total Sum "Jobs Subtotal" / Sum "Jobs Total Costs" x 100
-- ================================================================================
gpm_kpi AS (
  SELECT
    jc.businessUnitNormalized as business_unit,
    -- Method 1: Simple average (less accurate)
    ROUND(AVG(jc.gpm_percent), 2) as gpm_avg_method,
    -- Method 2: Weighted average (more accurate - what ST likely uses)
    ROUND(SAFE_DIVIDE(
      SUM(jc.gross_profit),
      NULLIF(SUM(jc.revenue_subtotal), 0)
    ) * 100, 2) as gpm_weighted_method
  FROM `kpi-auto-471020.st_mart_v2.job_costing` jc
  WHERE jc.businessUnitNormalized LIKE '%Production'
    AND DATE(jc.job_start_date) BETWEEN start_date AND end_date
    AND jc.jobStatus IN ('Completed', 'Hold')
  GROUP BY jc.businessUnitNormalized
),

-- ================================================================================
-- KPI 5: DOLLARS COLLECTED
-- ST Report: "Collections"
-- Filter: Business Unit (Production only)
-- Metric: Sum of "Amount" Column
-- ================================================================================
dollars_collected_kpi AS (
  SELECT
    j.businessUnitNormalized as business_unit,
    ROUND(SUM(p.amount), 2) as dollars_collected
  FROM `kpi-auto-471020.st_raw_v2.raw_payments` p
  JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON p.invoiceId = i.id
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
  WHERE DATE(p.createdOn) BETWEEN start_date AND end_date
    AND j.businessUnitNormalized LIKE '%Production'
  GROUP BY j.businessUnitNormalized
),

-- ================================================================================
-- KPI 6: ESTIMATES COUNT
-- ST Report: "Daily WBR C/R"
-- Filter: Specific Job Types (ESTIMATE variants)
-- Metric: Sum of "Completed Job" Column
-- ================================================================================
estimates_kpi AS (
  SELECT
    j.businessUnitNormalized as business_unit,
    COUNT(DISTINCT e.id) as estimate_count,
    COUNT(DISTINCT CASE WHEN e.status = 'Sold' THEN e.id END) as sold_estimates
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id
  WHERE j.businessUnitNormalized LIKE '%Sales'
    AND DATE(e.createdOn) BETWEEN start_date AND end_date
  GROUP BY j.businessUnitNormalized
),

-- ================================================================================
-- KPI 7: SUCCESS RATE (Close Rate)
-- ST Report: "Daily WBR C/R"
-- Filter: Specific Job Types (ESTIMATE variants)
-- Metric: "Close Rate" Average for Whole Column
-- ================================================================================
success_rate_kpi AS (
  SELECT
    j.businessUnitNormalized as business_unit,
    COUNT(DISTINCT e.id) as total_estimates,
    COUNT(DISTINCT CASE WHEN e.status = 'Sold' THEN e.id END) as sold_estimates,
    ROUND(SAFE_DIVIDE(
      COUNT(DISTINCT CASE WHEN e.status = 'Sold' THEN e.id END),
      COUNT(DISTINCT e.id)
    ) * 100, 2) as close_rate_pct
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id
  WHERE j.businessUnitNormalized LIKE '%Sales'
    AND DATE(e.createdOn) BETWEEN start_date AND end_date
  GROUP BY j.businessUnitNormalized
),

-- ================================================================================
-- KPI 8: FUTURE BOOKINGS
-- ST Report: "KPIs FOREMAN Job Cost"
-- Filter: Job Start Date from current Monday through full year
-- Metric: Sum of "Jobs Subtotal" Column
-- ================================================================================
future_bookings_kpi AS (
  SELECT
    jc.businessUnitNormalized as business_unit,
    ROUND(SUM(
      CASE
        WHEN DATE(jc.job_start_date) > end_date
        THEN jc.revenue_subtotal
        ELSE 0
      END
    ), 2) as future_bookings
  FROM `kpi-auto-471020.st_mart_v2.job_costing` jc
  WHERE jc.businessUnitNormalized LIKE '%Production'
    AND DATE(jc.job_start_date) > end_date
    AND DATE(jc.job_start_date) <= DATE_ADD(end_date, INTERVAL 1 YEAR)
    AND jc.jobStatus IN ('Scheduled', 'In Progress', 'Hold', 'Completed')
  GROUP BY jc.businessUnitNormalized
),

-- ================================================================================
-- KPI 9: WARRANTY %
-- ST Report: "KPIs FOREMAN Job Cost"
-- Filter: Job Type IN (Warranty, Touchup)
-- Metric: "Jobs Total Cost" / "$ Produced" as percentage
-- ================================================================================
warranty_kpi AS (
  SELECT
    jc.businessUnitNormalized as business_unit,
    -- Total cost of warranty jobs
    ROUND(SUM(
      CASE
        WHEN jc.is_warranty = TRUE
        THEN jc.total_cost
        ELSE 0
      END
    ), 2) as warranty_cost,
    -- Total dollars produced
    ROUND(SUM(jc.revenue_subtotal), 2) as total_produced,
    -- Warranty % calculation
    ROUND(SAFE_DIVIDE(
      SUM(CASE WHEN jc.is_warranty = TRUE THEN jc.total_cost ELSE 0 END),
      NULLIF(SUM(jc.revenue_subtotal), 0)
    ) * 100, 2) as warranty_pct
  FROM `kpi-auto-471020.st_mart_v2.job_costing` jc
  WHERE jc.businessUnitNormalized LIKE '%Production'
    AND DATE(jc.job_start_date) BETWEEN start_date AND end_date
    AND jc.jobStatus IN ('Completed', 'Hold')
  GROUP BY jc.businessUnitNormalized
),

-- ================================================================================
-- KPI 10: OUTSTANDING A/R
-- ST Report: "AR Report"
-- Filter: Location Name != Name, Net Amount >= 10, Business Unit (Production)
-- Metric: Sum total of "Net Amount" Column
-- ================================================================================
outstanding_ar_kpi AS (
  SELECT
    j.businessUnitNormalized as business_unit,
    ROUND(SUM(i.balance), 2) as outstanding_ar
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_locations` l ON j.locationId = l.id
  WHERE j.businessUnitNormalized LIKE '%Production'
    AND i.balance >= 10
    AND DATE(i.createdOn) <= end_date
    -- Location filter would go here if needed
  GROUP BY j.businessUnitNormalized
),

-- ================================================================================
-- KPI 11: ESTIMATES SCHEDULED
-- This seems to be estimates that have appointments scheduled
-- ================================================================================
estimates_scheduled_kpi AS (
  SELECT
    j.businessUnitNormalized as business_unit,
    COUNT(DISTINCT e.id) as estimates_scheduled
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id
  JOIN `kpi-auto-471020.st_raw_v2.raw_appointments` a ON j.id = a.jobId
  WHERE j.businessUnitNormalized LIKE '%Sales'
    AND DATE(e.createdOn) BETWEEN start_date AND end_date
    AND a.scheduledStart IS NOT NULL
  GROUP BY j.businessUnitNormalized
)

-- ================================================================================
-- FINAL CONSOLIDATION
-- ================================================================================
SELECT
  COALESCE(l.business_unit, tb.business_unit, dp.business_unit) as business_unit,

  -- Sales KPIs
  l.lead_count_st_method as leads,
  ek.estimate_count as estimates,
  tb.total_booked,
  sr.close_rate_pct as success_rate,
  es.estimates_scheduled,

  -- Production KPIs
  dp.dollars_produced,
  g.gpm_weighted_method as gpm_pct,
  dc.dollars_collected,
  fb.future_bookings,
  w.warranty_pct,
  ar.outstanding_ar

FROM leads_kpi l
FULL OUTER JOIN total_booked_kpi tb ON l.business_unit = tb.business_unit
FULL OUTER JOIN estimates_kpi ek ON COALESCE(l.business_unit, tb.business_unit) = ek.business_unit
FULL OUTER JOIN success_rate_kpi sr ON COALESCE(l.business_unit, tb.business_unit) = sr.business_unit
FULL OUTER JOIN estimates_scheduled_kpi es ON COALESCE(l.business_unit, tb.business_unit) = es.business_unit
FULL OUTER JOIN dollars_produced_kpi dp
  ON REPLACE(COALESCE(l.business_unit, tb.business_unit), 'Sales', 'Production') = dp.business_unit
FULL OUTER JOIN gpm_kpi g
  ON COALESCE(dp.business_unit, REPLACE(l.business_unit, 'Sales', 'Production')) = g.business_unit
FULL OUTER JOIN dollars_collected_kpi dc
  ON COALESCE(dp.business_unit, g.business_unit) = dc.business_unit
FULL OUTER JOIN future_bookings_kpi fb
  ON COALESCE(dp.business_unit, g.business_unit) = fb.business_unit
FULL OUTER JOIN warranty_kpi w
  ON COALESCE(dp.business_unit, g.business_unit) = w.business_unit
FULL OUTER JOIN outstanding_ar_kpi ar
  ON COALESCE(dp.business_unit, g.business_unit) = ar.business_unit

ORDER BY business_unit;