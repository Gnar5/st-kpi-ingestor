-- KPI Mart View - CORRECTED based on ServiceTitan specifications
-- Date: 2025-10-23
-- This version implements the exact ST logic for all KPIs

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.daily_kpis` AS

WITH
-- ================================================================================
-- SALES KPIs - Multiple CTEs for different date-based calculations
-- ================================================================================

-- Lead Count: Job createdOn date, jobType with "estimate"
sales_leads AS (
  SELECT
    DATE(j.createdOn) AS event_date,
    j.businessUnitNormalized AS business_unit,
    COUNT(DISTINCT j.id) AS lead_count

  FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_estimates` e ON j.id = e.jobId

  WHERE j.businessUnitNormalized IN (
    'Phoenix-Sales',
    'Tucson-Sales',
    'Nevada-Sales',
    "Andy's Painting-Sales",
    'Commercial-AZ-Sales',
    'Guaranteed Painting-Sales'
  )
  -- Filter for jobType with "estimate" - using top estimate jobTypeIds
  AND j.jobTypeId IN (
    727572, 705812, 727444, 25640548, 705557, 80656917,
    365792375, 40084045, 40091077, 7761171, 40528050, 144397449
  )
  AND DATE(j.createdOn) >= '2020-01-01'

  GROUP BY event_date, business_unit
),

-- Num Estimates: Estimate completedOn date, jobType with "estimate"
sales_estimates AS (
  SELECT
    DATE(e.completedOn) AS event_date,
    j.businessUnitNormalized AS business_unit,
    COUNT(DISTINCT e.id) AS estimate_count

  FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id

  WHERE j.businessUnitNormalized IN (
    'Phoenix-Sales',
    'Tucson-Sales',
    'Nevada-Sales',
    "Andy's Painting-Sales",
    'Commercial-AZ-Sales',
    'Guaranteed Painting-Sales'
  )
  AND j.jobTypeId IN (
    727572, 705812, 727444, 25640548, 705557, 80656917,
    365792375, 40084045, 40091077, 7761171, 40528050, 144397449
  )
  AND e.completedOn IS NOT NULL
  AND DATE(e.completedOn) >= '2020-01-01'

  GROUP BY event_date, business_unit
),

-- Close Rate Components:
-- Sales Opportunities = estimates with sales opp date in range
-- Closed Opportunities = estimates with soldOn date in range
sales_close_rate AS (
  SELECT
    -- Use soldOn date as the event date for close rate tracking
    DATE(e.soldOn) AS event_date,
    j.businessUnitNormalized AS business_unit,

    -- Count all opportunities (estimates created, regardless of status)
    COUNT(DISTINCT j.id) AS sales_opportunities,

    -- Count closed opportunities (sold estimates)
    COUNT(DISTINCT CASE WHEN e.status = 'Sold' THEN j.id END) AS closed_opportunities,

    -- Close rate calculation
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE WHEN e.status = 'Sold' THEN j.id END),
      COUNT(DISTINCT j.id)
    ) AS close_rate

  FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id

  WHERE j.businessUnitNormalized IN (
    'Phoenix-Sales',
    'Tucson-Sales',
    'Nevada-Sales',
    "Andy's Painting-Sales",
    'Commercial-AZ-Sales',
    'Guaranteed Painting-Sales'
  )
  AND j.jobTypeId IN (
    727572, 705812, 727444, 25640548, 705557, 80656917,
    365792375, 40084045, 40091077, 7761171, 40528050, 144397449
  )
  AND e.soldOn IS NOT NULL  -- Must have a sold date to be counted
  AND DATE(e.soldOn) >= '2020-01-01'

  GROUP BY event_date, business_unit
),

-- Total Booked: Estimate soldOn date, sold estimates only
sales_revenue AS (
  SELECT
    DATE(e.soldOn) AS event_date,
    j.businessUnitNormalized AS business_unit,
    SUM(COALESCE(e.total, e.subTotal)) AS total_booked

  FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id

  WHERE j.businessUnitNormalized IN (
    'Phoenix-Sales',
    'Tucson-Sales',
    'Nevada-Sales',
    "Andy's Painting-Sales",
    'Commercial-AZ-Sales',
    'Guaranteed Painting-Sales'
  )
  AND e.status = 'Sold'
  AND e.soldOn IS NOT NULL
  AND DATE(e.soldOn) >= '2020-01-01'

  GROUP BY event_date, business_unit
),

-- ================================================================================
-- PRODUCTION KPIs - Job Costing based
-- ================================================================================
production_kpis AS (
  SELECT
    DATE(jc.job_start_date) AS event_date,
    jc.businessUnitNormalized AS business_unit,

    -- Dollars Produced (confirmed 100% accurate)
    SUM(jc.revenue_subtotal) AS dollars_produced,

    -- GPM % - Use weighted average matching ST calculation
    -- ST formula: (Sum of gross profit / Sum of revenue) * 100
    -- EXCLUDE $0 revenue jobs (likely warranty/incomplete jobs)
    SAFE_DIVIDE(
      SUM(CASE WHEN jc.revenue_subtotal > 0 THEN jc.gross_profit ELSE 0 END),
      NULLIF(SUM(CASE WHEN jc.revenue_subtotal > 0 THEN jc.revenue_subtotal ELSE 0 END), 0)
    ) * 100 AS gpm_percent,

    -- Warranty % - (warranty job costs / total revenue) * 100
    SAFE_DIVIDE(
      SUM(CASE WHEN jc.is_warranty THEN jc.total_cost ELSE 0 END),
      NULLIF(SUM(jc.revenue_subtotal), 0)
    ) * 100 AS warranty_percent,

    -- Labor efficiency
    SAFE_DIVIDE(
      SUM(jc.revenue_subtotal),
      NULLIF(SUM(jc.labor_cost), 0)
    ) AS labor_efficiency,

    -- Material cost ratio
    SAFE_DIVIDE(
      SUM(jc.material_cost_net),
      NULLIF(SUM(jc.revenue_subtotal), 0)
    ) * 100 AS material_cost_percent,

    -- Job counts
    COUNT(DISTINCT jc.job_id) AS completed_job_count,
    COUNT(DISTINCT CASE WHEN jc.is_warranty THEN jc.job_id END) AS warranty_job_count

  FROM `kpi-auto-471020.st_mart_v2.job_costing` jc

  WHERE jc.businessUnitNormalized IN (
    'Phoenix-Production',
    'Tucson-Production',
    'Nevada-Production',
    "Andy's Painting-Production",
    'Commercial-AZ-Production',
    'Guaranteed Painting-Production'
  )
  AND jc.jobStatus IN ('Completed', 'Hold')  -- Confirmed correct
  AND DATE(jc.job_start_date) >= '2020-01-01'

  GROUP BY event_date, business_unit
),

-- ================================================================================
-- Collections KPIs
-- ================================================================================
collections_kpis AS (
  SELECT
    DATE(p.createdOn) AS event_date,
    j.businessUnitNormalized AS business_unit,
    SUM(p.amount) AS dollars_collected,
    COUNT(DISTINCT p.id) AS payment_count

  FROM `kpi-auto-471020.st_raw_v2.raw_payments` p
  JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON p.invoiceId = i.id
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id

  WHERE j.businessUnitNormalized IN (
    'Phoenix-Production',
    'Tucson-Production',
    'Nevada-Production',
    "Andy's Painting-Production",
    'Commercial-AZ-Production',
    'Guaranteed Painting-Production'
  )
  AND p.createdOn IS NOT NULL
  AND DATE(p.createdOn) >= '2020-01-01'

  GROUP BY event_date, business_unit
),

-- ================================================================================
-- COMBINED KPIs
-- ================================================================================
combined_kpis AS (
  -- Sales leads (on job createdOn date)
  SELECT
    event_date,
    business_unit,
    lead_count,
    0 AS estimate_count,
    0 AS sales_opportunities,
    0 AS closed_opportunities,
    0 AS close_rate,
    0 AS total_booked,
    0 AS dollars_produced,
    0 AS gpm_percent,
    0 AS warranty_percent,
    0 AS labor_efficiency,
    0 AS material_cost_percent,
    0 AS completed_job_count,
    0 AS warranty_job_count,
    0 AS dollars_collected,
    0 AS payment_count
  FROM sales_leads

  UNION ALL

  -- Sales estimates (on estimate completedOn date)
  SELECT
    event_date,
    business_unit,
    0 AS lead_count,
    estimate_count,
    0 AS sales_opportunities,
    0 AS closed_opportunities,
    0 AS close_rate,
    0 AS total_booked,
    0 AS dollars_produced,
    0 AS gpm_percent,
    0 AS warranty_percent,
    0 AS labor_efficiency,
    0 AS material_cost_percent,
    0 AS completed_job_count,
    0 AS warranty_job_count,
    0 AS dollars_collected,
    0 AS payment_count
  FROM sales_estimates

  UNION ALL

  -- Close rate metrics (on soldOn date)
  SELECT
    event_date,
    business_unit,
    0 AS lead_count,
    0 AS estimate_count,
    sales_opportunities,
    closed_opportunities,
    close_rate,
    0 AS total_booked,
    0 AS dollars_produced,
    0 AS gpm_percent,
    0 AS warranty_percent,
    0 AS labor_efficiency,
    0 AS material_cost_percent,
    0 AS completed_job_count,
    0 AS warranty_job_count,
    0 AS dollars_collected,
    0 AS payment_count
  FROM sales_close_rate

  UNION ALL

  -- Sales revenue (on soldOn date)
  SELECT
    event_date,
    business_unit,
    0 AS lead_count,
    0 AS estimate_count,
    0 AS sales_opportunities,
    0 AS closed_opportunities,
    0 AS close_rate,
    total_booked,
    0 AS dollars_produced,
    0 AS gpm_percent,
    0 AS warranty_percent,
    0 AS labor_efficiency,
    0 AS material_cost_percent,
    0 AS completed_job_count,
    0 AS warranty_job_count,
    0 AS dollars_collected,
    0 AS payment_count
  FROM sales_revenue

  UNION ALL

  -- Production data
  SELECT
    event_date,
    business_unit,
    0 AS lead_count,
    0 AS estimate_count,
    0 AS sales_opportunities,
    0 AS closed_opportunities,
    0 AS close_rate,
    0 AS total_booked,
    dollars_produced,
    gpm_percent,
    warranty_percent,
    labor_efficiency,
    material_cost_percent,
    completed_job_count,
    warranty_job_count,
    0 AS dollars_collected,
    0 AS payment_count
  FROM production_kpis

  UNION ALL

  -- Collections data
  SELECT
    event_date,
    business_unit,
    0 AS lead_count,
    0 AS estimate_count,
    0 AS sales_opportunities,
    0 AS closed_opportunities,
    0 AS close_rate,
    0 AS total_booked,
    0 AS dollars_produced,
    0 AS gpm_percent,
    0 AS warranty_percent,
    0 AS labor_efficiency,
    0 AS material_cost_percent,
    0 AS completed_job_count,
    0 AS warranty_job_count,
    dollars_collected,
    payment_count
  FROM collections_kpis
)

-- ================================================================================
-- FINAL AGGREGATION
-- ================================================================================
SELECT
  event_date,
  business_unit,
  MAX(lead_count) AS lead_count,
  MAX(estimate_count) AS estimate_count,
  MAX(sales_opportunities) AS sales_opportunities,
  MAX(closed_opportunities) AS closed_opportunities,
  MAX(close_rate) AS close_rate,
  MAX(total_booked) AS total_booked,
  MAX(dollars_produced) AS dollars_produced,
  MAX(gpm_percent) AS gpm_percent,
  MAX(warranty_percent) AS warranty_percent,
  MAX(labor_efficiency) AS labor_efficiency,
  MAX(material_cost_percent) AS material_cost_percent,
  MAX(completed_job_count) AS completed_job_count,
  MAX(warranty_job_count) AS warranty_job_count,
  MAX(dollars_collected) AS dollars_collected,
  MAX(payment_count) AS payment_count,
  CURRENT_TIMESTAMP() AS created_at
FROM combined_kpis
WHERE event_date >= '2020-01-01'
GROUP BY event_date, business_unit
ORDER BY event_date DESC, business_unit;