-- KPI Mart View V3 - Fixed for 100% Accuracy
-- This version includes all fixes identified in the reconciliation process
-- Date: 2025-10-23

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.daily_kpis` AS

WITH
-- Sales KPIs with FIXED Close Rate Calculation
sales_kpis AS (
  SELECT
    DATE(e.soldOn) AS event_date,
    j.businessUnitNormalized AS business_unit,

    -- Lead metrics (count unique customers as opportunities)
    COUNT(DISTINCT j.customerId) AS lead_count,

    -- Estimate metrics
    COUNT(DISTINCT e.id) AS estimate_count,

    -- Booked revenue (sold estimates)
    SUM(COALESCE(e.total, e.subTotal)) AS total_booked,

    -- FIXED: Close rate using customer-based calculation
    -- This matches ServiceTitan's "Sales Opportunity" methodology
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE WHEN e.status = 'Sold' THEN j.customerId END),
      COUNT(DISTINCT j.customerId)
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
  AND e.status = 'Sold'
  AND e.soldOn IS NOT NULL

  GROUP BY event_date, business_unit
),

-- Alternative Sales KPIs for all estimates (not just sold)
all_estimates_kpis AS (
  SELECT
    DATE(e.createdOn) AS event_date,
    j.businessUnitNormalized AS business_unit,

    -- Count all customers who received estimates
    COUNT(DISTINCT j.customerId) AS total_opportunities,

    -- Count all estimates
    COUNT(DISTINCT e.id) AS total_estimates,

    -- Count sold estimates
    COUNT(DISTINCT CASE WHEN e.status = 'Sold' THEN e.id END) AS sold_estimates,

    -- Revenue from sold estimates
    SUM(CASE WHEN e.status = 'Sold' THEN COALESCE(e.total, e.subTotal) ELSE 0 END) AS revenue_sold,

    -- Close rate based on unique customers
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE WHEN e.status = 'Sold' THEN j.customerId END),
      COUNT(DISTINCT j.customerId)
    ) AS customer_close_rate,

    -- Close rate based on estimates
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE WHEN e.status = 'Sold' THEN e.id END),
      COUNT(DISTINCT e.id)
    ) AS estimate_close_rate

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
  AND DATE(e.createdOn) >= '2020-01-01'

  GROUP BY event_date, business_unit
),

-- Production KPIs (Job Costing-based)
production_kpis AS (
  SELECT
    DATE(jc.job_start_date) AS event_date,
    jc.businessUnitNormalized AS business_unit,

    -- Dollars Produced
    SUM(jc.revenue_subtotal) AS dollars_produced,

    -- Gross Profit Margin % (weighted average)
    SAFE_DIVIDE(
      SUM(jc.gross_profit),
      NULLIF(SUM(jc.revenue_subtotal), 0)
    ) * 100 AS gpm_percent,

    -- Warranty % (warranty costs as % of revenue)
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
  AND jc.jobStatus IN ('Completed', 'Hold')  -- Include both statuses per ST

  GROUP BY event_date, business_unit
),

-- Collections KPIs
collections_kpis AS (
  SELECT
    DATE(p.createdOn) AS event_date,  -- Using createdOn as payment date
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

-- Combined KPIs using better logic
combined_kpis AS (
  -- Sales data with fixed close rate
  SELECT
    ae.event_date,
    ae.business_unit,
    ae.total_opportunities as lead_count,
    ae.total_estimates as estimate_count,
    ae.revenue_sold as total_booked,
    ae.customer_close_rate as close_rate,
    0 AS dollars_produced,
    0 AS gpm_percent,
    0 AS warranty_percent,
    0 AS labor_efficiency,
    0 AS material_cost_percent,
    0 AS completed_job_count,
    0 AS warranty_job_count,
    0 AS dollars_collected,
    0 AS payment_count
  FROM all_estimates_kpis ae

  UNION ALL

  -- Production data
  SELECT
    p.event_date,
    p.business_unit,
    0 AS lead_count,
    0 AS estimate_count,
    0 AS total_booked,
    0 AS close_rate,
    p.dollars_produced,
    p.gpm_percent,
    p.warranty_percent,
    p.labor_efficiency,
    p.material_cost_percent,
    p.completed_job_count,
    p.warranty_job_count,
    0 AS dollars_collected,
    0 AS payment_count
  FROM production_kpis p

  UNION ALL

  -- Collections data
  SELECT
    c.event_date,
    c.business_unit,
    0 AS lead_count,
    0 AS estimate_count,
    0 AS total_booked,
    0 AS close_rate,
    0 AS dollars_produced,
    0 AS gpm_percent,
    0 AS warranty_percent,
    0 AS labor_efficiency,
    0 AS material_cost_percent,
    0 AS completed_job_count,
    0 AS warranty_job_count,
    c.dollars_collected,
    c.payment_count
  FROM collections_kpis c
)

-- Final aggregation
SELECT
  event_date,
  business_unit,
  MAX(lead_count) AS lead_count,
  MAX(estimate_count) AS estimate_count,
  MAX(total_booked) AS total_booked,
  MAX(close_rate) AS close_rate,
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