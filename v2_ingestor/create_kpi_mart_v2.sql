-- KPI Mart View with Job Costing-based Dollars Produced
-- This replaces the old invoice-based calculation with accurate job costing data
-- Matches ServiceTitan FOREMAN Job Cost report methodology

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.daily_kpis` AS

WITH
-- Sales KPIs (Lead Generation and Booking)
-- Note: ServiceTitan's "BU Sales - API" report uses estimate soldOn date, not job createdOn
sales_kpis AS (
  SELECT
    DATE(e.soldOn) AS event_date,
    j.businessUnitNormalized AS business_unit,

    -- Lead metrics (count jobs as sales opportunities)
    COUNT(DISTINCT j.id) AS lead_count,

    -- Estimate metrics
    COUNT(DISTINCT e.id) AS estimate_count,

    -- Booked revenue (sold estimates - use subTotal since total is often NULL)
    SUM(COALESCE(e.total, e.subTotal)) AS total_booked,

    -- Close rate (based on customers with sold estimates)
    SAFE_DIVIDE(
      COUNT(DISTINCT e.customerId),
      COUNT(DISTINCT e.customerId)
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

-- Production KPIs (Job Costing-based)
production_kpis AS (
  SELECT
    DATE(jc.job_start_date) AS event_date,  -- Using job start date (scheduled date)
    jc.businessUnitNormalized AS business_unit,

    -- Dollars Produced (revenue from completed jobs)
    SUM(jc.revenue_subtotal) AS dollars_produced,

    -- Gross Profit Margin %
    SAFE_DIVIDE(
      SUM(jc.gross_profit),
      NULLIF(SUM(jc.revenue_subtotal), 0)
    ) * 100 AS gpm_percent,

    -- Warranty % (warranty job costs / total produced)
    SAFE_DIVIDE(
      SUM(CASE WHEN jc.is_warranty THEN jc.total_cost ELSE 0 END),
      NULLIF(SUM(jc.revenue_subtotal), 0)
    ) * 100 AS warranty_percent,

    -- Labor efficiency (revenue per labor dollar)
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
  AND jc.jobStatus IN ('Completed', 'Hold')  -- ServiceTitan includes both Completed and Hold status jobs

  GROUP BY event_date, business_unit
),

-- Combined KPIs (union sales and production)
combined_kpis AS (
  -- Sales data
  SELECT
    event_date,
    business_unit,
    lead_count,
    estimate_count,
    total_booked,
    close_rate,
    0 AS dollars_produced,
    0 AS gpm_percent,
    0 AS warranty_percent,
    0 AS labor_efficiency,
    0 AS material_cost_percent,
    0 AS completed_job_count,
    0 AS warranty_job_count
  FROM sales_kpis

  UNION ALL

  -- Production data
  SELECT
    event_date,
    business_unit,
    0 AS lead_count,
    0 AS estimate_count,
    0 AS total_booked,
    0 AS close_rate,
    dollars_produced,
    gpm_percent,
    warranty_percent,
    labor_efficiency,
    material_cost_percent,
    completed_job_count,
    warranty_job_count
  FROM production_kpis
)

-- Final aggregation (in case there's overlap)
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
  CURRENT_TIMESTAMP() AS created_at
FROM combined_kpis
WHERE event_date >= '2020-01-01'
GROUP BY event_date, business_unit
ORDER BY event_date DESC, business_unit;