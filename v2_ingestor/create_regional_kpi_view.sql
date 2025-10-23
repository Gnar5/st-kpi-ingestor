-- Regional KPI View: Combines Sales & Production BUs by Region
-- Aggregates daily_kpis into regional rollups (Phoenix, Tucson, Nevada, etc.)
-- Each region shows combined metrics from both Sales and Production business units

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.regional_kpis` AS

WITH

-- Map business units to regions
business_unit_regions AS (
  SELECT 'Phoenix-Sales' AS business_unit, 'Phoenix' AS region
  UNION ALL SELECT 'Phoenix-Production', 'Phoenix'
  UNION ALL SELECT 'Tucson-Sales', 'Tucson'
  UNION ALL SELECT 'Tucson-Production', 'Tucson'
  UNION ALL SELECT 'Nevada-Sales', 'Nevada'
  UNION ALL SELECT 'Nevada-Production', 'Nevada'
  UNION ALL SELECT "Andy's Painting Sales", 'Andys Painting'
  UNION ALL SELECT "Andy's Painting-Production", 'Andys Painting'
  UNION ALL SELECT 'Guaranteed Painting-Sales', 'Guaranteed Painting'
  UNION ALL SELECT 'Guaranteed Painting-Production', 'Guaranteed Painting'
  UNION ALL SELECT 'Commercial-AZ-Sales', 'Commercial AZ'
  UNION ALL SELECT 'Commercial-AZ-Production', 'Commercial AZ'
),

-- Aggregate KPIs by region and date
regional_aggregates AS (
  SELECT
    k.event_date,
    r.region,

    -- Sales KPIs (only from Sales BUs)
    SUM(CASE WHEN k.business_unit LIKE '%Sales' THEN k.lead_count ELSE 0 END) as lead_count,
    SUM(CASE WHEN k.business_unit LIKE '%Sales' THEN k.total_booked ELSE 0 END) as total_booked,
    SUM(CASE WHEN k.business_unit LIKE '%Sales' THEN k.num_estimates ELSE 0 END) as num_estimates,

    -- Production KPIs (only from Production BUs) - sum dollars, weighted avg for percentages
    SUM(CASE WHEN k.business_unit LIKE '%Production' THEN k.dollars_produced ELSE 0 END) as dollars_produced,

    -- Weighted average GPM (weight by dollars produced)
    SAFE_DIVIDE(
      SUM(CASE WHEN k.business_unit LIKE '%Production' THEN k.gpm_percent * k.dollars_produced ELSE 0 END),
      NULLIF(SUM(CASE WHEN k.business_unit LIKE '%Production' THEN k.dollars_produced ELSE 0 END), 0)
    ) as gpm_percent,

    -- Weighted average Warranty % (weight by dollars produced)
    SAFE_DIVIDE(
      SUM(CASE WHEN k.business_unit LIKE '%Production' THEN k.warranty_percent * k.dollars_produced ELSE 0 END),
      NULLIF(SUM(CASE WHEN k.business_unit LIKE '%Production' THEN k.dollars_produced ELSE 0 END), 0)
    ) as warranty_percent,

    -- Payment KPIs (combined from both)
    SUM(k.dollars_collected) as dollars_collected,
    SUM(k.outstanding_ar) as outstanding_ar,

    -- Future bookings (from Sales BUs)
    SUM(CASE WHEN k.business_unit LIKE '%Sales' THEN k.future_bookings ELSE 0 END) as future_bookings

  FROM `kpi-auto-471020.st_mart_v2.daily_kpis` k
  INNER JOIN business_unit_regions r ON k.business_unit = r.business_unit
  GROUP BY k.event_date, r.region
)

SELECT
  event_date,
  region,

  -- Sales Metrics
  lead_count,
  total_booked,
  num_estimates,
  SAFE_DIVIDE(num_estimates, NULLIF(lead_count, 0)) * 100 as close_rate_percent,

  -- Production Metrics
  dollars_produced,
  gpm_percent,
  warranty_percent,

  -- Payment Metrics
  dollars_collected,
  outstanding_ar,

  -- Future Bookings
  future_bookings,

  -- Metadata
  CURRENT_TIMESTAMP() as _created_at

FROM regional_aggregates
WHERE event_date >= '2020-01-01'
ORDER BY event_date DESC, region;


-- Sample queries to use with this view:

-- 1. Get all KPIs for Phoenix region for last 30 days
-- SELECT * FROM `kpi-auto-471020.st_mart_v2.regional_kpis`
-- WHERE region = 'Phoenix' AND event_date >= CURRENT_DATE('America/Phoenix') - 30
-- ORDER BY event_date DESC;

-- 2. Compare regions for a specific date
-- SELECT region, lead_count, total_booked, dollars_produced, gpm_percent
-- FROM `kpi-auto-471020.st_mart_v2.regional_kpis`
-- WHERE event_date = '2025-10-21'
-- ORDER BY total_booked DESC;

-- 3. Get monthly totals by region
-- SELECT
--   region,
--   DATE_TRUNC(event_date, MONTH) as month,
--   SUM(lead_count) as total_leads,
--   SUM(total_booked) as total_booked,
--   SUM(dollars_produced) as total_produced,
--   AVG(gpm_percent) as avg_gpm
-- FROM `kpi-auto-471020.st_mart_v2.regional_kpis`
-- WHERE event_date >= '2025-01-01'
-- GROUP BY region, month
-- ORDER BY month DESC, region;

-- 4. Get all regions for Looker dashboard (latest 90 days)
-- SELECT * FROM `kpi-auto-471020.st_mart_v2.regional_kpis`
-- WHERE event_date >= CURRENT_DATE('America/Phoenix') - 90
-- ORDER BY event_date DESC, region;
