-- Regional KPI View (Fixed for Job Costing)
-- Combines Sales and Production BUs into regional rollups
-- Matches the new daily_kpis schema from job costing implementation

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
  UNION ALL SELECT "Andy's Painting-Sales", 'Andys Painting'
  UNION ALL SELECT "Andy's Painting-Production", 'Andys Painting'
  UNION ALL SELECT 'Commercial-AZ-Sales', 'Commercial Phoenix'
  UNION ALL SELECT 'Commercial-AZ-Production', 'Commercial Phoenix'
  UNION ALL SELECT 'Guaranteed Painting-Sales', 'College Station'
  UNION ALL SELECT 'Guaranteed Painting-Production', 'College Station'
),

-- Aggregate by region
regional_aggregates AS (
  SELECT
    k.event_date,
    r.region,

    -- Sales metrics (from Sales BUs only)
    SUM(CASE WHEN k.business_unit LIKE '%Sales' THEN k.lead_count ELSE 0 END) as lead_count,
    SUM(CASE WHEN k.business_unit LIKE '%Sales' THEN k.estimate_count ELSE 0 END) as estimate_count,
    SUM(CASE WHEN k.business_unit LIKE '%Sales' THEN k.total_booked ELSE 0 END) as total_booked,

    -- Close rate (weighted average by estimates)
    SAFE_DIVIDE(
      SUM(CASE WHEN k.business_unit LIKE '%Sales' THEN k.close_rate * k.estimate_count ELSE 0 END),
      NULLIF(SUM(CASE WHEN k.business_unit LIKE '%Sales' THEN k.estimate_count ELSE 0 END), 0)
    ) as close_rate,

    -- Production metrics (from Production BUs only)
    SUM(CASE WHEN k.business_unit LIKE '%Production' THEN k.dollars_produced ELSE 0 END) as dollars_produced,

    -- GPM % (weighted average by dollars produced)
    SAFE_DIVIDE(
      SUM(CASE WHEN k.business_unit LIKE '%Production' THEN k.gpm_percent * k.dollars_produced ELSE 0 END),
      NULLIF(SUM(CASE WHEN k.business_unit LIKE '%Production' THEN k.dollars_produced ELSE 0 END), 0)
    ) as gpm_percent,

    -- Warranty % (weighted average by dollars produced)
    SAFE_DIVIDE(
      SUM(CASE WHEN k.business_unit LIKE '%Production' THEN k.warranty_percent * k.dollars_produced ELSE 0 END),
      NULLIF(SUM(CASE WHEN k.business_unit LIKE '%Production' THEN k.dollars_produced ELSE 0 END), 0)
    ) as warranty_percent,

    -- Labor efficiency (weighted average by dollars produced)
    SAFE_DIVIDE(
      SUM(CASE WHEN k.business_unit LIKE '%Production' THEN k.labor_efficiency * k.dollars_produced ELSE 0 END),
      NULLIF(SUM(CASE WHEN k.business_unit LIKE '%Production' THEN k.dollars_produced ELSE 0 END), 0)
    ) as labor_efficiency,

    -- Job counts
    SUM(CASE WHEN k.business_unit LIKE '%Production' THEN k.completed_job_count ELSE 0 END) as completed_job_count,
    SUM(CASE WHEN k.business_unit LIKE '%Production' THEN k.warranty_job_count ELSE 0 END) as warranty_job_count,

    -- Calculate average sale amount
    SAFE_DIVIDE(
      SUM(CASE WHEN k.business_unit LIKE '%Sales' THEN k.total_booked ELSE 0 END),
      NULLIF(SUM(CASE WHEN k.business_unit LIKE '%Sales' AND k.total_booked > 0 THEN k.estimate_count ELSE 0 END), 0)
    ) as avg_sale_amount

  FROM `kpi-auto-471020.st_mart_v2.daily_kpis` k
  INNER JOIN business_unit_regions r ON k.business_unit = r.business_unit
  GROUP BY k.event_date, r.region
)

SELECT
  event_date,
  region,
  lead_count,
  estimate_count,
  ROUND(total_booked, 2) as total_booked,
  ROUND(close_rate * 100, 1) as close_rate_percent,
  ROUND(dollars_produced, 2) as dollars_produced,
  ROUND(gpm_percent, 1) as gpm_percent,
  ROUND(warranty_percent, 2) as warranty_percent,
  ROUND(labor_efficiency, 2) as labor_efficiency,
  completed_job_count,
  warranty_job_count,
  ROUND(avg_sale_amount, 2) as avg_sale_amount,
  CURRENT_TIMESTAMP() as created_at
FROM regional_aggregates
WHERE event_date >= '2020-01-01'
ORDER BY event_date DESC, region;