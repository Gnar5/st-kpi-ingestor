-- KPI Mart: All 10 KPIs by Business Unit and Date
-- This table can be materialized daily or queried on-demand

CREATE OR REPLACE TABLE `kpi-auto-471020.st_mart_v2.daily_kpis` AS

WITH 

-- Define excluded job types
excluded_types AS (
  SELECT jobTypeName FROM UNNEST([
    'PM Inspection',
    'Safety Inspection',
    'Window/Solar Washing'
  ]) AS jobTypeName
),

-- Define warranty types
warranty_types AS (
  SELECT jobTypeName FROM UNNEST([
    'Warranty',
    'Touchup'
  ]) AS jobTypeName
),

-- Sales business units
sales_units AS (
  SELECT businessUnit FROM UNNEST([
    'Andy\'s Painting Sales',
    'Commercial-AZ-Sales',
    'Guaranteed Painting-Sales',
    'Nevada-Sales',
    'Phoenix-Sales',
    'Tucson-Sales'
  ]) AS businessUnit
),

-- Production business units
production_units AS (
  SELECT businessUnit FROM UNNEST([
    'Andy\'s Painting-Production',
    'Commercial-AZ-Production',
    'Guaranteed Painting-Production',
    'Nevada-Production',
    'Phoenix-Production',
    'Tucson-Production'
  ]) AS businessUnit
),

-- ============================================================================
-- KPI 1: Leads (by job creation date)
-- ============================================================================
-- DEFINITION: Count of unique customers with estimate jobs created on event_date
-- LOGIC CHANGE (per senior eng directive):
--   - Case-insensitive substring match on 'estimate' in jobTypeName
--   - NO COMM exclusion (previously excluded COMM jobs, now includes all)
--   - Exclude customers with 'test' in name (case-insensitive)
--   - Sales BUs only
-- ============================================================================
leads AS (
  SELECT
    DATE(j.createdOn) as event_date,
    j.businessUnitNormalized as business_unit,
    COUNT(DISTINCT j.customerId) as lead_count
  FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
  INNER JOIN sales_units s ON j.businessUnitNormalized = s.businessUnit
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_customers` c ON j.customerId = c.id
  WHERE LOWER(j.jobTypeName) LIKE '%estimate%'
    AND (c.name IS NULL OR LOWER(c.name) NOT LIKE '%test%')
    AND j.createdOn >= '2020-01-01'
  GROUP BY event_date, business_unit
),

-- ============================================================================
-- KPI 2: Total Booked (sum of sold estimate amounts by sold date)
-- ============================================================================
-- DEFINITION: Sum of ALL sold estimates on the date they were sold (Phoenix/Arizona timezone)
-- LOGIC CHANGE (per diagnostics analysis + user validation):
--   - Use soldOn date (not createdOn) - matches ST UI perfectly
--   - CRITICAL: Convert to America/Phoenix timezone before DATE() to match ST UI
--   - NO job type filter - includes ALL sold estimates (residential + commercial)
--   - Status must be 'Sold'
--   - Sales BUs only
-- RATIONALE:
--   1. User confirmed Nevada shows $27,150 with NO job type filter in ST UI
--   2. Timezone issue found: estimate 386522888 sold 6:02 PM Arizona time on Aug 18
--      but UTC timestamp shows Aug 19. ST UI uses local timezone.
-- VALIDATION:
--   Phoenix 8/18=$30,241.51 (exact match with timezone fix)
--   Tucson 8/18=$4,844.58 (exact match with timezone fix)
--   Nevada 8/18=$27,150.00 (exact match with timezone fix)
-- ============================================================================
total_booked AS (
  SELECT
    DATE(e.soldOn, 'America/Phoenix') as event_date,
    j.businessUnitNormalized as business_unit,
    SUM(e.subtotal) as total_booked
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id
  INNER JOIN sales_units s ON j.businessUnitNormalized = s.businessUnit
  WHERE e.soldOn >= '2020-01-01'
    AND e.status = 'Sold'
  GROUP BY event_date, business_unit
),

-- KPI 3: $ Produced (from invoices by completion date)
dollars_produced AS (
  SELECT
    DATE(i.invoiceDate) as event_date,
    j.businessUnitNormalized as business_unit,
    SUM(i.total) as dollars_produced
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
  INNER JOIN production_units p ON j.businessUnitNormalized = p.businessUnit
  LEFT JOIN excluded_types ex ON j.jobTypeName = ex.jobTypeName
  WHERE ex.jobTypeName IS NULL
    AND DATE(i.invoiceDate) >= '2020-01-01'
  GROUP BY event_date, business_unit
),

-- KPI 4: G.P.M (Gross Profit Margin from job costing)
gpm AS (
  SELECT
    DATE(i.invoiceDate) as event_date,
    j.businessUnitNormalized as business_unit,
    SUM(i.total) as revenue,
    SUM(COALESCE(jc.total_job_cost, 0)) as total_cost,
    CASE 
      WHEN SUM(i.total) > 0 THEN
        ((SUM(i.total) - SUM(COALESCE(jc.total_job_cost, 0))) / SUM(i.total)) * 100
      ELSE NULL
    END as gpm_percent
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
  LEFT JOIN `kpi-auto-471020.st_mart_v2.fact_job_costing` jc ON i.jobId = jc.jobId
  INNER JOIN production_units p ON j.businessUnitNormalized = p.businessUnit
  LEFT JOIN excluded_types ex ON j.jobTypeName = ex.jobTypeName
  WHERE ex.jobTypeName IS NULL
    AND DATE(i.invoiceDate) >= '2020-01-01'
  GROUP BY event_date, business_unit
),

-- KPI 5: $ Collected (payments)
dollars_collected AS (
  SELECT
    DATE(p.createdOn) as event_date,
    j.businessUnitNormalized as business_unit,
    SUM(p.amount) as dollars_collected
  FROM `kpi-auto-471020.st_raw_v2.raw_payments` p
  JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON p.invoiceId = i.id
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
  INNER JOIN production_units pu ON j.businessUnitNormalized = pu.businessUnit
  WHERE p.createdOn >= '2020-01-01'
  GROUP BY event_date, business_unit
),

-- KPI 6: # Estimates (completed)
num_estimates AS (
  SELECT
    DATE(j.completedOn) as event_date,
    j.businessUnitNormalized as business_unit,
    COUNT(*) as completed_estimates
  FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
  INNER JOIN sales_units s ON j.businessUnitNormalized = s.businessUnit
  WHERE j.completedOn >= '2020-01-01'
    AND j.jobStatus = 'Completed'
    AND LOWER(j.jobTypeName) LIKE '%estimate%'
  GROUP BY event_date, business_unit
),

-- KPI 7: Success Rate / Close Rate
close_rate AS (
  SELECT
    DATE(e.createdOn) as event_date,
    j.businessUnitNormalized as business_unit,
    AVG(CASE WHEN e.status = 'Sold' THEN 1.0 ELSE 0.0 END) * 100 as close_rate_percent
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON e.jobId = j.id
  INNER JOIN sales_units s ON j.businessUnitNormalized = s.businessUnit
  WHERE e.createdOn >= '2020-01-01'
    AND LOWER(j.jobTypeName) LIKE '%estimate%'
  GROUP BY event_date, business_unit
),

-- KPI 8: Future Bookings (jobs scheduled for future)
future_bookings AS (
  SELECT
    CURRENT_DATE('America/Phoenix') as event_date,
    j.businessUnitNormalized as business_unit,
    SUM(i.total) as future_bookings
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
  INNER JOIN production_units p ON j.businessUnitNormalized = p.businessUnit
  LEFT JOIN excluded_types ex ON j.jobTypeName = ex.jobTypeName
  WHERE ex.jobTypeName IS NULL
    AND DATE(i.invoiceDate) > CURRENT_DATE('America/Phoenix')
    AND DATE(i.invoiceDate) <= DATE_ADD(CURRENT_DATE('America/Phoenix'), INTERVAL 1 YEAR)
  GROUP BY business_unit
),

-- KPI 9: Warranty % (warranty costs / produced)
warranty_pct AS (
  SELECT
    DATE(i.invoiceDate) as event_date,
    j.businessUnitNormalized as business_unit,
    SUM(CASE WHEN wt.jobTypeName IS NOT NULL THEN COALESCE(jc.total_job_cost, 0) ELSE 0 END) as warranty_cost,
    SUM(i.total) as total_produced
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
  LEFT JOIN `kpi-auto-471020.st_mart_v2.fact_job_costing` jc ON i.jobId = jc.jobId
  LEFT JOIN warranty_types wt ON j.jobTypeName = wt.jobTypeName
  INNER JOIN production_units p ON j.businessUnitNormalized = p.businessUnit
  WHERE i.invoiceDate >= '2020-01-01'
  GROUP BY event_date, business_unit
),

-- KPI 10: Outstanding A/R
outstanding_ar AS (
  SELECT
    CURRENT_DATE('America/Phoenix') as event_date,
    j.businessUnitNormalized as business_unit,
    SUM(i.balance) as outstanding_ar
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
  JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
  INNER JOIN production_units p ON j.businessUnitNormalized = p.businessUnit
  WHERE i.balance >= 10
  GROUP BY business_unit
),

-- Combine all date-based KPIs
all_dates AS (
  SELECT DISTINCT event_date, business_unit FROM leads
  UNION DISTINCT
  SELECT DISTINCT event_date, business_unit FROM total_booked
  UNION DISTINCT
  SELECT DISTINCT event_date, business_unit FROM dollars_produced
  UNION DISTINCT
  SELECT DISTINCT event_date, business_unit FROM gpm
  UNION DISTINCT
  SELECT DISTINCT event_date, business_unit FROM dollars_collected
  UNION DISTINCT
  SELECT DISTINCT event_date, business_unit FROM num_estimates
  UNION DISTINCT
  SELECT DISTINCT event_date, business_unit FROM close_rate
  UNION DISTINCT
  SELECT DISTINCT event_date, business_unit FROM warranty_pct
)

-- Final SELECT joining all KPIs
SELECT
  d.event_date,
  d.business_unit,
  COALESCE(l.lead_count, 0) as lead_count,
  COALESCE(tb.total_booked, 0) as total_booked,
  COALESCE(dp.dollars_produced, 0) as dollars_produced,
  COALESCE(g.gpm_percent, 0) as gpm_percent,
  COALESCE(dc.dollars_collected, 0) as dollars_collected,
  COALESCE(ne.completed_estimates, 0) as num_estimates,
  COALESCE(cr.close_rate_percent, 0) as close_rate_percent,
  COALESCE(fb.future_bookings, 0) as future_bookings,
  CASE
    WHEN wp.total_produced > 0 THEN (wp.warranty_cost / wp.total_produced) * 100
    ELSE 0
  END as warranty_percent,
  COALESCE(ar.outstanding_ar, 0) as outstanding_ar
FROM all_dates d
LEFT JOIN leads l ON d.event_date = l.event_date AND d.business_unit = l.business_unit
LEFT JOIN total_booked tb ON d.event_date = tb.event_date AND d.business_unit = tb.business_unit
LEFT JOIN dollars_produced dp ON d.event_date = dp.event_date AND d.business_unit = dp.business_unit
LEFT JOIN gpm g ON d.event_date = g.event_date AND d.business_unit = g.business_unit
LEFT JOIN dollars_collected dc ON d.event_date = dc.event_date AND d.business_unit = dc.business_unit
LEFT JOIN num_estimates ne ON d.event_date = ne.event_date AND d.business_unit = ne.business_unit
LEFT JOIN close_rate cr ON d.event_date = cr.event_date AND d.business_unit = cr.business_unit
LEFT JOIN future_bookings fb ON d.business_unit = fb.business_unit
LEFT JOIN warranty_pct wp ON d.event_date = wp.event_date AND d.business_unit = wp.business_unit
LEFT JOIN outstanding_ar ar ON d.business_unit = ar.business_unit
ORDER BY d.event_date DESC, d.business_unit;
