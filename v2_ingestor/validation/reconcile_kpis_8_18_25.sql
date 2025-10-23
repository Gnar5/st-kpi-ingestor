-- ================================================================================
-- KPI Reconciliation Query: ServiceTitan vs BigQuery
-- Date Range: August 18-24, 2025
-- Purpose: Compare all KPIs against ST export with variance calculation
-- ================================================================================

DECLARE start_date DATE DEFAULT '2025-08-18';
DECLARE end_date DATE DEFAULT '2025-08-24';

WITH
-- ================================================================================
-- SERVICETITAN EXPECTED VALUES (from ST RE-RUN 8.18.25 export)
-- ================================================================================
st_expected AS (
  SELECT 'Tucson' as region, 39 as lead_count, 46 as num_estimates, 51.22 as close_rate_percent,
         89990.11 as total_booked, 83761.16 as dollars_produced, 48.00 as gpm_percent, 0.38 as warranty_percent
  UNION ALL
  SELECT 'Phoenix', 96, 85, 39.74, 116551.26, 232891.98, 50.83, 1.26
  UNION ALL
  SELECT 'Nevada', 28, 22, 60.87, 105890.00, 23975.00, 24.04, 10.46
  UNION ALL
  SELECT 'Andys Painting', 25, 24, 35.71, 30896.91, 53752.56, 47.83, 1.42
  UNION ALL
  SELECT 'Commercial AZ', 22, 24, 26.92, 119803.60, 77345.25, 46.98, 0.00
  UNION ALL
  SELECT 'Guaranteed Painting', 8, 7, 77.78, 26067.40, 30472.30, 45.84, 0.00
),

-- ================================================================================
-- BIGQUERY ACTUAL VALUES (from daily_kpis mart)
-- ================================================================================
bq_regional_agg AS (
  SELECT
    CASE
      WHEN business_unit LIKE 'Tucson%' THEN 'Tucson'
      WHEN business_unit LIKE 'Phoenix%' THEN 'Phoenix'
      WHEN business_unit LIKE 'Nevada%' THEN 'Nevada'
      WHEN business_unit LIKE 'Andy%' THEN 'Andys Painting'
      WHEN business_unit LIKE 'Commercial%' THEN 'Commercial AZ'
      WHEN business_unit LIKE 'Guaranteed%' THEN 'Guaranteed Painting'
    END as region,

    -- Sales metrics (from Sales BUs only)
    SUM(CASE WHEN business_unit LIKE '%Sales' THEN lead_count ELSE 0 END) as lead_count,
    SUM(CASE WHEN business_unit LIKE '%Sales' THEN estimate_count ELSE 0 END) as num_estimates,
    AVG(CASE WHEN business_unit LIKE '%Sales' AND close_rate > 0 THEN close_rate ELSE NULL END) * 100 as close_rate_percent,
    SUM(CASE WHEN business_unit LIKE '%Sales' THEN total_booked ELSE 0 END) as total_booked,

    -- Production metrics (from Production BUs only)
    SUM(CASE WHEN business_unit LIKE '%Production' THEN dollars_produced ELSE 0 END) as dollars_produced,
    AVG(CASE WHEN business_unit LIKE '%Production' AND gpm_percent > 0 THEN gpm_percent ELSE NULL END) as gpm_percent,
    AVG(CASE WHEN business_unit LIKE '%Production' THEN warranty_percent ELSE NULL END) as warranty_percent

  FROM `kpi-auto-471020.st_mart_v2.daily_kpis`
  WHERE event_date BETWEEN start_date AND end_date
  GROUP BY region
),

-- ================================================================================
-- VARIANCE CALCULATION
-- ================================================================================
variance_calc AS (
  SELECT
    st.region,

    -- Lead Count
    st.lead_count as st_lead_count,
    bq.lead_count as bq_lead_count,
    bq.lead_count - st.lead_count as lead_count_delta,
    ROUND(SAFE_DIVIDE(bq.lead_count - st.lead_count, st.lead_count) * 100, 2) as lead_count_pct,
    ROUND(SAFE_DIVIDE(bq.lead_count, st.lead_count) * 100, 2) as lead_count_accuracy,

    -- Num Estimates
    st.num_estimates as st_num_estimates,
    bq.num_estimates as bq_num_estimates,
    bq.num_estimates - st.num_estimates as estimates_delta,
    ROUND(SAFE_DIVIDE(bq.num_estimates - st.num_estimates, st.num_estimates) * 100, 2) as estimates_pct,
    ROUND(SAFE_DIVIDE(bq.num_estimates, st.num_estimates) * 100, 2) as estimates_accuracy,

    -- Close Rate
    st.close_rate_percent as st_close_rate,
    ROUND(bq.close_rate_percent, 2) as bq_close_rate,
    ROUND(bq.close_rate_percent - st.close_rate_percent, 2) as close_rate_delta,
    ROUND(SAFE_DIVIDE(bq.close_rate_percent - st.close_rate_percent, st.close_rate_percent) * 100, 2) as close_rate_pct,

    -- Total Booked
    ROUND(st.total_booked, 2) as st_total_booked,
    ROUND(bq.total_booked, 2) as bq_total_booked,
    ROUND(bq.total_booked - st.total_booked, 2) as booked_delta,
    ROUND(SAFE_DIVIDE(bq.total_booked - st.total_booked, st.total_booked) * 100, 4) as booked_pct,

    -- Dollars Produced
    ROUND(st.dollars_produced, 2) as st_dollars_produced,
    ROUND(bq.dollars_produced, 2) as bq_dollars_produced,
    ROUND(bq.dollars_produced - st.dollars_produced, 2) as produced_delta,
    ROUND(SAFE_DIVIDE(bq.dollars_produced - st.dollars_produced, st.dollars_produced) * 100, 4) as produced_pct,

    -- GPM %
    st.gpm_percent as st_gpm,
    ROUND(bq.gpm_percent, 2) as bq_gpm,
    ROUND(bq.gpm_percent - st.gpm_percent, 2) as gpm_delta,
    ROUND(SAFE_DIVIDE(bq.gpm_percent - st.gpm_percent, st.gpm_percent) * 100, 2) as gpm_pct,

    -- Warranty %
    st.warranty_percent as st_warranty,
    ROUND(bq.warranty_percent, 2) as bq_warranty,
    ROUND(bq.warranty_percent - st.warranty_percent, 2) as warranty_delta,
    ROUND(SAFE_DIVIDE(bq.warranty_percent - st.warranty_percent, NULLIF(st.warranty_percent, 0)) * 100, 2) as warranty_pct

  FROM st_expected st
  LEFT JOIN bq_regional_agg bq ON st.region = bq.region
)

-- ================================================================================
-- FINAL OUTPUT: Variance Report
-- ================================================================================
SELECT
  region,

  -- Lead Count
  st_lead_count,
  bq_lead_count,
  lead_count_delta,
  CONCAT(CAST(lead_count_pct AS STRING), '%') as lead_count_variance_pct,
  CONCAT(CAST(lead_count_accuracy AS STRING), '%') as lead_count_accuracy,
  CASE
    WHEN ABS(lead_count_delta) <= 1 THEN '✅ EXACT'
    WHEN ABS(lead_count_delta) <= 5 THEN '⚠️ MINOR'
    ELSE '🔴 MAJOR'
  END as lead_status,

  -- Num Estimates
  st_num_estimates,
  bq_num_estimates,
  estimates_delta,
  CONCAT(CAST(estimates_pct AS STRING), '%') as estimates_variance_pct,
  CONCAT(CAST(estimates_accuracy AS STRING), '%') as estimates_accuracy,
  CASE
    WHEN ABS(estimates_delta) <= 1 THEN '✅ EXACT'
    WHEN ABS(estimates_delta) <= 5 THEN '⚠️ MINOR'
    ELSE '🔴 MAJOR'
  END as estimates_status,

  -- Close Rate
  st_close_rate,
  bq_close_rate,
  close_rate_delta,
  CONCAT(CAST(close_rate_pct AS STRING), '%') as close_rate_variance_pct,
  CASE
    WHEN ABS(close_rate_delta) <= 1 THEN '✅ EXACT'
    WHEN ABS(close_rate_delta) <= 5 THEN '⚠️ MINOR'
    ELSE '🔴 MAJOR'
  END as close_rate_status,

  -- Total Booked
  st_total_booked,
  bq_total_booked,
  booked_delta,
  CONCAT(CAST(booked_pct AS STRING), '%') as booked_variance_pct,
  CASE
    WHEN ABS(booked_delta) < 0.01 THEN '✅ EXACT'
    WHEN ABS(booked_delta) < 100 THEN '⚠️ MINOR'
    ELSE '🔴 MAJOR'
  END as booked_status,

  -- Dollars Produced
  st_dollars_produced,
  bq_dollars_produced,
  produced_delta,
  CONCAT(CAST(produced_pct AS STRING), '%') as produced_variance_pct,
  CASE
    WHEN ABS(produced_delta) < 0.01 THEN '✅ EXACT'
    WHEN ABS(produced_delta) < 100 THEN '⚠️ MINOR'
    ELSE '🔴 MAJOR'
  END as produced_status,

  -- GPM %
  st_gpm,
  bq_gpm,
  gpm_delta,
  CONCAT(CAST(gpm_pct AS STRING), '%') as gpm_variance_pct,
  CASE
    WHEN ABS(gpm_delta) < 1 THEN '✅ EXACT'
    WHEN ABS(gpm_delta) < 5 THEN '⚠️ MINOR'
    ELSE '🔴 MAJOR'
  END as gpm_status,

  -- Warranty %
  st_warranty,
  bq_warranty,
  warranty_delta,
  CONCAT(CAST(warranty_pct AS STRING), '%') as warranty_variance_pct,
  CASE
    WHEN ABS(warranty_delta) < 0.5 THEN '✅ EXACT'
    WHEN ABS(warranty_delta) < 2 THEN '⚠️ MINOR'
    ELSE '🔴 MAJOR'
  END as warranty_status

FROM variance_calc
ORDER BY region;

-- ================================================================================
-- SUMMARY STATS
-- ================================================================================
/*
Uncomment to get summary statistics:

SELECT
  '✅ EXACT MATCH' as category,
  SUM(CASE WHEN ABS(booked_delta) < 0.01 THEN 1 ELSE 0 END) as total_booked_count,
  SUM(CASE WHEN ABS(produced_delta) < 0.01 THEN 1 ELSE 0 END) as dollars_produced_count,
  SUM(CASE WHEN ABS(lead_count_delta) <= 1 THEN 1 ELSE 0 END) as lead_count_count
FROM variance_calc

UNION ALL

SELECT
  '🔴 MAJOR VARIANCE',
  SUM(CASE WHEN ABS(booked_delta) >= 100 THEN 1 ELSE 0 END),
  SUM(CASE WHEN ABS(produced_delta) >= 100 THEN 1 ELSE 0 END),
  SUM(CASE WHEN ABS(lead_count_delta) > 5 THEN 1 ELSE 0 END)
FROM variance_calc;
*/