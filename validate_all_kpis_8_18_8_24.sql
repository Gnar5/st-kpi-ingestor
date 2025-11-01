-- Comprehensive KPI Validation for Week 8/18/2025 - 8/24/2025
-- Compare all 10 KPIs across all 6 Business Units against ServiceTitan baseline

WITH date_range AS (
  SELECT
    DATE('2025-08-18') as start_date,
    DATE('2025-08-24') as end_date
),

-- ServiceTitan Baseline Data
st_baseline AS (
  SELECT 'Tucson' as bu_name, 899 as bu_id, 39 as st_leads, 46 as st_estimates, 51.22 as st_close_rate, 89990.11 as st_booked, 83761.16 as st_produced, 48.00 as st_gpm, 0.38 as st_warranty, 412265.50 as st_ar, 150992.16 as st_future, 92624.87 as st_collected
  UNION ALL SELECT 'Phoenix', 898, 96, 85, 39.74, 116551.26, 232891.98, 50.83, 1.26, 269530.00, 1076890.00, 250825.33
  UNION ALL SELECT 'Nevada', 901, 28, 22, 60.87, 105890.00, 23975.00, 24.04, 10.46, 216853.00, 239753.00, 95877.28
  UNION ALL SELECT "Andy's Painting", 95763481, 25, 24, 35.71, 30896.91, 53752.56, 47.83, 1.42, 164367.00, 249145.00, 65297.29
  UNION ALL SELECT 'Commercial AZ', 2305, 22, 24, 26.92, 119803.60, 77345.25, 46.98, 0.00, 488924.00, 355529.00, 62439.50
  UNION ALL SELECT 'Guaranteed Painting', 117043321, 8, 7, 77.78, 26067.40, 30472.30, 45.84, 0.00, 195840.00, 174697.00, 65521.11
),

-- 1. LEADS (Sales BUs)
bq_leads AS (
  SELECT
    REPLACE(business_unit, '-Sales', '') as bu_name,
    SUM(leads_count) as bq_leads
  FROM `kpi-auto-471020.st_mart_v2.leads_daily_bu`
  CROSS JOIN date_range
  WHERE kpi_date BETWEEN date_range.start_date AND date_range.end_date
  GROUP BY business_unit
),

-- 2. ESTIMATES (Sales BUs)
bq_estimates AS (
  SELECT
    REPLACE(business_unit, '-Sales', '') as bu_name,
    SUM(estimate_count) as bq_estimates
  FROM `kpi-auto-471020.st_mart_v2.completed_estimates_daily`
  CROSS JOIN date_range
  WHERE kpi_date BETWEEN date_range.start_date AND date_range.end_date
  GROUP BY business_unit
),

-- 3. CLOSE RATE (Sales BUs)
bq_close_rate AS (
  SELECT
    REPLACE(business_unit, '-Sales', '') as bu_name,
    ROUND(
      SAFE_DIVIDE(
        SUM(sold_jobs_count),
        SUM(opportunity_count)
      ) * 100,
      2
    ) as bq_close_rate
  FROM `kpi-auto-471020.st_mart_v2.opportunity_daily`
  CROSS JOIN date_range
  WHERE kpi_date BETWEEN date_range.start_date AND date_range.end_date
  GROUP BY business_unit
),

-- 4. TOTAL BOOKED (Sales BUs)
bq_booked AS (
  SELECT
    REPLACE(business_unit, '-Sales', '') as bu_name,
    ROUND(SUM(total_booked), 2) as bq_booked
  FROM `kpi-auto-471020.st_mart_v2.total_booked_daily`
  CROSS JOIN date_range
  WHERE kpi_date BETWEEN date_range.start_date AND date_range.end_date
  GROUP BY business_unit
),

-- 5. DOLLARS PRODUCED (Production BUs)
bq_produced AS (
  SELECT
    REPLACE(business_unit, '-Production', '') as bu_name,
    ROUND(SUM(revenue_subtotal), 2) as bq_produced
  FROM `kpi-auto-471020.st_mart_v2.dollars_produced_daily`
  CROSS JOIN date_range
  WHERE kpi_date BETWEEN date_range.start_date AND date_range.end_date
  GROUP BY business_unit
),

-- 6. GPM (Production BUs)
bq_gpm AS (
  SELECT
    REPLACE(business_unit, '-Production', '') as bu_name,
    ROUND(
      SAFE_DIVIDE(SUM(gross_profit), SUM(total_revenue)) * 100,
      2
    ) as bq_gpm
  FROM `kpi-auto-471020.st_mart_v2.gpm_daily_bu`
  CROSS JOIN date_range
  WHERE kpi_date BETWEEN date_range.start_date AND date_range.end_date
  GROUP BY business_unit
),

-- 7. WARRANTY (Production BUs)
bq_warranty AS (
  SELECT
    REPLACE(business_unit, '-Production', '') as bu_name,
    ROUND(
      SAFE_DIVIDE(SUM(warranty_revenue), SUM(total_revenue)) * 100,
      2
    ) as bq_warranty
  FROM `kpi-auto-471020.st_mart_v2.warranty_percent_daily_bu`
  CROSS JOIN date_range
  WHERE kpi_date BETWEEN date_range.start_date AND date_range.end_date
  GROUP BY business_unit
),

-- 8. OUTSTANDING AR (Production BUs, snapshot as of end_date)
bq_ar AS (
  SELECT
    REPLACE(business_unit, '-Production', '') as bu_name,
    ROUND(SUM(outstanding_amount), 2) as bq_ar
  FROM `kpi-auto-471020.st_mart_v2.outstanding_ar_daily_bu`
  CROSS JOIN date_range
  WHERE kpi_date = date_range.end_date
  GROUP BY business_unit
),

-- 9. FUTURE BOOKINGS (Production BUs, snapshot as of end_date)
bq_future AS (
  SELECT
    REPLACE(business_unit, '-Production', '') as bu_name,
    ROUND(SUM(future_bookings_amount), 2) as bq_future
  FROM `kpi-auto-471020.st_mart_v2.future_bookings_daily_bu`
  CROSS JOIN date_range
  WHERE kpi_date = date_range.end_date
  GROUP BY business_unit
),

-- 10. DOLLARS COLLECTED (Production BUs)
bq_collected AS (
  SELECT
    REPLACE(business_unit, '-Production', '') as bu_name,
    ROUND(SUM(amount_collected), 2) as bq_collected
  FROM `kpi-auto-471020.st_mart_v2.collections_daily_bu`
  CROSS JOIN date_range
  WHERE kpi_date BETWEEN date_range.start_date AND date_range.end_date
  GROUP BY business_unit
)

-- FINAL COMPARISON
SELECT
  st.bu_name as business_unit,

  -- LEADS
  st.st_leads as st_leads,
  l.bq_leads,
  l.bq_leads - st.st_leads as leads_diff,
  ROUND(SAFE_DIVIDE(ABS(l.bq_leads - st.st_leads), st.st_leads) * 100, 1) as leads_pct_diff,

  -- ESTIMATES
  st.st_estimates as st_estimates,
  e.bq_estimates,
  e.bq_estimates - st.st_estimates as est_diff,
  ROUND(SAFE_DIVIDE(ABS(e.bq_estimates - st.st_estimates), st.st_estimates) * 100, 1) as est_pct_diff,

  -- CLOSE RATE
  st.st_close_rate as st_close_rate,
  cr.bq_close_rate,
  ROUND(cr.bq_close_rate - st.st_close_rate, 2) as close_rate_diff,

  -- TOTAL BOOKED
  st.st_booked as st_booked,
  b.bq_booked,
  ROUND(b.bq_booked - st.st_booked, 2) as booked_diff,
  ROUND(SAFE_DIVIDE(ABS(b.bq_booked - st.st_booked), st.st_booked) * 100, 1) as booked_pct_diff,

  -- DOLLARS PRODUCED
  st.st_produced as st_produced,
  p.bq_produced,
  ROUND(p.bq_produced - st.st_produced, 2) as produced_diff,
  ROUND(SAFE_DIVIDE(ABS(p.bq_produced - st.st_produced), st.st_produced) * 100, 1) as produced_pct_diff,

  -- GPM
  st.st_gpm as st_gpm,
  g.bq_gpm,
  ROUND(g.bq_gpm - st.st_gpm, 2) as gpm_diff,

  -- WARRANTY
  st.st_warranty as st_warranty,
  w.bq_warranty,
  ROUND(w.bq_warranty - st.st_warranty, 2) as warranty_diff,

  -- OUTSTANDING AR
  st.st_ar as st_ar,
  ar.bq_ar,
  ROUND(ar.bq_ar - st.st_ar, 2) as ar_diff,
  ROUND(SAFE_DIVIDE(ABS(ar.bq_ar - st.st_ar), st.st_ar) * 100, 1) as ar_pct_diff,

  -- FUTURE BOOKINGS
  st.st_future as st_future,
  f.bq_future,
  ROUND(f.bq_future - st.st_future, 2) as future_diff,
  ROUND(SAFE_DIVIDE(ABS(f.bq_future - st.st_future), st.st_future) * 100, 1) as future_pct_diff,

  -- COLLECTED
  st.st_collected as st_collected,
  c.bq_collected,
  ROUND(c.bq_collected - st.st_collected, 2) as collected_diff,
  ROUND(SAFE_DIVIDE(ABS(c.bq_collected - st.st_collected), st.st_collected) * 100, 1) as collected_pct_diff

FROM st_baseline st
LEFT JOIN bq_leads l ON st.bu_name = l.bu_name
LEFT JOIN bq_estimates e ON st.bu_name = e.bu_name
LEFT JOIN bq_close_rate cr ON st.bu_name = cr.bu_name
LEFT JOIN bq_booked b ON st.bu_name = b.bu_name
LEFT JOIN bq_produced p ON st.bu_name = p.bu_name
LEFT JOIN bq_gpm g ON st.bu_name = g.bu_name
LEFT JOIN bq_warranty w ON st.bu_name = w.bu_name
LEFT JOIN bq_ar ar ON st.bu_name = ar.bu_name
LEFT JOIN bq_future f ON st.bu_name = f.bu_name
LEFT JOIN bq_collected c ON st.bu_name = c.bu_name
ORDER BY st.bu_name;
