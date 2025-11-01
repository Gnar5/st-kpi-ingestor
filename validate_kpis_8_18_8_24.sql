-- Validate all 10 KPIs for week 8/18/2025 - 8/24/2025
-- Compare BigQuery against ServiceTitan baseline

WITH date_range AS (
  SELECT
    DATE('2025-08-18') as start_date,
    DATE('2025-08-24') as end_date
),

-- 1. Lead Count
bq_leads AS (
  SELECT
    business_unit,
    SUM(lead_count) as lead_count
  FROM `kpi-auto-471020.st_mart_v2.leads_daily_bu`
  CROSS JOIN date_range
  WHERE kpi_date BETWEEN date_range.start_date AND date_range.end_date
  GROUP BY business_unit
),

-- 2. Num Estimates
bq_estimates AS (
  SELECT
    business_unit,
    SUM(estimate_count) as num_estimates
  FROM `kpi-auto-471020.st_mart_v2.completed_estimates_daily`
  CROSS JOIN date_range
  WHERE kpi_date BETWEEN date_range.start_date AND date_range.end_date
  GROUP BY business_unit
),

-- 3. Close Rate (from opportunity_daily)
bq_close_rate AS (
  SELECT
    business_unit,
    ROUND(AVG(close_rate_percent), 2) as close_rate_percent
  FROM `kpi-auto-471020.st_mart_v2.opportunity_daily`
  CROSS JOIN date_range
  WHERE kpi_date BETWEEN date_range.start_date AND date_range.end_date
  GROUP BY business_unit
),

-- 4. Total Booked
bq_booked AS (
  SELECT
    business_unit,
    ROUND(SUM(total_booked), 2) as total_booked
  FROM `kpi-auto-471020.st_mart_v2.total_booked_daily`
  CROSS JOIN date_range
  WHERE kpi_date BETWEEN date_range.start_date AND date_range.end_date
  GROUP BY business_unit
),

-- 5. Dollars Produced
bq_produced AS (
  SELECT
    business_unit,
    ROUND(SUM(total_revenue), 2) as dollars_produced
  FROM `kpi-auto-471020.st_mart_v2.dollars_produced_daily`
  CROSS JOIN date_range
  WHERE kpi_date BETWEEN date_range.start_date AND date_range.end_date
  GROUP BY business_unit
),

-- 6. GPM Percent
bq_gpm AS (
  SELECT
    business_unit,
    ROUND(SAFE_DIVIDE(SUM(gross_profit), SUM(total_revenue)) * 100, 2) as gpm_percent
  FROM `kpi-auto-471020.st_mart_v2.gpm_daily_bu`
  CROSS JOIN date_range
  WHERE kpi_date BETWEEN date_range.start_date AND date_range.end_date
  GROUP BY business_unit
),

-- 7. Warranty Percent
bq_warranty AS (
  SELECT
    business_unit,
    ROUND(AVG(warranty_percent), 2) as warranty_percent
  FROM `kpi-auto-471020.st_mart_v2.warranty_percent_daily_bu`
  CROSS JOIN date_range
  WHERE kpi_date BETWEEN date_range.start_date AND date_range.end_date
  GROUP BY business_unit
),

-- 8. Outstanding AR (as of end date)
bq_ar AS (
  SELECT
    business_unit,
    ROUND(SUM(outstanding_amount), 2) as outstanding_ar
  FROM `kpi-auto-471020.st_mart_v2.outstanding_ar_daily_bu`
  CROSS JOIN date_range
  WHERE kpi_date = date_range.end_date
  GROUP BY business_unit
),

-- 9. Future Bookings (as of end date)
bq_future AS (
  SELECT
    business_unit,
    ROUND(SUM(future_bookings_amount), 2) as future_bookings
  FROM `kpi-auto-471020.st_mart_v2.future_bookings_daily_bu`
  CROSS JOIN date_range
  WHERE kpi_date = date_range.end_date
  GROUP BY business_unit
),

-- 10. Dollars Collected
bq_collected AS (
  SELECT
    business_unit,
    ROUND(SUM(amount_collected), 2) as dollars_collected
  FROM `kpi-auto-471020.st_mart_v2.collections_daily_bu`
  CROSS JOIN date_range
  WHERE kpi_date BETWEEN date_range.start_date AND date_range.end_date
  GROUP BY business_unit
),

-- ServiceTitan baseline
st_baseline AS (
  SELECT 'Tucson-Production' as business_unit, 39 as st_lead_count, 46 as st_num_estimates, 51.22 as st_close_rate, 89990.11 as st_total_booked, 83761.16 as st_dollars_produced, 48.00 as st_gpm, 0.38 as st_warranty, 412265.50 as st_ar, 150992.16 as st_future, 92624.87 as st_collected
  UNION ALL SELECT 'Phoenix-Production', 96, 85, 39.74, 116551.26, 232891.98, 50.83, 1.26, 269530.00, 1076890.00, 250825.33
  UNION ALL SELECT 'Nevada-Production', 28, 22, 60.87, 105890.00, 23975.00, 24.04, 10.46, 216853.00, 239753.00, 95877.28
  UNION ALL SELECT 'Andy''s Painting-Production', 25, 24, 35.71, 30896.91, 53752.56, 47.83, 1.42, 164367.00, 249145.00, 65297.29
  UNION ALL SELECT 'Commercial-AZ-Production', 22, 24, 26.92, 119803.60, 77345.25, 46.98, 0.00, 488924.00, 355529.00, 62439.50
  UNION ALL SELECT 'Guaranteed Painting-Production', 8, 7, 77.78, 26067.40, 30472.30, 45.84, 0.00, 195840.00, 174697.00, 65521.11
)

-- Join everything and compare
SELECT
  COALESCE(st.business_unit, l.business_unit) as business_unit,

  -- Lead Count
  st.st_lead_count,
  l.lead_count as bq_lead_count,
  l.lead_count - st.st_lead_count as lead_diff,

  -- Num Estimates
  st.st_num_estimates,
  e.num_estimates as bq_num_estimates,
  e.num_estimates - st.st_num_estimates as est_diff,

  -- Close Rate
  st.st_close_rate,
  cr.close_rate_percent as bq_close_rate,
  ROUND(cr.close_rate_percent - st.st_close_rate, 2) as close_rate_diff,

  -- Total Booked
  st.st_total_booked,
  b.total_booked as bq_total_booked,
  ROUND(b.total_booked - st.st_total_booked, 2) as booked_diff,

  -- Dollars Produced
  st.st_dollars_produced,
  p.dollars_produced as bq_dollars_produced,
  ROUND(p.dollars_produced - st.st_dollars_produced, 2) as produced_diff,

  -- GPM
  st.st_gpm,
  g.gpm_percent as bq_gpm,
  ROUND(g.gpm_percent - st.st_gpm, 2) as gpm_diff,

  -- Warranty
  st.st_warranty,
  w.warranty_percent as bq_warranty,
  ROUND(w.warranty_percent - st.st_warranty, 2) as warranty_diff,

  -- Outstanding AR
  st.st_ar,
  ar.outstanding_ar as bq_ar,
  ROUND(ar.outstanding_ar - st.st_ar, 2) as ar_diff,

  -- Future Bookings
  st.st_future,
  f.future_bookings as bq_future,
  ROUND(f.future_bookings - st.st_future, 2) as future_diff,

  -- Collections
  st.st_collected,
  c.dollars_collected as bq_collected,
  ROUND(c.dollars_collected - st.st_collected, 2) as collected_diff

FROM st_baseline st
LEFT JOIN bq_leads l ON st.business_unit = l.business_unit
LEFT JOIN bq_estimates e ON st.business_unit = e.business_unit
LEFT JOIN bq_close_rate cr ON st.business_unit = cr.business_unit
LEFT JOIN bq_booked b ON st.business_unit = b.business_unit
LEFT JOIN bq_produced p ON st.business_unit = p.business_unit
LEFT JOIN bq_gpm g ON st.business_unit = g.business_unit
LEFT JOIN bq_warranty w ON st.business_unit = w.business_unit
LEFT JOIN bq_ar ar ON st.business_unit = ar.business_unit
LEFT JOIN bq_future f ON st.business_unit = f.business_unit
LEFT JOIN bq_collected c ON st.business_unit = c.business_unit
ORDER BY st.business_unit;
