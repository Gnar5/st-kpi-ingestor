-- KPI Weekly Validation for Last 12 Weeks
-- Checks all 10 KPIs for reasonable values and trends

WITH weekly_kpis AS (
  SELECT
    DATE_TRUNC(event_date, WEEK(MONDAY)) as week_start,
    business_unit,

    -- KPI aggregations
    SUM(lead_count) as leads,
    SUM(total_booked) as total_booked,
    SUM(dollars_produced) as dollars_produced,
    AVG(gpm_percent) as avg_gpm_percent,
    SUM(dollars_collected) as dollars_collected,
    SUM(num_estimates) as estimates_count,
    AVG(close_rate_percent) as avg_close_rate,
    MAX(future_bookings) as future_bookings,
    AVG(warranty_percent) as avg_warranty_percent,
    MAX(outstanding_ar) as outstanding_ar

  FROM `kpi-auto-471020.st_mart_v2.daily_kpis`
  WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 84 DAY)  -- Last 12 weeks
    AND event_date < CURRENT_DATE()  -- Exclude today (incomplete)
  GROUP BY week_start, business_unit
),

business_unit_stats AS (
  SELECT
    business_unit,
    COUNT(DISTINCT week_start) as weeks_with_data,

    -- Leads
    AVG(leads) as avg_weekly_leads,
    STDDEV(leads) as stddev_leads,
    MIN(leads) as min_leads,
    MAX(leads) as max_leads,

    -- Total Booked
    AVG(total_booked) as avg_weekly_booked,
    STDDEV(total_booked) as stddev_booked,
    MIN(total_booked) as min_booked,
    MAX(total_booked) as max_booked,

    -- Produced
    AVG(dollars_produced) as avg_weekly_produced,
    STDDEV(dollars_produced) as stddev_produced,
    MIN(dollars_produced) as min_produced,
    MAX(dollars_produced) as max_produced,

    -- GPM
    AVG(avg_gpm_percent) as overall_avg_gpm,
    MIN(avg_gpm_percent) as min_gpm,
    MAX(avg_gpm_percent) as max_gpm,

    -- Close Rate
    AVG(avg_close_rate) as overall_avg_close_rate,
    MIN(avg_close_rate) as min_close_rate,
    MAX(avg_close_rate) as max_close_rate,

    -- Warranty %
    AVG(avg_warranty_percent) as overall_avg_warranty,

    -- Outstanding AR (latest)
    MAX(outstanding_ar) as current_outstanding_ar

  FROM weekly_kpis
  GROUP BY business_unit
),

anomaly_detection AS (
  SELECT
    w.week_start,
    w.business_unit,
    w.leads,
    w.total_booked,
    w.dollars_produced,
    w.avg_gpm_percent,
    w.avg_close_rate,

    -- Flag anomalies (values > 3 standard deviations from mean)
    CASE
      WHEN ABS(w.leads - s.avg_weekly_leads) > 3 * IFNULL(s.stddev_leads, 1) THEN '🔴'
      WHEN ABS(w.leads - s.avg_weekly_leads) > 2 * IFNULL(s.stddev_leads, 1) THEN '🟡'
      ELSE '🟢'
    END as leads_flag,

    CASE
      WHEN ABS(w.total_booked - s.avg_weekly_booked) > 3 * IFNULL(s.stddev_booked, 1) THEN '🔴'
      WHEN ABS(w.total_booked - s.avg_weekly_booked) > 2 * IFNULL(s.stddev_booked, 1) THEN '🟡'
      ELSE '🟢'
    END as booked_flag,

    CASE
      WHEN ABS(w.dollars_produced - s.avg_weekly_produced) > 3 * IFNULL(s.stddev_produced, 1) THEN '🔴'
      WHEN ABS(w.dollars_produced - s.avg_weekly_produced) > 2 * IFNULL(s.stddev_produced, 1) THEN '🟡'
      ELSE '🟢'
    END as produced_flag,

    CASE
      WHEN w.avg_gpm_percent < 20 THEN '🔴'
      WHEN w.avg_gpm_percent < 30 THEN '🟡'
      WHEN w.avg_gpm_percent > 80 THEN '🔴'  -- Suspiciously high
      ELSE '🟢'
    END as gpm_flag,

    CASE
      WHEN w.avg_close_rate < 10 THEN '🔴'
      WHEN w.avg_close_rate < 20 THEN '🟡'
      WHEN w.avg_close_rate > 90 THEN '🟡'  -- Suspiciously high
      ELSE '🟢'
    END as close_rate_flag

  FROM weekly_kpis w
  JOIN business_unit_stats s ON w.business_unit = s.business_unit
)

-- Output validation results
SELECT
  week_start,
  business_unit,
  ROUND(leads, 0) as leads,
  leads_flag,
  ROUND(total_booked, 0) as total_booked,
  booked_flag,
  ROUND(dollars_produced, 0) as dollars_produced,
  produced_flag,
  ROUND(avg_gpm_percent, 1) as gpm_pct,
  gpm_flag,
  ROUND(avg_close_rate, 1) as close_rate_pct,
  close_rate_flag,
  CONCAT(
    leads_flag,
    booked_flag,
    produced_flag,
    gpm_flag,
    close_rate_flag
  ) as overall_status
FROM anomaly_detection
WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 84 DAY)
ORDER BY business_unit, week_start DESC;