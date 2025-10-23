-- Comprehensive Reconciliation Report
-- Purpose: Monthly validation of BigQuery KPIs against ServiceTitan exports
-- Usage: Update @start_date and @end_date parameters, then compare results with ST exports
--
-- Root Causes Identified:
-- 1. Production "Dollars Produced" - ST includes jobStatus IN ('Completed', 'Hold')
-- 2. Sales "Total Booked" - ST uses estimate.soldOn date, not job.createdOn date
--
-- This query provides side-by-side comparison for validation

DECLARE start_date DATE DEFAULT '2025-08-18';
DECLARE end_date DATE DEFAULT '2025-08-24';

-- =============================================================================
-- PRODUCTION KPIs - Compare with ServiceTitan FOREMAN Job Cost Report
-- =============================================================================
WITH production_comparison AS (
  SELECT
    'PRODUCTION' as kpi_type,
    business_unit,
    ROUND(SUM(dollars_produced), 2) as bq_value,

    -- Expected ST values for 08/18-08/24 (update these for each validation)
    CASE business_unit
      WHEN 'Phoenix-Production' THEN 232891.98
      WHEN 'Tucson-Production' THEN 83761.16
      WHEN 'Nevada-Production' THEN 23975.00
      WHEN "Andy's Painting-Production" THEN 53752.56
      WHEN 'Commercial-AZ-Production' THEN 77345.25
      WHEN 'Guaranteed Painting-Production' THEN 30472.30
      ELSE 0
    END as st_expected_value

  FROM `kpi-auto-471020.st_mart_v2.daily_kpis`
  WHERE business_unit LIKE '%Production'
    AND event_date BETWEEN start_date AND end_date
  GROUP BY business_unit
),

-- =============================================================================
-- SALES KPIs - Compare with ServiceTitan BU Sales - API Report
-- =============================================================================
sales_comparison AS (
  SELECT
    'SALES' as kpi_type,
    business_unit,
    ROUND(SUM(total_booked), 2) as bq_value,

    -- Expected ST values for 08/18-08/24 (update these for each validation)
    CASE business_unit
      WHEN 'Phoenix-Sales' THEN 116551.26
      WHEN 'Tucson-Sales' THEN 89990.11
      WHEN 'Nevada-Sales' THEN 105890.00
      WHEN "Andy's Painting-Sales" THEN 30896.91
      WHEN 'Commercial-AZ-Sales' THEN 119803.60
      WHEN 'Guaranteed Painting-Sales' THEN 26067.40
      ELSE 0
    END as st_expected_value

  FROM `kpi-auto-471020.st_mart_v2.daily_kpis`
  WHERE business_unit LIKE '%Sales'
    AND event_date BETWEEN start_date AND end_date
  GROUP BY business_unit
),

-- =============================================================================
-- COMBINED RESULTS
-- =============================================================================
all_comparisons AS (
  SELECT * FROM production_comparison
  UNION ALL
  SELECT * FROM sales_comparison
)

SELECT
  kpi_type,
  business_unit,
  bq_value,
  st_expected_value,
  ROUND(bq_value - st_expected_value, 2) as variance,
  CASE
    WHEN st_expected_value = 0 THEN NULL
    ELSE ROUND(((bq_value - st_expected_value) / st_expected_value) * 100, 2)
  END as variance_percent,
  CASE
    WHEN ABS(bq_value - st_expected_value) < 0.01 THEN '✅ EXACT MATCH'
    WHEN ABS(bq_value - st_expected_value) < 100 THEN '⚠️ MINOR VARIANCE'
    ELSE '🔴 MAJOR VARIANCE'
  END as status
FROM all_comparisons
ORDER BY kpi_type, business_unit;

-- =============================================================================
-- FIELD MAPPING REFERENCE
-- =============================================================================
--
-- ServiceTitan FOREMAN Job Cost Report:
--   - Date Field: Job Start Date (first scheduled appointment)
--   - Job Status Filter: Completed AND Hold
--   - Revenue Field: Job invoice subtotal
--   - BigQuery Equivalent: DATE(job_costing.job_start_date) WHERE jobStatus IN ('Completed', 'Hold')
--
-- ServiceTitan BU Sales - API Report:
--   - Date Field: Estimate Sold Date
--   - Status Filter: Sold estimates only
--   - Revenue Field: Estimate total (or subTotal if total is NULL)
--   - BigQuery Equivalent: DATE(estimates.soldOn) WHERE status = 'Sold'
--
-- =============================================================================
