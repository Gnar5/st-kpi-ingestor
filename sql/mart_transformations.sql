-- ========================================
-- MART TRANSFORMATION QUERIES
-- ========================================
-- These queries transform raw data into mart fact tables
-- Run these via mart update endpoints

-- ========================================
-- 1. LEADS DAILY FACT
-- ========================================
-- Aggregates leads by date and BU
CREATE OR REPLACE TABLE `kpi-auto-471020.st_kpi_mart.leads_daily_fact` AS
SELECT
  DATE(job_created_on) as kpi_date,
  bu_key,
  COUNT(*) as leads
FROM `kpi-auto-471020.st_raw.raw_leads`
WHERE job_created_on IS NOT NULL
  AND bu_key IS NOT NULL
GROUP BY kpi_date, bu_key;

-- ========================================
-- 2. COLLECTIONS DAILY FACT
-- ========================================
-- Aggregates collections by payment date and BU
CREATE OR REPLACE TABLE `kpi-auto-471020.st_kpi_mart.collections_daily_fact` AS
SELECT
  DATE(payment_date) as kpi_date,
  bu_key,
  ROUND(SUM(amount), 2) as collected_amount
FROM `kpi-auto-471020.st_raw.raw_collections`
WHERE payment_date IS NOT NULL
  AND bu_key IS NOT NULL
GROUP BY kpi_date, bu_key;

-- ========================================
-- 3. WBR DAILY FACT
-- ========================================
-- Aggregates WBR metrics by date and BU
-- Using corrected close_rate calculation
CREATE OR REPLACE TABLE `kpi-auto-471020.st_kpi_mart.wbr_daily_fact` AS
SELECT
  event_date as kpi_date,
  bu_name as bu_key,
  SUM(sales_opportunities) as estimates,
  SUM(closed_opportunities) as booked,
  -- Close rate as decimal (e.g., 0.3727 = 37.27%)
  ROUND(
    SUM(closed_opportunities) / NULLIF(SUM(sales_opportunities), 0),
    4
  ) as close_rate_decimal,
  ROUND(SUM(total_sales), 2) as total_sales,
  -- Average closed sale
  ROUND(
    SUM(total_sales) / NULLIF(SUM(closed_opportunities), 0),
    2
  ) as avg_closed_sale,
  SUM(completed_jobs) as completed_jobs
FROM `kpi-auto-471020.st_raw.raw_daily_wbr_v2`
WHERE event_date IS NOT NULL
  AND bu_name IS NOT NULL
GROUP BY kpi_date, bu_key;

-- ========================================
-- 4. FOREMAN DAILY FACT (NEW)
-- ========================================
-- Aggregates foreman job metrics by start date and BU
CREATE TABLE IF NOT EXISTS `kpi-auto-471020.st_kpi_mart.foreman_daily_fact` (
  kpi_date DATE,
  bu_key STRING,
  total_jobs INT64,
  total_subtotal NUMERIC,
  total_costs NUMERIC,
  gm_pct NUMERIC
);

CREATE OR REPLACE TABLE `kpi-auto-471020.st_kpi_mart.foreman_daily_fact` AS
SELECT
  DATE(job_start) as kpi_date,
  bu_key,
  COUNT(*) as total_jobs,
  ROUND(SUM(job_subtotal), 2) as total_subtotal,
  ROUND(SUM(job_total_costs), 2) as total_costs,
  -- GM% calculated from totals (matches ServiceTitan)
  ROUND(
    (SUM(job_subtotal) - SUM(job_total_costs)) * 100.0 / NULLIF(SUM(job_subtotal), 0),
    2
  ) as gm_pct
FROM `kpi-auto-471020.st_raw.raw_foreman`
WHERE job_start IS NOT NULL
  AND bu_key IS NOT NULL
GROUP BY kpi_date, bu_key;
