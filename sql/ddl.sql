CREATE SCHEMA IF NOT EXISTS `kpi-auto-471020`.st_kpi OPTIONS(location="US");
CREATE SCHEMA IF NOT EXISTS `kpi-auto-471020`.st_kpi_mart OPTIONS(location="US");

CREATE TABLE IF NOT EXISTS `kpi-auto-471020`.st_kpi.raw_leads (
  bu_key STRING,
  job_created_on TIMESTAMP,
  created_date TIMESTAMP,
  customer_name STRING,
  job_type STRING,
  job_id STRING,
  updated_on TIMESTAMP,
  raw JSON
);
CREATE TABLE IF NOT EXISTS `kpi-auto-471020`.st_kpi.raw_daily_wbr (
  bu_key STRING, event_time TIMESTAMP, job_type STRING, total_sales NUMERIC, completed_job INT64, close_rate NUMERIC, job_id STRING, updated_on TIMESTAMP, raw JSON
);
CREATE TABLE IF NOT EXISTS `kpi-auto-471020`.st_kpi.raw_foreman (
  bu_key STRING, job_id STRING, job_start TIMESTAMP, job_type STRING,
  job_subtotal NUMERIC, job_total_costs NUMERIC, job_gm_pct NUMERIC,
  is_residential BOOL, scheduled_start_date DATE, updated_on TIMESTAMP, raw JSON
);
CREATE TABLE IF NOT EXISTS `kpi-auto-471020`.st_kpi.raw_collections (
  bu_key STRING, payment_date TIMESTAMP, amount NUMERIC, job_id STRING, updated_on TIMESTAMP, raw JSON
);
CREATE TABLE IF NOT EXISTS `kpi-auto-471020`.st_kpi.raw_ar (
  bu_key STRING, as_of TIMESTAMP, location_name STRING, net_amount NUMERIC, updated_on TIMESTAMP, raw JSON
);
CREATE TABLE IF NOT EXISTS `kpi-auto-471020`.st_kpi_mart.dim_calendar AS
SELECT d AS date, DATE_TRUNC(d, WEEK(MONDAY)) AS week_start
FROM UNNEST(GENERATE_DATE_ARRAY('2023-01-01', DATE_ADD(CURRENT_DATE(), INTERVAL 365 DAY))) d;
