-- BigQuery dedupe script for ServiceTitan raw ingestion tables.
-- Run sections individually (or the whole file) with:
--   bq query --use_legacy_sql=false < sql/dedupe_raw_tables.sql

-- 1. Leads: keep the latest record per job + BU.
-- Changed to order by job_created_on DESC (actual ServiceTitan data), then updated_on DESC (our ingestion time)
-- This ensures we keep the most recent lead data from ServiceTitan, not just our most recent pull
CREATE OR REPLACE TABLE `kpi-auto-471020.st_raw.raw_leads` AS
WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY job_id, bu_key
           ORDER BY job_created_on DESC, updated_on DESC
         ) AS rn
  FROM `kpi-auto-471020.st_raw.raw_leads`
)
SELECT * EXCEPT(rn)
FROM ranked
WHERE rn = 1;

-- 2. Collections: dedupe by invoice (job_id) + payment date.
CREATE OR REPLACE TABLE `kpi-auto-471020.st_raw.raw_collections` AS
WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY job_id, payment_date
           ORDER BY updated_on DESC
         ) AS rn
  FROM `kpi-auto-471020.st_raw.raw_collections`
)
SELECT * EXCEPT(rn)
FROM ranked
WHERE rn = 1;

-- 3. Accounts Receivable: use raw payload identity; keep most recent snapshot.
CREATE OR REPLACE TABLE `kpi-auto-471020.st_raw.raw_ar` AS
WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY TO_JSON_STRING(raw)
           ORDER BY updated_on DESC
         ) AS rn
  FROM `kpi-auto-471020.st_raw.raw_ar`
)
SELECT * EXCEPT(rn)
FROM ranked
WHERE rn = 1;

-- 4. Foreman job cost: dedupe by job and BU.
CREATE OR REPLACE TABLE `kpi-auto-471020.st_raw.raw_foreman` AS
WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY job_id, bu_key
           ORDER BY updated_on DESC
         ) AS rn
  FROM `kpi-auto-471020.st_raw.raw_foreman`
)
SELECT * EXCEPT(rn)
FROM ranked
WHERE rn = 1;

-- 5. Future bookings: keep the latest as_of snapshot per job + scheduled date.
CREATE OR REPLACE TABLE `kpi-auto-471020.st_raw.raw_future_bookings` AS
WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY job_id, scheduled_date
           ORDER BY as_of_date DESC
         ) AS rn
  FROM `kpi-auto-471020.st_raw.raw_future_bookings`
)
SELECT * EXCEPT(rn)
FROM ranked
WHERE rn = 1;

-- 6. Daily WBR (v2): dedupe by day/BU/estimator (rows are otherwise identical).
CREATE OR REPLACE TABLE `kpi-auto-471020.st_raw.raw_daily_wbr_v2` AS
WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY event_date, bu_name, estimator
           ORDER BY total_sales DESC, avg_closed_sale DESC
         ) AS rn
  FROM `kpi-auto-471020.st_raw.raw_daily_wbr_v2`
)
SELECT * EXCEPT(rn)
FROM ranked
WHERE rn = 1;
