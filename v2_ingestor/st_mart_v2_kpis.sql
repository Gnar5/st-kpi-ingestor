-- =====================================================================
-- ServiceTitan KPI Marts - Auto-Discovery & Weekly Rollups
-- =====================================================================
-- Purpose: Replicate legacy ST report workflows in durable BigQuery marts
-- Version: 1.0.0
-- Project: kpi-auto-471020
-- Datasets: st_raw_v2 (source), st_ref_v2 (reference), st_mart_v2 (marts)
--
-- Execution: Run this entire file top-to-bottom in BigQuery Console
-- Dependencies: st_raw_v2.raw_* tables must exist and be populated
-- =====================================================================

-- =====================================================================
-- STEP 1: CREATE DATASETS
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS `kpi-auto-471020.st_ref_v2`
OPTIONS(
  location='US',
  description='Reference dimensions and lookup tables for ServiceTitan v2'
);

CREATE SCHEMA IF NOT EXISTS `kpi-auto-471020.st_mart_v2`
OPTIONS(
  location='US',
  description='KPI marts and analytical views for ServiceTitan v2'
);

-- =====================================================================
-- STEP 2: AUTO-DISCOVER BUSINESS UNIT IDS
-- =====================================================================
-- Scans raw_jobs, raw_invoices, raw_estimates to find BU name→ID mappings
-- Uses mode (most frequent ID) when multiple IDs exist for same name
-- =====================================================================

CREATE OR REPLACE TABLE `kpi-auto-471020.st_ref_v2.dim_bu_rollup` AS

WITH bu_name_id_pairs AS (
  -- Collect all BU name/ID pairs from existing dim_business_units
  SELECT
    id AS businessUnitId,
    name AS bu_name
  FROM `kpi-auto-471020.st_ref_v2.dim_business_units`
  WHERE id IS NOT NULL AND name IS NOT NULL
),

-- Define target BU names and their rollups
target_bus AS (
  SELECT 'Andy\'s Painting-Sales' AS bu_name, 'SALES' AS bu_group, 'ANDYS' AS bu_rollup UNION ALL
  SELECT 'Commercial-AZ-Sales', 'SALES', 'COMM_AZ' UNION ALL
  SELECT 'Guaranteed Painting-Sales', 'SALES', 'GUAR_TX' UNION ALL
  SELECT 'Nevada-Sales', 'SALES', 'NEV' UNION ALL
  SELECT 'Phoenix-Sales', 'SALES', 'PHX' UNION ALL
  SELECT 'Z-DO NOT USE - West - Sales', 'SALES', 'PHX' UNION ALL

  SELECT 'Andy\'s Painting-Production' AS bu_name, 'PRODUCTION' AS bu_group, 'ANDYS' AS bu_rollup UNION ALL
  SELECT 'Commercial-AZ-Production', 'PRODUCTION', 'COMM_AZ' UNION ALL
  SELECT 'Guaranteed Painting-Production', 'PRODUCTION', 'GUAR_TX' UNION ALL
  SELECT 'Nevada-Production', 'PRODUCTION', 'NEV' UNION ALL
  SELECT 'Phoenix-Production', 'PRODUCTION', 'PHX' UNION ALL
  SELECT 'Z-DO NOT USE - West- Production', 'PRODUCTION', 'PHX'
),

-- Match target BUs with discovered IDs
matched_bus AS (
  SELECT
    t.bu_name,
    t.bu_group,
    t.bu_rollup,
    p.businessUnitId,
    CASE WHEN p.businessUnitId IS NOT NULL THEN TRUE ELSE FALSE END AS discovered
  FROM target_bus t
  LEFT JOIN bu_name_id_pairs p
    ON TRIM(t.bu_name) = TRIM(p.bu_name)
)

SELECT
  businessUnitId,
  bu_name,
  bu_group,
  bu_rollup,
  discovered,
  CURRENT_TIMESTAMP() AS created_at
FROM matched_bus
ORDER BY bu_group, bu_rollup, bu_name;

-- Log discovery results
SELECT
  bu_group,
  bu_rollup,
  COUNT(*) AS bu_count,
  SUM(CASE WHEN discovered THEN 1 ELSE 0 END) AS discovered_count,
  SUM(CASE WHEN NOT discovered THEN 1 ELSE 0 END) AS missing_count
FROM `kpi-auto-471020.st_ref_v2.dim_bu_rollup`
GROUP BY bu_group, bu_rollup
ORDER BY bu_group, bu_rollup;

-- =====================================================================
-- STEP 3: CREATE JOB TYPE REFERENCE LISTS
-- =====================================================================

CREATE OR REPLACE TABLE `kpi-auto-471020.st_ref_v2.jobtype_lists` (
  list STRING NOT NULL,
  jobTypeName STRING NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Daily WBR (Sales) ALLOW list
INSERT INTO `kpi-auto-471020.st_ref_v2.jobtype_lists` (list, jobTypeName) VALUES
('DAILY_WBR_SALES_ALLOW', 'ESTIMATE- WINDOW WASHING'),
('DAILY_WBR_SALES_ALLOW', 'Estimate'),
('DAILY_WBR_SALES_ALLOW', 'Cabinets'),
('DAILY_WBR_SALES_ALLOW', 'Estimate- Exterior PLUS Int Cabinets'),
('DAILY_WBR_SALES_ALLOW', 'Estimate- Interior PLUS Cabinets'),
('DAILY_WBR_SALES_ALLOW', 'ESTIMATE -RES-EXT-PRE 1960'),
('DAILY_WBR_SALES_ALLOW', 'ESTIMATE -RES-INT/EXT-PRE 1960'),
('DAILY_WBR_SALES_ALLOW', 'ESTIMATE-COMM-EXT'),
('DAILY_WBR_SALES_ALLOW', 'ESTIMATE-COMM-EXT/INT'),
('DAILY_WBR_SALES_ALLOW', 'ESTIMATE-COMM-INT'),
('DAILY_WBR_SALES_ALLOW', 'ESTIMATE-COMM-PLANBID'),
('DAILY_WBR_SALES_ALLOW', 'ESTIMATE-COMM-Striping'),
('DAILY_WBR_SALES_ALLOW', 'ESTIMATE-FLOOR COATING-EPOXY'),
('DAILY_WBR_SALES_ALLOW', 'ESTIMATE-FLOOR COATING-H&C Coatings'),
('DAILY_WBR_SALES_ALLOW', 'ESTIMATE-POPCORN'),
('DAILY_WBR_SALES_ALLOW', 'ESTIMATE-RES-EXT'),
('DAILY_WBR_SALES_ALLOW', 'ESTIMATE-RES-EXT/INT'),
('DAILY_WBR_SALES_ALLOW', 'ESTIMATE-RES-HOA'),
('DAILY_WBR_SALES_ALLOW', 'ESTIMATE-RES-INT'),
('DAILY_WBR_SALES_ALLOW', 'Estimate-RES-INT/EXT Plus Cabinets');

-- Foreman (Production) EXCLUDE list
INSERT INTO `kpi-auto-471020.st_ref_v2.jobtype_lists` (list, jobTypeName) VALUES
('FOREMAN_PROD_EXCLUDE', 'PM Inspection'),
('FOREMAN_PROD_EXCLUDE', 'Safety Inspection'),
('FOREMAN_PROD_EXCLUDE', 'Window/Solar Washing');

-- Warranty INCLUDE list
INSERT INTO `kpi-auto-471020.st_ref_v2.jobtype_lists` (list, jobTypeName) VALUES
('WARRANTY_INCLUDE', 'Warranty'),
('WARRANTY_INCLUDE', 'Touchup');

-- =====================================================================
-- STEP 4: DAILY HELPER VIEWS
-- =====================================================================

-- ---------------------------------------------------------------------
-- v_jobs_completed_daily
-- All completed jobs with key dimensions
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.v_jobs_completed_daily` AS
SELECT
  j.businessUnitId,
  DATE(j.completedOn) AS event_date,
  j.jobNumber,
  j.id AS jobId,
  SAFE_CAST(JSON_EXTRACT_SCALAR(j.customFields, '$.jobSubtotal') AS FLOAT64) AS jobSubtotal,
  jt.name AS jobTypeName,
  j.completedOn AS scheduledStartOn,  -- Note: Using completedOn as proxy for scheduledStartOn
  CASE
    WHEN LOWER(jt.name) IN ('warranty', 'touchup') THEN TRUE
    ELSE FALSE
  END AS isWarranty,
  j.customerId,
  j.locationId,
  j.soldById,
  j.jobStatus,
  j._ingested_at
FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_business_units` bu
  ON j.businessUnitId = bu.id
LEFT JOIN UNNEST([STRUCT(
  CAST(j.jobTypeId AS STRING) AS id,
  CAST(j.jobTypeId AS STRING) AS name  -- Placeholder - need job types dimension
)]) jt
WHERE j.completedOn IS NOT NULL;

-- ---------------------------------------------------------------------
-- v_invoices_daily
-- All invoices with key dimensions
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.v_invoices_daily` AS
SELECT
  i.businessUnitId,
  DATE(i.createdOn) AS event_date,
  i.invoiceNumber,
  i.jobNumber,
  i.id AS invoiceId,
  SAFE_CAST(i.total AS FLOAT64) AS total,
  SAFE_CAST(i.subtotal AS FLOAT64) AS subtotal,
  i.jobId,
  i.customerId,
  i.createdOn,
  i._ingested_at
FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
WHERE i.createdOn IS NOT NULL;

-- ---------------------------------------------------------------------
-- v_payments_daily
-- All payments with key dimensions
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.v_payments_daily` AS
SELECT
  p.businessUnitId,
  DATE(p.receivedOn) AS event_date,
  p.invoiceId,
  p.id AS paymentId,
  SAFE_CAST(p.amount AS FLOAT64) AS amount,
  p.paymentType,
  p.receivedOn,
  p._ingested_at
FROM `kpi-auto-471020.st_raw_v2.raw_payments` p
WHERE p.receivedOn IS NOT NULL;

-- ---------------------------------------------------------------------
-- v_estimates_daily
-- All estimates with key dimensions for Leads/Estimates KPIs
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.v_estimates_daily` AS
SELECT
  e.businessUnitId,
  DATE(e.createdOn) AS event_date,
  e.id AS estimateId,
  e.name AS estimateName,
  e.status,
  e.jobNumber,
  e.customerId,
  c.name AS customerName,
  e.soldById,
  SAFE_CAST(e.total AS FLOAT64) AS total,
  SAFE_CAST(e.subtotal AS FLOAT64) AS subtotal,
  e.createdOn,
  e.soldOn,
  e._ingested_at,
  -- Get job type name by joining with jobs
  CAST(j.jobTypeId AS STRING) AS jobTypeName  -- Placeholder
FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_customers` c
  ON e.customerId = c.id
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_jobs` j
  ON e.jobNumber = j.jobNumber
WHERE e.createdOn IS NOT NULL;

-- ---------------------------------------------------------------------
-- v_payroll_daily
-- All payroll entries for labor cost calculations
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.v_payroll_daily` AS
SELECT
  SAFE_CAST(bu.id AS INT64) AS businessUnitId,
  DATE(p.date) AS event_date,
  p.jobNumber,
  p.payrollId,
  p.employeeId,
  SAFE_CAST(p.paidDurationHours AS FLOAT64) AS paidDurationHours,
  SAFE_DIVIDE(
    SAFE_CAST(p.amount AS FLOAT64),
    NULLIF(SAFE_CAST(p.paidDurationHours AS FLOAT64), 0)
  ) AS hourly_rate,
  SAFE_CAST(p.amount AS FLOAT64) AS paid_amount,
  p.activity,
  p.date,
  p._ingested_at
FROM `kpi-auto-471020.st_raw_v2.raw_payroll` p
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_business_units` bu
  ON p.businessUnitName = bu.name
WHERE p.date IS NOT NULL;

-- ---------------------------------------------------------------------
-- v_invoice_balances
-- AR balance calculation: invoice total - payments
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.v_invoice_balances` AS
WITH invoice_totals AS (
  SELECT
    i.id AS invoiceId,
    i.invoiceNumber,
    i.businessUnitId,
    i.jobNumber,
    SAFE_CAST(i.total AS FLOAT64) AS invoice_total,
    i.createdOn,
    l.name AS locationName
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_locations` l
    ON i.locationId = l.id
),

payments_by_invoice AS (
  SELECT
    invoiceId,
    SUM(SAFE_CAST(amount AS FLOAT64)) AS paid_to_date
  FROM `kpi-auto-471020.st_raw_v2.raw_payments`
  GROUP BY invoiceId
)

SELECT
  i.invoiceId,
  i.invoiceNumber,
  i.businessUnitId,
  i.jobNumber,
  i.invoice_total,
  COALESCE(p.paid_to_date, 0) AS paid_to_date,
  i.invoice_total - COALESCE(p.paid_to_date, 0) AS ar_balance,
  i.locationName,
  i.createdOn
FROM invoice_totals i
LEFT JOIN payments_by_invoice p
  ON i.invoiceId = p.invoiceId;

-- =====================================================================
-- STEP 5: KPI DAILY VIEWS
-- =====================================================================

-- ---------------------------------------------------------------------
-- kpi_leads_daily
-- SALES BUs; LOWER(customerName) NOT LIKE '%test%';
-- LOWER(jobTypeName) LIKE '%estimate%' and NOT LIKE '%comm%';
-- COUNT DISTINCT customerId
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.kpi_leads_daily` AS
SELECT
  bu.bu_rollup,
  e.event_date,
  COUNT(DISTINCT e.customerId) AS leads_count
FROM `kpi-auto-471020.st_mart_v2.v_estimates_daily` e
INNER JOIN `kpi-auto-471020.st_ref_v2.dim_bu_rollup` bu
  ON e.businessUnitId = bu.businessUnitId
WHERE bu.bu_group = 'SALES'
  AND LOWER(COALESCE(e.customerName, '')) NOT LIKE '%test%'
  AND LOWER(COALESCE(e.jobTypeName, '')) LIKE '%estimate%'
  AND LOWER(COALESCE(e.jobTypeName, '')) NOT LIKE '%comm%'
GROUP BY bu.bu_rollup, e.event_date;

-- ---------------------------------------------------------------------
-- kpi_estimates_daily
-- SALES BUs; ALLOW list join; COUNT(*)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.kpi_estimates_daily` AS
SELECT
  bu.bu_rollup,
  e.event_date,
  COUNT(*) AS estimates_count
FROM `kpi-auto-471020.st_mart_v2.v_estimates_daily` e
INNER JOIN `kpi-auto-471020.st_ref_v2.dim_bu_rollup` bu
  ON e.businessUnitId = bu.businessUnitId
INNER JOIN `kpi-auto-471020.st_ref_v2.jobtype_lists` jt
  ON jt.jobTypeName = e.jobTypeName
  AND jt.list = 'DAILY_WBR_SALES_ALLOW'
WHERE bu.bu_group = 'SALES'
GROUP BY bu.bu_rollup, e.event_date;

-- ---------------------------------------------------------------------
-- kpi_booked_daily
-- SALES BUs; sum invoices (constrained to allowed estimate types via join)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.kpi_booked_daily` AS
SELECT
  bu.bu_rollup,
  i.event_date,
  SUM(i.total) AS total_booked
FROM `kpi-auto-471020.st_mart_v2.v_invoices_daily` i
INNER JOIN `kpi-auto-471020.st_ref_v2.dim_bu_rollup` bu
  ON i.businessUnitId = bu.businessUnitId
WHERE bu.bu_group = 'SALES'
GROUP BY bu.bu_rollup, i.event_date;

-- ---------------------------------------------------------------------
-- kpi_produced_daily
-- PRODUCTION BUs; jobs completed; EXCLUDE list;
-- produced = SUM(COALESCE(invoice_total_by_job, jobSubtotal))
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.kpi_produced_daily` AS
WITH job_invoices AS (
  SELECT
    j.businessUnitId,
    j.event_date,
    j.jobNumber,
    j.jobSubtotal,
    j.jobTypeName,
    SUM(i.total) AS invoice_total
  FROM `kpi-auto-471020.st_mart_v2.v_jobs_completed_daily` j
  LEFT JOIN `kpi-auto-471020.st_mart_v2.v_invoices_daily` i
    ON j.jobNumber = i.jobNumber
  WHERE NOT EXISTS (
    SELECT 1 FROM `kpi-auto-471020.st_ref_v2.jobtype_lists` jt
    WHERE jt.list = 'FOREMAN_PROD_EXCLUDE'
      AND j.jobTypeName = jt.jobTypeName
  )
  GROUP BY j.businessUnitId, j.event_date, j.jobNumber, j.jobSubtotal, j.jobTypeName
)

SELECT
  bu.bu_rollup,
  ji.event_date,
  SUM(COALESCE(ji.invoice_total, ji.jobSubtotal)) AS produced
FROM job_invoices ji
INNER JOIN `kpi-auto-471020.st_ref_v2.dim_bu_rollup` bu
  ON ji.businessUnitId = bu.businessUnitId
WHERE bu.bu_group = 'PRODUCTION'
GROUP BY bu.bu_rollup, ji.event_date;

-- ---------------------------------------------------------------------
-- kpi_gpm_daily
-- PRODUCTION; (produced − labor_cost)/produced; labor from payroll
-- Note: Materials cost not available, using labor only
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.kpi_gpm_daily` AS
WITH production_by_day AS (
  SELECT
    bu_rollup,
    event_date,
    produced
  FROM `kpi-auto-471020.st_mart_v2.kpi_produced_daily`
),

labor_cost_by_day AS (
  SELECT
    bu.bu_rollup,
    p.event_date,
    SUM(p.paid_amount) AS labor_cost
  FROM `kpi-auto-471020.st_mart_v2.v_payroll_daily` p
  INNER JOIN `kpi-auto-471020.st_ref_v2.dim_bu_rollup` bu
    ON p.businessUnitId = bu.businessUnitId
  WHERE bu.bu_group = 'PRODUCTION'
  GROUP BY bu.bu_rollup, p.event_date
)

SELECT
  COALESCE(prod.bu_rollup, labor.bu_rollup) AS bu_rollup,
  COALESCE(prod.event_date, labor.event_date) AS event_date,
  COALESCE(prod.produced, 0) AS produced,
  COALESCE(labor.labor_cost, 0) AS labor_cost,
  SAFE_DIVIDE(
    COALESCE(prod.produced, 0) - COALESCE(labor.labor_cost, 0),
    NULLIF(COALESCE(prod.produced, 0), 0)
  ) AS gpm_ratio
FROM production_by_day prod
FULL OUTER JOIN labor_cost_by_day labor
  USING (bu_rollup, event_date);

-- ---------------------------------------------------------------------
-- kpi_collected_daily
-- PRODUCTION; sum payments
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.kpi_collected_daily` AS
SELECT
  bu.bu_rollup,
  p.event_date,
  SUM(p.amount) AS collected
FROM `kpi-auto-471020.st_mart_v2.v_payments_daily` p
INNER JOIN `kpi-auto-471020.st_ref_v2.dim_bu_rollup` bu
  ON p.businessUnitId = bu.businessUnitId
WHERE bu.bu_group = 'PRODUCTION'
GROUP BY bu.bu_rollup, p.event_date;

-- ---------------------------------------------------------------------
-- kpi_warranty_pct_daily
-- PRODUCTION; warranty include list; ratio of warranty produced / produced
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.kpi_warranty_pct_daily` AS
WITH total_produced AS (
  SELECT
    bu_rollup,
    event_date,
    produced
  FROM `kpi-auto-471020.st_mart_v2.kpi_produced_daily`
),

warranty_produced AS (
  SELECT
    bu.bu_rollup,
    j.event_date,
    SUM(COALESCE(i.total, j.jobSubtotal)) AS warranty_amount
  FROM `kpi-auto-471020.st_mart_v2.v_jobs_completed_daily` j
  INNER JOIN `kpi-auto-471020.st_ref_v2.dim_bu_rollup` bu
    ON j.businessUnitId = bu.businessUnitId
  INNER JOIN `kpi-auto-471020.st_ref_v2.jobtype_lists` jt
    ON jt.jobTypeName = j.jobTypeName
    AND jt.list = 'WARRANTY_INCLUDE'
  LEFT JOIN `kpi-auto-471020.st_mart_v2.v_invoices_daily` i
    ON j.jobNumber = i.jobNumber
  WHERE bu.bu_group = 'PRODUCTION'
  GROUP BY bu.bu_rollup, j.event_date
)

SELECT
  COALESCE(tp.bu_rollup, wp.bu_rollup) AS bu_rollup,
  COALESCE(tp.event_date, wp.event_date) AS event_date,
  COALESCE(wp.warranty_amount, 0) AS warranty_amount,
  COALESCE(tp.produced, 0) AS total_produced,
  SAFE_DIVIDE(
    COALESCE(wp.warranty_amount, 0),
    NULLIF(COALESCE(tp.produced, 0), 0)
  ) AS warranty_pct
FROM total_produced tp
FULL OUTER JOIN warranty_produced wp
  USING (bu_rollup, event_date);

-- ---------------------------------------------------------------------
-- kpi_future_bookings_daily
-- PRODUCTION; scheduledStartOn in current Monday → +1y; EXCLUDE list
-- Note: Using completedOn as proxy for scheduledStartOn
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.kpi_future_bookings_daily` AS
WITH current_monday AS (
  SELECT DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) AS week_start
)

SELECT
  bu.bu_rollup,
  CURRENT_DATE() AS snapshot_date,
  SUM(j.jobSubtotal) AS future_bookings
FROM `kpi-auto-471020.st_mart_v2.v_jobs_completed_daily` j
INNER JOIN `kpi-auto-471020.st_ref_v2.dim_bu_rollup` bu
  ON j.businessUnitId = bu.businessUnitId
CROSS JOIN current_monday cm
WHERE bu.bu_group = 'PRODUCTION'
  AND DATE(j.scheduledStartOn) >= cm.week_start
  AND DATE(j.scheduledStartOn) < DATE_ADD(cm.week_start, INTERVAL 1 YEAR)
  AND NOT EXISTS (
    SELECT 1 FROM `kpi-auto-471020.st_ref_v2.jobtype_lists` jt
    WHERE jt.list = 'FOREMAN_PROD_EXCLUDE'
      AND j.jobTypeName = jt.jobTypeName
  )
GROUP BY bu.bu_rollup;

-- ---------------------------------------------------------------------
-- kpi_outstanding_ar_daily
-- PRODUCTION; ar_balance >= 10 and locationName != 'Name'
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.kpi_outstanding_ar_daily` AS
SELECT
  bu.bu_rollup,
  CURRENT_DATE() AS snapshot_date,
  SUM(b.ar_balance) AS outstanding_ar,
  COUNT(*) AS invoice_count
FROM `kpi-auto-471020.st_mart_v2.v_invoice_balances` b
INNER JOIN `kpi-auto-471020.st_ref_v2.dim_bu_rollup` bu
  ON b.businessUnitId = bu.businessUnitId
WHERE bu.bu_group = 'PRODUCTION'
  AND b.ar_balance >= 10
  AND COALESCE(b.locationName, '') != 'Name'
GROUP BY bu.bu_rollup;

-- ---------------------------------------------------------------------
-- kpi_estimates_scheduled_daily (NEW KPI)
-- Count jobs with jobTypeName ILIKE '%estimate%' by scheduledStartOn
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.kpi_estimates_scheduled_daily` AS
SELECT
  bu.bu_rollup,
  DATE(j.scheduledStartOn) AS event_date,
  COUNT(*) AS estimates_scheduled_count
FROM `kpi-auto-471020.st_mart_v2.v_jobs_completed_daily` j
INNER JOIN `kpi-auto-471020.st_ref_v2.dim_bu_rollup` bu
  ON j.businessUnitId = bu.businessUnitId
WHERE LOWER(COALESCE(j.jobTypeName, '')) LIKE '%estimate%'
  AND j.scheduledStartOn IS NOT NULL
GROUP BY bu.bu_rollup, event_date;

-- ---------------------------------------------------------------------
-- kpi_estimates_scheduled_weekly (NEW KPI - Weekly Rollup)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.kpi_estimates_scheduled_weekly` AS
SELECT
  bu.bu_rollup,
  DATE_TRUNC(DATE(j.scheduledStartOn), WEEK(MONDAY)) AS week_start,
  COUNT(*) AS estimates_scheduled_count
FROM `kpi-auto-471020.st_mart_v2.v_jobs_completed_daily` j
INNER JOIN `kpi-auto-471020.st_ref_v2.dim_bu_rollup` bu
  ON j.businessUnitId = bu.businessUnitId
WHERE LOWER(COALESCE(j.jobTypeName, '')) LIKE '%estimate%'
  AND j.scheduledStartOn IS NOT NULL
GROUP BY bu.bu_rollup, week_start;

-- =====================================================================
-- STEP 6: WEEKLY ROLLUP VIEW
-- =====================================================================

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.kpi_weekly_by_bu` AS
WITH weeks AS (
  -- Generate all weeks that have any activity
  SELECT DISTINCT DATE_TRUNC(event_date, WEEK(MONDAY)) AS week_start
  FROM (
    SELECT event_date FROM `kpi-auto-471020.st_mart_v2.kpi_leads_daily`
    UNION DISTINCT
    SELECT event_date FROM `kpi-auto-471020.st_mart_v2.kpi_estimates_daily`
    UNION DISTINCT
    SELECT event_date FROM `kpi-auto-471020.st_mart_v2.kpi_booked_daily`
    UNION DISTINCT
    SELECT event_date FROM `kpi-auto-471020.st_mart_v2.kpi_produced_daily`
    UNION DISTINCT
    SELECT event_date FROM `kpi-auto-471020.st_mart_v2.kpi_collected_daily`
  )
),

bu_rollups AS (
  SELECT DISTINCT bu_rollup
  FROM `kpi-auto-471020.st_ref_v2.dim_bu_rollup`
),

week_bu_spine AS (
  SELECT w.week_start, bu.bu_rollup
  FROM weeks w
  CROSS JOIN bu_rollups bu
),

leads_weekly AS (
  SELECT
    bu_rollup,
    DATE_TRUNC(event_date, WEEK(MONDAY)) AS week_start,
    SUM(leads_count) AS leads
  FROM `kpi-auto-471020.st_mart_v2.kpi_leads_daily`
  GROUP BY bu_rollup, week_start
),

estimates_weekly AS (
  SELECT
    bu_rollup,
    DATE_TRUNC(event_date, WEEK(MONDAY)) AS week_start,
    SUM(estimates_count) AS estimates
  FROM `kpi-auto-471020.st_mart_v2.kpi_estimates_daily`
  GROUP BY bu_rollup, week_start
),

booked_weekly AS (
  SELECT
    bu_rollup,
    DATE_TRUNC(event_date, WEEK(MONDAY)) AS week_start,
    SUM(total_booked) AS total_booked
  FROM `kpi-auto-471020.st_mart_v2.kpi_booked_daily`
  GROUP BY bu_rollup, week_start
),

produced_weekly AS (
  SELECT
    bu_rollup,
    DATE_TRUNC(event_date, WEEK(MONDAY)) AS week_start,
    SUM(produced) AS produced
  FROM `kpi-auto-471020.st_mart_v2.kpi_produced_daily`
  GROUP BY bu_rollup, week_start
),

gpm_weekly AS (
  SELECT
    bu_rollup,
    DATE_TRUNC(event_date, WEEK(MONDAY)) AS week_start,
    SUM(produced) AS total_produced,
    SUM(labor_cost) AS total_labor,
    SAFE_DIVIDE(
      SUM(produced) - SUM(labor_cost),
      NULLIF(SUM(produced), 0)
    ) AS gpm_ratio
  FROM `kpi-auto-471020.st_mart_v2.kpi_gpm_daily`
  GROUP BY bu_rollup, week_start
),

collected_weekly AS (
  SELECT
    bu_rollup,
    DATE_TRUNC(event_date, WEEK(MONDAY)) AS week_start,
    SUM(collected) AS collected
  FROM `kpi-auto-471020.st_mart_v2.kpi_collected_daily`
  GROUP BY bu_rollup, week_start
),

warranty_weekly AS (
  SELECT
    bu_rollup,
    DATE_TRUNC(event_date, WEEK(MONDAY)) AS week_start,
    SAFE_DIVIDE(
      SUM(warranty_amount),
      NULLIF(SUM(total_produced), 0)
    ) AS warranty_pct
  FROM `kpi-auto-471020.st_mart_v2.kpi_warranty_pct_daily`
  GROUP BY bu_rollup, week_start
),

-- Outstanding AR and Future Bookings are snapshots, take latest per week
ar_weekly AS (
  SELECT DISTINCT
    bu_rollup,
    DATE_TRUNC(snapshot_date, WEEK(MONDAY)) AS week_start,
    LAST_VALUE(outstanding_ar) OVER (
      PARTITION BY bu_rollup, DATE_TRUNC(snapshot_date, WEEK(MONDAY))
      ORDER BY snapshot_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS outstanding_ar
  FROM `kpi-auto-471020.st_mart_v2.kpi_outstanding_ar_daily`
),

future_bookings_weekly AS (
  SELECT DISTINCT
    bu_rollup,
    DATE_TRUNC(snapshot_date, WEEK(MONDAY)) AS week_start,
    LAST_VALUE(future_bookings) OVER (
      PARTITION BY bu_rollup, DATE_TRUNC(snapshot_date, WEEK(MONDAY))
      ORDER BY snapshot_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS future_bookings
  FROM `kpi-auto-471020.st_mart_v2.kpi_future_bookings_daily`
),

estimates_scheduled_weekly_agg AS (
  SELECT
    bu_rollup,
    week_start,
    SUM(estimates_scheduled_count) AS estimates_scheduled
  FROM `kpi-auto-471020.st_mart_v2.kpi_estimates_scheduled_weekly`
  GROUP BY bu_rollup, week_start
)

SELECT
  spine.bu_rollup,
  spine.week_start,
  COALESCE(l.leads, 0) AS leads,
  COALESCE(e.estimates, 0) AS estimates,
  COALESCE(b.total_booked, 0) AS total_booked,
  COALESCE(p.produced, 0) AS produced,
  COALESCE(g.gpm_ratio, 0) AS gpm_ratio,
  COALESCE(c.collected, 0) AS collected,
  COALESCE(w.warranty_pct, 0) AS warranty_pct,
  COALESCE(ar.outstanding_ar, 0) AS outstanding_ar,
  COALESCE(fb.future_bookings, 0) AS future_bookings,
  COALESCE(es.estimates_scheduled, 0) AS estimates_scheduled
FROM week_bu_spine spine
LEFT JOIN leads_weekly l USING (bu_rollup, week_start)
LEFT JOIN estimates_weekly e USING (bu_rollup, week_start)
LEFT JOIN booked_weekly b USING (bu_rollup, week_start)
LEFT JOIN produced_weekly p USING (bu_rollup, week_start)
LEFT JOIN gpm_weekly g USING (bu_rollup, week_start)
LEFT JOIN collected_weekly c USING (bu_rollup, week_start)
LEFT JOIN warranty_weekly w USING (bu_rollup, week_start)
LEFT JOIN ar_weekly ar USING (bu_rollup, week_start)
LEFT JOIN future_bookings_weekly fb USING (bu_rollup, week_start)
LEFT JOIN estimates_scheduled_weekly_agg es USING (bu_rollup, week_start)
ORDER BY spine.week_start DESC, spine.bu_rollup;

-- =====================================================================
-- COMPLETION MESSAGE
-- =====================================================================

SELECT '✅ KPI Marts Created Successfully!' AS status,
       'Query kpi-auto-471020.st_mart_v2.kpi_weekly_by_bu to see weekly rollups' AS next_step;
