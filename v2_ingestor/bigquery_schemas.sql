-- ============================================
-- ServiceTitan v2 Ingestor - BigQuery DDL
-- ============================================
-- This file contains table definitions for all entities
-- Tables are partitioned by modifiedOn and clustered for optimal query performance

-- ============================================
-- 1. CREATE DATASETS
-- ============================================

CREATE SCHEMA IF NOT EXISTS `kpi-auto-471020.st_raw_v2`
OPTIONS (
  description = 'Raw data from ServiceTitan entity APIs (v2)',
  location = 'US'
);

CREATE SCHEMA IF NOT EXISTS `kpi-auto-471020.st_stage_v2`
OPTIONS (
  description = 'Staged/transformed data for analytics (v2)',
  location = 'US'
);

CREATE SCHEMA IF NOT EXISTS `kpi-auto-471020.st_mart_v2`
OPTIONS (
  description = 'Business-ready data marts (v2)',
  location = 'US'
);

CREATE SCHEMA IF NOT EXISTS `kpi-auto-471020.st_logs_v2`
OPTIONS (
  description = 'Ingestion logs and metadata (v2)',
  location = 'US'
);

-- ============================================
-- 2. LOGGING TABLES
-- ============================================

-- Ingestion run logs
CREATE TABLE IF NOT EXISTS `kpi-auto-471020.st_logs_v2.ingestion_logs` (
  entity_type STRING NOT NULL,
  run_id STRING NOT NULL,
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP,
  status STRING NOT NULL,
  records_fetched INT64,
  records_inserted INT64,
  duration_ms INT64,
  error_message STRING,
  metadata JSON
)
PARTITION BY DATE(start_time)
CLUSTER BY entity_type, status
OPTIONS (
  description = 'Ingestion run logs with performance metrics',
  partition_expiration_days = 365
);

-- Sync state tracking
CREATE TABLE IF NOT EXISTS `kpi-auto-471020.st_logs_v2.sync_state` (
  entity_type STRING NOT NULL,
  last_sync_time TIMESTAMP NOT NULL,
  last_sync_status STRING,
  records_processed INT64,
  updated_at TIMESTAMP NOT NULL
)
OPTIONS (
  description = 'Last sync timestamps for incremental loading'
);

-- ============================================
-- 3. RAW ENTITY TABLES
-- ============================================

-- Jobs
CREATE TABLE IF NOT EXISTS `kpi-auto-471020.st_raw_v2.raw_jobs` (
  id INT64 NOT NULL,
  jobNumber STRING,
  projectId INT64,
  customerId INT64,
  locationId INT64,
  jobStatus STRING,
  completedOn TIMESTAMP,
  businessUnitId INT64,
  jobTypeId INT64,
  priority STRING,
  campaignId INT64,
  summary STRING,
  customFields JSON,
  createdOn TIMESTAMP,
  createdById INT64,
  modifiedOn TIMESTAMP,
  tagTypeIds JSON,
  leadCallId INT64,
  bookingId INT64,
  soldById INT64,
  _ingested_at TIMESTAMP NOT NULL,
  _ingestion_source STRING NOT NULL
)
PARTITION BY DATE(modifiedOn)
CLUSTER BY businessUnitId, jobStatus
OPTIONS (
  description = 'Job data from ServiceTitan JPM API'
);

-- Invoices
CREATE TABLE IF NOT EXISTS `kpi-auto-471020.st_raw_v2.raw_invoices` (
  id INT64 NOT NULL,
  syncStatus STRING,
  summary STRING,
  referenceNumber STRING,
  invoiceDate TIMESTAMP,
  dueDate TIMESTAMP,
  subTotal FLOAT64,
  salesTax FLOAT64,
  total FLOAT64,
  balance FLOAT64,
  invoiceTypeId INT64,
  jobId INT64,
  projectId INT64,
  businessUnitId INT64,
  locationId INT64,
  customerId INT64,
  depositedOn TIMESTAMP,
  createdOn TIMESTAMP,
  modifiedOn TIMESTAMP,
  adjustmentToId INT64,
  status STRING,
  employeeId INT64,
  commissionEligibilityDate TIMESTAMP,
  items JSON,
  customFields JSON,
  _ingested_at TIMESTAMP NOT NULL,
  _ingestion_source STRING NOT NULL
)
PARTITION BY DATE(modifiedOn)
CLUSTER BY businessUnitId, jobId, status
OPTIONS (
  description = 'Invoice data from ServiceTitan Accounting API'
);

-- Estimates
CREATE TABLE IF NOT EXISTS `kpi-auto-471020.st_raw_v2.raw_estimates` (
  id INT64 NOT NULL,
  jobId INT64,
  projectId INT64,
  locationId INT64,
  customerId INT64,
  name STRING,
  jobNumber STRING,
  status STRING,
  summary STRING,
  createdOn TIMESTAMP,
  modifiedOn TIMESTAMP,
  soldOn TIMESTAMP,
  soldById INT64,
  estimateNumber STRING,
  businessUnitId INT64,
  items JSON,
  subtotal FLOAT64,
  totalTax FLOAT64,
  total FLOAT64,
  _ingested_at TIMESTAMP NOT NULL,
  _ingestion_source STRING NOT NULL
)
PARTITION BY DATE(modifiedOn)
CLUSTER BY businessUnitId, jobId, status
OPTIONS (
  description = 'Estimate data from ServiceTitan Sales API'
);

-- Payments
CREATE TABLE IF NOT EXISTS `kpi-auto-471020.st_raw_v2.raw_payments` (
  id INT64 NOT NULL,
  invoiceId INT64,
  amount FLOAT64,
  paymentTypeId INT64,
  status STRING,
  memo STRING,
  referenceNumber STRING,
  unappliedAmount FLOAT64,
  createdOn TIMESTAMP,
  modifiedOn TIMESTAMP,
  businessUnitId INT64,
  batchId INT64,
  _ingested_at TIMESTAMP NOT NULL,
  _ingestion_source STRING NOT NULL
)
PARTITION BY DATE(modifiedOn)
CLUSTER BY invoiceId, paymentTypeId, status
OPTIONS (
  description = 'Payment data from ServiceTitan Accounting API'
);

-- Payroll
CREATE TABLE IF NOT EXISTS `kpi-auto-471020.st_raw_v2.raw_payroll` (
  id INT64 NOT NULL,
  employeeId INT64,
  jobId INT64,
  invoiceId INT64,
  rate FLOAT64,
  hours FLOAT64,
  amount FLOAT64,
  paidDate TIMESTAMP,
  description STRING,
  payrollTypeId INT64,
  createdOn TIMESTAMP,
  modifiedOn TIMESTAMP,
  businessUnitId INT64,
  _ingested_at TIMESTAMP NOT NULL,
  _ingestion_source STRING NOT NULL
)
PARTITION BY DATE(modifiedOn)
CLUSTER BY employeeId, paidDate
OPTIONS (
  description = 'Payroll data from ServiceTitan Payroll API'
);

-- Customers
CREATE TABLE IF NOT EXISTS `kpi-auto-471020.st_raw_v2.raw_customers` (
  id INT64 NOT NULL,
  active BOOL,
  name STRING,
  type STRING,
  address JSON,
  email STRING,
  phoneNumber STRING,
  balance FLOAT64,
  customFields JSON,
  createdOn TIMESTAMP,
  createdById INT64,
  modifiedOn TIMESTAMP,
  mergedToId INT64,
  _ingested_at TIMESTAMP NOT NULL,
  _ingestion_source STRING NOT NULL
)
PARTITION BY DATE(modifiedOn)
CLUSTER BY type, active
OPTIONS (
  description = 'Customer data from ServiceTitan CRM API'
);

-- Locations
CREATE TABLE IF NOT EXISTS `kpi-auto-471020.st_raw_v2.raw_locations` (
  id INT64 NOT NULL,
  customerId INT64,
  active BOOL,
  name STRING,
  address JSON,
  taxZoneId INT64,
  zoneId INT64,
  createdOn TIMESTAMP,
  modifiedOn TIMESTAMP,
  customFields JSON,
  _ingested_at TIMESTAMP NOT NULL,
  _ingestion_source STRING NOT NULL
)
PARTITION BY DATE(modifiedOn)
CLUSTER BY customerId, active
OPTIONS (
  description = 'Location data from ServiceTitan CRM API'
);

-- Campaigns
CREATE TABLE IF NOT EXISTS `kpi-auto-471020.st_raw_v2.raw_campaigns` (
  id INT64 NOT NULL,
  active BOOL,
  name STRING,
  categoryId INT64,
  category STRING,
  createdOn TIMESTAMP,
  modifiedOn TIMESTAMP,
  _ingested_at TIMESTAMP NOT NULL,
  _ingestion_source STRING NOT NULL
)
PARTITION BY DATE(modifiedOn)
CLUSTER BY active, categoryId
OPTIONS (
  description = 'Campaign data from ServiceTitan Marketing API'
);

-- ============================================
-- 4. STAGE VIEWS (Examples)
-- ============================================

-- Jobs with denormalized customer info
CREATE OR REPLACE VIEW `kpi-auto-471020.st_stage_v2.jobs_enriched` AS
SELECT
  j.*,
  c.name AS customer_name,
  c.type AS customer_type,
  l.address AS location_address
FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_customers` c ON j.customerId = c.id
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_locations` l ON j.locationId = l.id;

-- Invoice summary by business unit
CREATE OR REPLACE VIEW `kpi-auto-471020.st_stage_v2.invoices_by_bu` AS
SELECT
  businessUnitId,
  DATE(invoiceDate) AS invoice_date,
  status,
  COUNT(*) AS invoice_count,
  SUM(total) AS total_amount,
  SUM(balance) AS total_balance
FROM `kpi-auto-471020.st_raw_v2.raw_invoices`
GROUP BY businessUnitId, invoice_date, status;

-- ============================================
-- 5. MART TABLES (Examples)
-- ============================================

-- Daily revenue by business unit
CREATE TABLE IF NOT EXISTS `kpi-auto-471020.st_mart_v2.daily_revenue_by_bu` (
  date DATE NOT NULL,
  business_unit_id INT64 NOT NULL,
  total_invoiced FLOAT64,
  total_collected FLOAT64,
  outstanding_balance FLOAT64,
  invoice_count INT64,
  updated_at TIMESTAMP NOT NULL
)
PARTITION BY date
CLUSTER BY business_unit_id
OPTIONS (
  description = 'Daily revenue metrics by business unit'
);

-- Job completion metrics
CREATE TABLE IF NOT EXISTS `kpi-auto-471020.st_mart_v2.job_completion_metrics` (
  date DATE NOT NULL,
  business_unit_id INT64 NOT NULL,
  jobs_completed INT64,
  jobs_in_progress INT64,
  avg_completion_days FLOAT64,
  revenue_from_completed FLOAT64,
  updated_at TIMESTAMP NOT NULL
)
PARTITION BY date
CLUSTER BY business_unit_id
OPTIONS (
  description = 'Job completion KPIs'
);

-- ============================================
-- 6. USEFUL QUERIES
-- ============================================

-- Get latest sync status for all entities
-- SELECT entity_type, last_sync_time, last_sync_status, records_processed
-- FROM `kpi-auto-471020.st_logs_v2.sync_state`
-- ORDER BY updated_at DESC;

-- Get recent ingestion runs
-- SELECT entity_type, run_id, start_time, end_time, status, records_inserted, duration_ms
-- FROM `kpi-auto-471020.st_logs_v2.ingestion_logs`
-- WHERE DATE(start_time) = CURRENT_DATE()
-- ORDER BY start_time DESC;

-- Get failed runs in last 24 hours
-- SELECT entity_type, run_id, start_time, error_message
-- FROM `kpi-auto-471020.st_logs_v2.ingestion_logs`
-- WHERE status = 'failed'
--   AND start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
-- ORDER BY start_time DESC;
