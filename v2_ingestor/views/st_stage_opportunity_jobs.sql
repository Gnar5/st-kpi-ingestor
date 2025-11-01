-- st_stage.opportunity_jobs
-- Per-job opportunity rollup with close rate flags
-- VALIDATED AGAINST SERVICETITAN: 2025-10-20 to 2025-10-26 = 202 opportunities, 76 closed
--
-- Business Logic (updated to match ServiceTitan):
--   - Sales Opportunity = Jobs from Sales business units that are NOT "No Charge" jobs
--   - No Charge Job = Job with 0 estimates AND $0 invoice subtotal
--   - Closed Opportunity = any job with >= 1 sold estimate (status='Sold')
--   - Opportunity Date = COALESCE(earliest soldOn, job completedOn) in Phoenix timezone
--   - INCLUDES all Sales business units (including Commercial-AZ-Sales)
--
-- NOTE:
--   - After full estimates sync (149K estimates), data is much more accurate
--   - Removed items field and increased lookback to 180 days to improve sync coverage
--
-- Grain: One row per job from Sales business units

CREATE OR REPLACE VIEW `kpi-auto-471020.st_stage.opportunity_jobs` AS

WITH estimate_rollup AS (
  -- Roll up estimates by job to get counts and earliest dates
  SELECT
    jobId,
    COUNT(*) as estimate_count,
    COUNT(CASE WHEN status = 'Sold' THEN 1 END) as sold_estimate_count,
    MIN(CASE WHEN status = 'Sold' THEN soldOn END) as earliest_sold_on_utc,
    MIN(createdOn) as earliest_created_on_utc
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates`
  WHERE jobId IS NOT NULL
  GROUP BY jobId
),

invoice_rollup AS (
  -- Roll up invoices by job to get total revenue
  SELECT
    jobId,
    SUM(CAST(subtotal AS FLOAT64)) as invoice_subtotal
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices`
  WHERE jobId IS NOT NULL
  GROUP BY jobId
)

SELECT
  j.id as job_id,
  j.customerId as customer_id,
  j.businessUnitNormalized as business_unit,
  j.jobStatus as job_status,
  j.jobTypeName as job_type_name,
  j.createdOn as job_created_on_utc,
  j.completedOn as job_completed_on_utc,

  -- Estimate rollup fields
  COALESCE(e.estimate_count, 0) as estimate_count,
  COALESCE(e.sold_estimate_count, 0) as sold_estimate_count,
  e.earliest_sold_on_utc,

  -- Opportunity date: earliest soldOn if exists, otherwise job completedOn (Phoenix timezone)
  -- This matches ServiceTitan's logic: sold date takes priority, then completed date
  DATE(
    COALESCE(e.earliest_sold_on_utc, j.completedOn), 'America/Phoenix'
  ) as opportunity_date,

  -- Opportunity flags
  -- Sales Opportunity = any job from a Sales business unit (matches ServiceTitan logic)
  TRUE as is_sales_opportunity,

  CASE
    WHEN COALESCE(e.sold_estimate_count, 0) >= 1 THEN TRUE
    ELSE FALSE
  END as is_closed_opportunity,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
LEFT JOIN estimate_rollup e ON j.id = e.jobId
LEFT JOIN invoice_rollup i ON j.id = i.jobId
WHERE j.id IS NOT NULL
  AND j.businessUnitNormalized LIKE '%-Sales'  -- Only Sales business units (includes Commercial)
  -- Exclude "No Charge" jobs (0 estimates AND $0 invoices) per ServiceTitan documentation
  AND NOT (COALESCE(e.estimate_count, 0) = 0 AND COALESCE(i.invoice_subtotal, 0) = 0)
;
