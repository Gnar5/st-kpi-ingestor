-- st_stage.sold_estimates
-- Estimates filtered for total booked revenue counting
-- VALIDATED: Matches daily_kpis.total_booked calculation
--
-- Business Logic:
--   - Total Booked = revenue from sold estimates
--   - Filter: estimate status = 'Sold'
--   - Date: estimate soldOn in America/Phoenix timezone
--   - Amount: estimate total (or subtotal if total is null)
--
-- Grain: One row per sold estimate

CREATE OR REPLACE VIEW `kpi-auto-471020.st_stage.sold_estimates` AS

SELECT
  e.id as estimate_id,
  e.jobId as job_id,
  j.customerId as customer_id,
  j.businessUnitNormalized as business_unit,
  e.status as estimate_status,

  -- Revenue amount: prefer total, fallback to subtotal
  COALESCE(e.total, e.subTotal) as revenue_amount,

  -- Original UTC timestamps
  e.createdOn as estimate_created_on_utc,
  e.soldOn as estimate_sold_on_utc,

  -- Sold date: soldOn in Phoenix timezone
  DATE(e.soldOn, 'America/Phoenix') as sold_date,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j
  ON e.jobId = j.id

WHERE
  -- Filter: sold estimates only
  e.status = 'Sold'

  -- Must have a sold date
  AND e.soldOn IS NOT NULL

  -- Basic nullability checks
  AND e.id IS NOT NULL
  AND e.jobId IS NOT NULL
;
