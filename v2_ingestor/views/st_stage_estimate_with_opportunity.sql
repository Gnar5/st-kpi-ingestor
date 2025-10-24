-- st_stage.estimate_with_opportunity
-- Estimates enriched with job-level opportunity information
--
-- Business Logic:
--   - Each estimate tagged with its job's opportunity status
--   - Useful for drill-down analysis from opportunity metrics
--
-- Grain: One row per estimate (estimate_id)

CREATE OR REPLACE VIEW `kpi-auto-471020.st_stage.estimate_with_opportunity` AS

SELECT
  e.id as estimate_id,
  e.jobId as job_id,
  e.estimateNumber,
  e.status as estimate_status,
  e.name as estimate_name,
  e.createdOn as estimate_created_on_utc,
  e.soldOn as estimate_sold_on_utc,
  e.soldById as sold_by_id,
  e.subtotal,
  e.totalTax as total_tax,
  e.total,

  -- Job-level opportunity fields
  o.customer_id,
  o.business_unit_id,
  o.job_status,
  o.opportunity_date,
  o.is_sales_opportunity,
  o.is_closed_opportunity,
  o.estimate_count as job_estimate_count,
  o.sold_estimate_count as job_sold_estimate_count,

  -- Is this estimate the winning sold estimate?
  CASE
    WHEN e.status = 'Sold' AND e.soldOn = o.earliest_sold_on_utc THEN TRUE
    ELSE FALSE
  END as is_winning_estimate,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
INNER JOIN `kpi-auto-471020.st_stage.opportunity_jobs` o
  ON e.jobId = o.job_id

WHERE e.id IS NOT NULL
;
