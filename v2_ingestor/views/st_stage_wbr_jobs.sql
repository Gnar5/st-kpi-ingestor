-- st_stage.wbr_jobs
-- Jobs filtered to match ServiceTitan "Daily WBR C/R" report logic
--
-- Purpose: This view replicates the EXACT filtering logic used in ServiceTitan's
--          "Daily WBR C/R" (Weekly Business Review - Close Rate) report.
--          This report is the source of truth for:
--          - # Completed Estimates
--          - Total Booked (Total Sales)
--          - Close Rate (Success Rate)
--
-- Business Logic (from ServiceTitan report definition):
--   - Date: Job Completion Date
--   - Job Types: MUST be one of 19 specific estimate types (see below)
--   - Business Units: Sales BUs only
--   - Excludes: None (job types list is already specific)
--
-- Grain: One row per job that appears in the WBR report

CREATE OR REPLACE VIEW `kpi-auto-471020.st_stage.wbr_jobs` AS

SELECT
  j.id as job_id,
  j.customerId as customer_id,
  j.businessUnitNormalized as business_unit,
  j.jobTypeName as job_type_name,
  j.jobStatus as job_status,

  -- Date fields
  j.createdOn as job_created_on_utc,
  j.completedOn as job_completed_on_utc,
  DATE(j.completedOn, 'America/Phoenix') as completion_date,

  -- Estimate metrics (join to get estimate totals)
  e.total_estimates,
  e.sold_estimates,
  e.total_estimate_amount,
  e.total_sold_amount,

  -- Close rate flag
  CASE WHEN e.sold_estimates > 0 THEN TRUE ELSE FALSE END as is_closed,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j

-- Join estimate summary for this job
LEFT JOIN (
  SELECT
    jobId,
    COUNT(*) as total_estimates,
    COUNT(CASE WHEN soldOn IS NOT NULL THEN 1 END) as sold_estimates,
    SUM(COALESCE(subtotal, 0)) as total_estimate_amount,
    SUM(CASE WHEN soldOn IS NOT NULL THEN COALESCE(subtotal, 0) ELSE 0 END) as total_sold_amount
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates`
  WHERE jobId IS NOT NULL
  GROUP BY jobId
) e ON j.id = e.jobId

WHERE
  -- Must have completed status
  j.jobStatus = 'Completed'

  -- Must have completion date
  AND j.completedOn IS NOT NULL

  -- CRITICAL: Must be one of the 19 WBR job types
  -- This list comes directly from ServiceTitan "Daily WBR C/R" report filters
  -- Note: Added "Estimate- Cabinets" as name variation (hyphen-space vs comma)
  AND j.jobTypeName IN (
    'ESTIMATE- WINDOW WASHING',
    'Estimate, Cabinets',
    'Estimate- Cabinets',  -- Name variation found in database
    'Estimate- Exterior PLUS Int Cabinets',
    'Estimate- Interior PLUS Cabinets',
    'ESTIMATE -RES-EXT-PRE 1960',
    'ESTIMATE -RES-INT/EXT-PRE 1960',
    'ESTIMATE-COMM-EXT',
    'ESTIMATE-COMM-EXT/INT',
    'ESTIMATE-COMM-INT',
    'ESTIMATE-COMM-PLANBID',
    'ESTIMATE-COMM-Striping',
    'ESTIMATE-FLOOR COATING-EPOXY',
    'ESTIMATE-FLOOR COATING-H&C Coatings',
    'ESTIMATE-POPCORN',
    'ESTIMATE-RES-EXT',
    'ESTIMATE-RES-EXT/INT',
    'ESTIMATE-RES-HOA',
    'ESTIMATE-RES-INT',
    'Estimate-RES-INT/EXT Plus Cabinets'
  )

  -- Only Sales business units (WBR is a sales report)
  AND j.businessUnitNormalized LIKE '%-Sales'

  -- NOTE: Removed estimate requirement - ServiceTitan includes jobs even without estimate records
  -- as long as they have the correct job type name (which implies they should be estimates)
;
