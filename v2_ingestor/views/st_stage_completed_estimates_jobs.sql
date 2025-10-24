-- st_stage.completed_estimates_jobs
-- Completed jobs from estimate job type allowlist
--
-- Business Logic:
--   - Completed Estimate = job with jobTypeId in allowlist AND completedOn IS NOT NULL
--   - Each completed job represents one completed estimate
--   - Date based on job completedOn in America/Phoenix timezone
--
-- Grain: One row per completed job (jobId)

CREATE OR REPLACE VIEW `kpi-auto-471020.st_stage.completed_estimates_jobs` AS

SELECT
  j.id as job_id,
  j.customerId as customer_id,
  j.businessUnitId as business_unit_id,
  j.jobTypeId as job_type_id,
  j.jobStatus as job_status,

  -- Original UTC timestamps
  j.createdOn as job_created_on_utc,
  j.completedOn as job_completed_on_utc,

  -- Completed date: completedOn in Phoenix timezone
  DATE(j.completedOn, 'America/Phoenix') as completed_date,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j

WHERE j.jobTypeId IN (
  -- Estimate job type allowlist (19 jobTypeIds)
  705557,      -- ESTIMATE-COMM-INT
  705812,      -- ESTIMATE-RES-INT
  727444,      -- ESTIMATE-COMM-EXT
  727572,      -- ESTIMATE-RES-EXT
  7761171,     -- ESTIMATE-FLOOR COATINGS-EPOXY
  25643501,
  25640548,
  40084045,
  40091077,
  40528050,
  52632595,
  53425776,
  53419951,
  53417012,
  66527167,
  80656917,
  142931404,
  144397449,
  365792375
)
AND j.id IS NOT NULL
AND j.completedOn IS NOT NULL  -- Only completed jobs
;
