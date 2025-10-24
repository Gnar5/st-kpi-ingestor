-- st_stage.leads_jobs
-- Jobs filtered by estimate job type allowlist for lead counting
--
-- Business Logic:
--   - Lead = unique customer with at least one eligible job created in period
--   - Only jobs with jobTypeId in estimate allowlist (19 specific IDs)
--   - Date based on job createdOn converted to America/Phoenix
--
-- Grain: One row per job (jobId) that matches allowlist

CREATE OR REPLACE VIEW `kpi-auto-471020.st_stage.leads_jobs` AS

SELECT
  j.id as job_id,
  j.customerId as customer_id,
  j.businessUnitId as business_unit_id,
  j.jobTypeId as job_type_id,
  j.jobStatus as job_status,

  -- Original UTC timestamps
  j.createdOn as job_created_on_utc,
  j.completedOn as job_completed_on_utc,

  -- Lead date: createdOn in Phoenix timezone
  DATE(j.createdOn, 'America/Phoenix') as lead_date,

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
AND j.customerId IS NOT NULL
AND j.createdOn IS NOT NULL
;
