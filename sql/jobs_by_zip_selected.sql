-- Jobs by ZIP for a specific set of job IDs
-- Project: kpi-auto-471020
-- Datasets: st_raw_v2 (raw), st_stage_v2 (stage), st_mart_v2 (mart)

-- 0) Ensure ZIP normalization view exists (safe to re-run)
CREATE OR REPLACE VIEW `kpi-auto-471020.st_stage_v2.locations_zip_norm` AS
WITH src AS (
  SELECT
    id AS locationId,
    LPAD(
      COALESCE(
        REGEXP_EXTRACT(JSON_VALUE(address, '$.postalCode'), r'(\d{5})'),
        REGEXP_EXTRACT(JSON_VALUE(address, '$.zip'), r'(\d{5})'),
        REGEXP_EXTRACT(TO_JSON_STRING(address), r'(\d{5})')
      ),
      5,
      '0'
    ) AS zip5,
    SAFE_CAST(JSON_VALUE(address, '$.longitude') AS FLOAT64) AS lon,
    SAFE_CAST(JSON_VALUE(address, '$.latitude') AS FLOAT64) AS lat
  FROM `kpi-auto-471020.st_raw_v2.raw_locations`
)
SELECT
  locationId,
  zip5,
  IF(lon IS NOT NULL AND lat IS NOT NULL, ST_GEOGPOINT(lon, lat), NULL) AS geo_point
FROM src
WHERE zip5 IS NOT NULL;

-- 1) Specific job IDs (inline array literal to avoid DECLARE)
--    Note: We can also switch to a CTE if preferred.
--    Example CTE version:
--    WITH selected_ids AS (SELECT id FROM UNNEST([1,2,3]) AS id)

-- 2) Create a detailed table of those jobs with ZIPs
CREATE OR REPLACE TABLE `kpi-auto-471020.st_stage_v2.selected_jobs_with_zip` AS
WITH jobs_filtered AS (
  SELECT
    j.id AS jobId,
    j.jobNumber,
    j.locationId,
    j.customerId,
    j.businessUnitId,
    j.jobTypeId,
    j.jobStatus,
    j.completedOn,
    j.createdOn
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
  WHERE j.id IN UNNEST([
    705557, 705812, 727444, 727572, 7761171, 25643501, 25640548,
    40084045, 40091077, 40528050, 52632595, 53425776, 53419951,
    53417012, 66527167, 80656917, 142931404, 144397449, 365792375
  ])
)
SELECT
  jf.jobId,
  jf.jobNumber,
  jf.locationId,
  z.zip5 AS zip_code,
  z.geo_point,
  jf.customerId,
  c.name AS customer_name,
  jf.businessUnitId,
  jf.jobTypeId,
  dj.jobTypeName AS job_type_name,
  jf.jobStatus,
  jf.createdOn,
  jf.completedOn
FROM jobs_filtered jf
LEFT JOIN `kpi-auto-471020.st_stage_v2.locations_zip_norm` z
  ON jf.locationId = z.locationId
LEFT JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` dj
  ON jf.jobId = dj.id
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_customers` c
  ON jf.customerId = c.id;

-- 3) Create an aggregated table: count of selected jobs by ZIP
CREATE OR REPLACE TABLE `kpi-auto-471020.st_mart_v2.jobs_by_zip_selected` AS
SELECT
  zip_code,
  COUNT(*) AS jobs_count,
  COUNT(DISTINCT jobId) AS jobs_distinct
FROM `kpi-auto-471020.st_stage_v2.selected_jobs_with_zip`
GROUP BY zip_code
ORDER BY jobs_count DESC;
