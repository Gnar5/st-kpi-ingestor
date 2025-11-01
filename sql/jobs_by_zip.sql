-- ZIP Analysis: Normalize locations and aggregate jobs by ZIP
-- Project: kpi-auto-471020
-- Datasets: st_raw_v2 (raw), st_stage_v2 (stage), st_mart_v2 (mart)

-- 1) Create a reusable ZIP normalization view from raw_locations
--    - Extracts first 5 digits from postalCode/zip (handles ZIP+4)
--    - Preserves leading zeros via LPAD
--    - Builds an optional geo_point from longitude/latitude when present
CREATE OR REPLACE VIEW `kpi-auto-471020.st_stage_v2.locations_zip_norm` AS
WITH src AS (
  SELECT
    id AS locationId,
    LPAD(
      COALESCE(
        REGEXP_EXTRACT(JSON_VALUE(address, '$.postalCode'), r'(\d{5})'),
        REGEXP_EXTRACT(JSON_VALUE(address, '$.zip'), r'(\d{5})'),
        REGEXP_EXTRACT(TO_JSON_STRING(address), r'(\d{5})')  -- last resort
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


-- 2) Ad-hoc aggregation: jobs by ZIP with optional date + job type filters
--    Usage in BigQuery UI: set parameters start_date, end_date (DATE), job_type_allowlist (ARRAY<INT64>)
--    Notes:
--      - If job_type_allowlist is empty, all job types are included
--      - completedOn filter is optional; include NULL to capture not-yet-completed jobs
DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY);
DECLARE end_date   DATE DEFAULT CURRENT_DATE();
DECLARE job_type_allowlist ARRAY<INT64> DEFAULT [];

WITH jobs_filtered AS (
  SELECT
    j.id AS jobId,
    j.locationId
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
  WHERE (
    j.completedOn IS NULL
    OR (
      j.completedOn >= TIMESTAMP(start_date)
      AND j.completedOn < TIMESTAMP(DATE_ADD(end_date, INTERVAL 1 DAY))
    )
  )
  AND (
    ARRAY_LENGTH(job_type_allowlist) = 0
    OR j.jobTypeId IN UNNEST(job_type_allowlist)
  )
), joined AS (
  SELECT
    z.zip5 AS zip_code,
    j.jobId
  FROM jobs_filtered j
  JOIN `kpi-auto-471020.st_stage_v2.locations_zip_norm` z
    ON j.locationId = z.locationId
)
SELECT
  zip_code,
  COUNT(*) AS jobs_count,
  COUNT(DISTINCT jobId) AS jobs_distinct
FROM joined
GROUP BY zip_code
ORDER BY jobs_count DESC;


-- 3) Persistent aggregation for dashboards: total jobs by ZIP (no filters)
--    Materialized view for quick Looker Studio exploration; apply report-level filters.
CREATE OR REPLACE MATERIALIZED VIEW `kpi-auto-471020.st_mart_v2.jobs_by_zip` AS
SELECT
  z.zip5 AS zip_code,
  COUNT(*) AS jobs_count
FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
JOIN `kpi-auto-471020.st_stage_v2.locations_zip_norm` z
  ON j.locationId = z.locationId
GROUP BY zip_code;

