-- Try to get exactly 245 jobs using appointment-based logic
-- ServiceTitan FOREMAN likely uses appointment dates for production reporting

CREATE OR REPLACE TABLE `kpi-auto-471020.st_stage.tmp_job_population_245` AS
WITH jobs_with_appts AS (
  SELECT
    j.id as job_id,
    j.jobNumber,
    j.jobStatus,
    j.businessUnitId,
    MIN(DATE(a.scheduledStart)) as first_appt_date
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
  JOIN `kpi-auto-471020.st_raw_v2.raw_appointments` a ON j.id = a.jobId
  WHERE j.jobStatus IN ('Completed', 'Hold', 'Scheduled', 'InProgress')
  GROUP BY 1,2,3,4
),
filtered_jobs AS (
  SELECT *
  FROM jobs_with_appts
  WHERE first_appt_date BETWEEN '2025-10-20' AND '2025-10-26'
)
SELECT
  job_id,
  jobNumber,
  jobStatus,
  businessUnitId,
  first_appt_date
FROM filtered_jobs;

-- Check count
SELECT
  'Total Jobs' as metric,
  COUNT(*) as count
FROM `kpi-auto-471020.st_stage.tmp_job_population_245`
UNION ALL
SELECT
  'By Status: ' || jobStatus,
  COUNT(*)
FROM `kpi-auto-471020.st_stage.tmp_job_population_245`
GROUP BY jobStatus;