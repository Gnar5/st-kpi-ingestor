-- Check dates for the 41 missing jobs
WITH missing_jobs AS (
  SELECT CAST(job_id AS INT64) as job_id FROM UNNEST([
    365622028,366912331,367596089,387052938,387377241,387563401,387848312,388015394,388743945,388934314,
    388939000,388940524,388942963,388987517,389003965,389145116,389205072,389209474,389215111,389266117,
    389308932,389437660,389438180,389449179,389617947,389653357,389664342,389664708,397594304,397597603,
    397656404,397657967,397667192,397669665,397671692,397675313,397686112,397706800,397802313,397827963,
    397830495
  ]) AS job_id
)

SELECT
  j.id as job_id,
  j.jobNumber,
  j.createdOn,
  j.completedOn,
  j.jobStatus,
  MIN(DATETIME(TIMESTAMP(a.scheduledStart), 'America/Phoenix')) as min_scheduled_start,
  MAX(DATETIME(TIMESTAMP(a.scheduledEnd), 'America/Phoenix')) as max_scheduled_end,
  COUNT(DISTINCT a.id) as appointment_count,
  -- Check if they're in job_costing_v4
  CASE WHEN jc.job_id IS NOT NULL THEN 'YES' ELSE 'NO' END as in_job_costing_v4
FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
INNER JOIN missing_jobs mj ON j.id = mj.job_id
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_appointments` a ON j.id = a.jobId
LEFT JOIN `kpi-auto-471020.st_mart_v2.job_costing_v4` jc ON j.id = jc.job_id
GROUP BY j.id, j.jobNumber, j.createdOn, j.completedOn, j.jobStatus, jc.job_id
ORDER BY min_scheduled_start;
