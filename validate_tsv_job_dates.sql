-- Sample 10 jobs from TSV and check their appointment dates
WITH tsv_sample AS (
  SELECT job_id FROM UNNEST([
    389324885, 397576600, 388742795, 389126602, 389182944, 
    389478663, 388899959, 388524852, 397594433, 389671462
  ]) AS job_id
)

SELECT 
  j.id as job_id,
  MIN(DATE(DATETIME(TIMESTAMP(a.scheduledStart), 'America/Phoenix'))) as first_appt_date,
  COUNT(a.id) as appt_count,
  STRING_AGG(DISTINCT CAST(DATE(DATETIME(TIMESTAMP(a.scheduledStart), 'America/Phoenix')) AS STRING), ', ' ORDER BY DATE(DATETIME(TIMESTAMP(a.scheduledStart), 'America/Phoenix'))) as all_appt_dates
FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
INNER JOIN tsv_sample ts ON j.id = ts.job_id
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_appointments` a ON j.id = a.jobId
GROUP BY j.id
ORDER BY j.id
