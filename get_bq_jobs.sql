SELECT jc.job_id
FROM `kpi-auto-471020.st_mart_v2.job_costing_v4` jc
INNER JOIN `kpi-auto-471020.st_raw_v2.raw_jobs` j ON jc.job_id = j.id
WHERE DATE(jc.job_start_date) BETWEEN '2025-10-20' AND '2025-10-26'
  AND jc.jobStatus != 'Canceled'
  AND j.businessUnitId IN (898, 899, 901, 2305, 95763481, 117043321)
ORDER BY jc.job_id
