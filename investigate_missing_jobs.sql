SELECT
  j.id,
  j.jobNumber,
  j.businessUnitId,
  bu.businessUnitNormalized,
  jc.jobStatus,
  DATE(jc.job_start_date) as job_start_date,
  jc.job_start_date as job_start_datetime,
  jc.revenue_subtotal,
  MIN(DATE(DATETIME(TIMESTAMP(a.scheduledStart), 'America/Phoenix'))) as first_appt_date,
  COUNT(a.id) as appt_count,
  DATE(DATETIME(TIMESTAMP(j.completedOn), 'America/Phoenix')) as completed_date,
  DATE(DATETIME(TIMESTAMP(j.createdOn), 'America/Phoenix')) as created_date
FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_appointments` a ON j.id = a.jobId
LEFT JOIN `kpi-auto-471020.st_mart_v2.job_costing_v4` jc ON j.id = jc.job_id
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_business_units` bu ON j.businessUnitId = bu.businessUnitId
WHERE j.id IN (365766216, 387944595, 388015522, 389061811, 389133384, 389144857, 389600506, 397950347)
GROUP BY j.id, j.jobNumber, j.businessUnitId, bu.businessUnitNormalized, jc.jobStatus, jc.job_start_date, jc.revenue_subtotal, j.completedOn, j.createdOn
ORDER BY j.id
