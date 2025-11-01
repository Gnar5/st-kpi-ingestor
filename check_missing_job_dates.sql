SELECT 
  j.id,
  j.jobStatus,
  j.createdOn,
  j.completedOn,
  COUNT(a.id) as appt_count,
  MIN(a.scheduledStart) as first_appt_scheduled,
  MAX(a.scheduledStart) as last_appt_scheduled
FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_appointments` a ON j.id = a.jobId
WHERE j.id IN (
  397819476, 397820255, 397820904, 397821505, 397821726, 397823198,
  397827963, 397830152, 397830495, 397836008, 397845069, 397853120,
  397860829, 397865823, 397874133, 397874779, 397878953, 397950347, 397951702
)
GROUP BY j.id, j.jobStatus, j.createdOn, j.completedOn
ORDER BY j.id
