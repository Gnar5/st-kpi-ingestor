WITH corrected_job_dates AS (
  SELECT 
    j.id as job_id,
    COALESCE(
      MIN(DATETIME(TIMESTAMP(a.scheduledStart), 'America/Phoenix')),
      DATETIME(TIMESTAMP(j.createdOn), 'America/Phoenix')  -- Skip completedOn, go straight to createdOn
    ) as job_start_date_corrected
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_appointments` a ON j.id = a.jobId
  GROUP BY j.id, j.createdOn
)

SELECT 
  COUNT(DISTINCT jc.job_id) as job_count,
  ROUND(SUM(jc.revenue_subtotal), 2) as total_revenue,
  ROUND(SUM(jc.labor_cost), 2) as total_labor,
  ROUND(SUM(jc.material_cost_net), 2) as total_materials,
  ROUND(SUM(jc.total_cost), 2) as total_costs,
  ROUND(SUM(jc.gross_profit) / NULLIF(SUM(jc.revenue_subtotal), 0) * 100, 2) as gpm_percent
FROM `kpi-auto-471020.st_mart_v2.job_costing_v4` jc
INNER JOIN `kpi-auto-471020.st_raw_v2.raw_jobs` j ON jc.job_id = j.id
INNER JOIN corrected_job_dates cjd ON j.id = cjd.job_id
WHERE DATE(cjd.job_start_date_corrected) BETWEEN '2025-10-20' AND '2025-10-26'
  AND j.businessUnitId IN (898, 899, 901, 2305, 95763481, 117043321)
