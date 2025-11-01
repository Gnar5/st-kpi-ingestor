SELECT
  COUNT(DISTINCT jc.job_id) as job_count,
  ROUND(SUM(jc.revenue_subtotal), 2) as total_revenue,
  ROUND(SUM(jc.labor_cost), 2) as total_labor,
  ROUND(SUM(jc.material_cost_net), 2) as total_materials,
  ROUND(SUM(jc.gross_profit), 2) as gross_profit,
  ROUND(SAFE_DIVIDE(SUM(jc.gross_profit), SUM(jc.revenue_subtotal)) * 100, 2) as gpm_percent
FROM `kpi-auto-471020.st_mart_v2.job_costing_v4` jc
INNER JOIN `kpi-auto-471020.st_raw_v2.raw_jobs` j ON jc.job_id = j.id
WHERE DATE(jc.job_start_date) BETWEEN '2025-10-20' AND '2025-10-26'
  AND jc.jobStatus != 'Canceled'
  AND j.businessUnitId IN (898, 899, 901, 2305, 95763481, 117043321)
