-- Materials breakdown for Phoenix-Production October week
SELECT 
  job_id,
  DATE(start_date) as job_date,
  ROUND(revenue_subtotal, 2) as revenue,
  ROUND(material_cost_net, 2) as material_cost,
  ROUND(labor_cost, 2) as labor_cost
FROM `kpi-auto-471020.st_stage.production_jobs`
WHERE DATE(start_date) BETWEEN '2025-10-20' AND '2025-10-26'
  AND business_unit = 'Phoenix-Production'
  AND material_cost_net > 0
ORDER BY material_cost_net DESC
LIMIT 20;
