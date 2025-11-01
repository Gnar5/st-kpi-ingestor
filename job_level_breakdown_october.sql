-- Job-level breakdown for October week (10/20-10/26) by Business Unit
-- Perfect for cross-referencing with ServiceTitan FOREMAN report

SELECT 
  business_unit,
  job_id,
  DATE(start_date) as job_date,
  ROUND(revenue_subtotal, 2) as revenue,
  ROUND(labor_cost, 2) as labor,
  ROUND(material_cost_net, 2) as materials,
  ROUND(revenue_subtotal - labor_cost - material_cost_net, 2) as gross_profit,
  ROUND(
    SAFE_DIVIDE(
      revenue_subtotal - labor_cost - material_cost_net,
      NULLIF(revenue_subtotal, 0)
    ) * 100, 2
  ) as gpm_percent
FROM `kpi-auto-471020.st_stage.production_jobs`
WHERE DATE(start_date) BETWEEN '2025-10-20' AND '2025-10-26'
ORDER BY 
  business_unit,
  job_date,
  revenue DESC;
