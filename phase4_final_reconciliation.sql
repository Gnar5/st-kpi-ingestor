-- Phase 4: Final Reconciliation using updated job_costing_v2
-- This should match ServiceTitan's numbers

WITH st_job_ids AS (
  SELECT CAST(job_id AS INT64) as job_id FROM UNNEST([
    361712253,363024202,364255346,365207798,365350342,365622028,366611946,366912331,367228489,367596089,
    367637561,387052938,387244959,387264608,387377241,387563401,387625123,387680999,387744395,387826576,
    387843470,387848312,387850483,387922418,387979122,387987187,388015394,388067731,388072660,388413301,
    388492644,388510634,388524852,388525076,388571483,388656562,388657413,388675028,388742795,388743945,
    388744199,388751942,388781334,388788307,388881200,388899959,388934314,388939000,388940524,388942963,
    388972633,388974729,388978707,388979117,388981346,388987517,389003965,389007816,389022627,389024855,
    389080703,389083232,389083995,389126476,389126602,389126650,389145116,389148898,389182317,389182944,
    389184844,389186404,389205021,389205072,389209474,389215111,389266117,389284427,389294575,389308932,
    389323999,389324885,389336810,389359078,389433089,389437660,389438180,389438781,389442906,389449179,
    389478663,389480902,389499476,389568833,389591994,389612440,389617947,389618389,389635183,389643709,
    389646395,389653357,389653681,389664342,389664616,389664675,389664708,389666802,389666834,389671462,
    397555115,397556470,397576600,397579507,397592957,397594304,397594433,397596121,397596564,397597603,
    397643384,397644200,397644337,397644686,397652196,397653732,397653981,397654339,397654361,397655265,
    397655909,397656404,397656424,397657967,397658510,397663774,397666477,397667192,397669665,397671692,
    397672182,397673826,397674675,397675313,397677704,397686112,397690209,397690946,397692507,397703002,
    397706800,397713160,397747162,397768877,397769180,397792892,397802313,397827963,397830495,397853120,
    397874133,397951702
  ]) AS job_id
),

overall_metrics AS (
  SELECT
    COUNT(*) as job_count,
    SUM(revenue_subtotal) as total_revenue,
    SUM(labor_cost) as total_labor,
    SUM(material_cost_net) as total_materials,
    SUM(gross_profit) as total_gross_profit,
    SAFE_DIVIDE(SUM(gross_profit), NULLIF(SUM(revenue_subtotal), 0)) * 100 as gpm_pct
  FROM `kpi-auto-471020.st_mart_v2.job_costing_v2` jc
  WHERE jc.job_id IN (SELECT job_id FROM st_job_ids)
),

by_bu AS (
  SELECT
    businessUnitNormalized as bu_name,
    COUNT(*) as job_count,
    SUM(revenue_subtotal) as revenue,
    SUM(labor_cost) as labor,
    SUM(material_cost_net) as materials,
    SAFE_DIVIDE(SUM(gross_profit), NULLIF(SUM(revenue_subtotal), 0)) * 100 as gpm_pct
  FROM `kpi-auto-471020.st_mart_v2.job_costing_v2` jc
  WHERE jc.job_id IN (SELECT job_id FROM st_job_ids)
  GROUP BY 1
)

SELECT
  '=== FINAL RECONCILIATION ===' as section,
  NULL as metric,
  NULL as bq_value,
  NULL as st_value,
  NULL as variance,
  NULL as status
UNION ALL
SELECT
  'OVERALL' as section,
  'Job Count' as metric,
  CAST(job_count AS STRING) as bq_value,
  '162' as st_value,
  CAST(job_count - 162 AS STRING) as variance,
  CASE WHEN ABS(job_count - 162) <= 10 THEN '✅' ELSE '❌' END as status
FROM overall_metrics
UNION ALL
SELECT
  'OVERALL' as section,
  'Revenue' as metric,
  CONCAT('$', FORMAT('%.2f', total_revenue)) as bq_value,
  '$474,562.00' as st_value,
  CONCAT('$', FORMAT('%.2f', total_revenue - 474562)) as variance,
  CASE WHEN ABS(total_revenue - 474562) <= 100 THEN '✅' ELSE '⚠️' END as status
FROM overall_metrics
UNION ALL
SELECT
  'OVERALL' as section,
  'Labor' as metric,
  CONCAT('$', FORMAT('%.2f', total_labor)) as bq_value,
  '$171,079.00' as st_value,
  CONCAT('$', FORMAT('%.2f', total_labor - 171079)) as variance,
  CASE WHEN ABS(total_labor - 171079) <= 10 THEN '✅' ELSE '⚠️' END as status
FROM overall_metrics
UNION ALL
SELECT
  'OVERALL' as section,
  'Materials' as metric,
  CONCAT('$', FORMAT('%.2f', total_materials)) as bq_value,
  '$105,292.00' as st_value,
  CONCAT('$', FORMAT('%.2f', total_materials - 105292)) as variance,
  CASE WHEN ABS(total_materials - 105292) <= 10 THEN '✅' ELSE '❌' END as status
FROM overall_metrics
UNION ALL
SELECT
  'OVERALL' as section,
  'GPM %' as metric,
  CONCAT(FORMAT('%.2f', gpm_pct), '%') as bq_value,
  '41.93%' as st_value,
  CONCAT(FORMAT('%.2f', gpm_pct - 41.93), 'pp') as variance,
  CASE WHEN ABS(gpm_pct - 41.93) <= 1 THEN '✅' ELSE '⚠️' END as status
FROM overall_metrics
UNION ALL
-- Business Unit breakdown
SELECT
  'BU: ' || bu_name as section,
  'GPM %' as metric,
  CONCAT(FORMAT('%.2f', gpm_pct), '%') as bq_value,
  'N/A' as st_value,
  'N/A' as variance,
  '📊' as status
FROM by_bu
ORDER BY
  CASE
    WHEN section = '=== FINAL RECONCILIATION ===' THEN 1
    WHEN section = 'OVERALL' THEN 2
    ELSE 3
  END,
  CASE metric
    WHEN 'Job Count' THEN 1
    WHEN 'Revenue' THEN 2
    WHEN 'Labor' THEN 3
    WHEN 'Materials' THEN 4
    WHEN 'GPM %' THEN 5
  END,
  section