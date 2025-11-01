-- Comprehensive reconciliation analysis for ServiceTitan GPM report
-- Week: 10/20/2025 - 10/26/2025

WITH st_job_ids AS (
  -- ServiceTitan's 162 job IDs from their export
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

job_details AS (
  SELECT
    j.id as job_id,
    j.jobNumber,
    j.jobStatus,
    j.businessUnitId,
    DATE(j.completedOn) as completed_date,
    DATE(j.createdOn) as created_date,
    DATE(MIN(a.scheduledStart)) as scheduled_start_date
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_appointments` a ON j.id = a.jobId
  WHERE j.id IN (SELECT job_id FROM st_job_ids)
  GROUP BY 1,2,3,4,5,6
),

job_costs AS (
  SELECT
    jd.job_id,
    jd.jobNumber,
    jd.jobStatus,
    bu.name as business_unit,
    jd.scheduled_start_date,
    jd.completed_date,

    -- Revenue from invoices
    IFNULL(inv.revenue, 0) as revenue,

    -- Labor costs (timesheets + payroll adjustments)
    IFNULL(labor.tech_pay, 0) + IFNULL(pa.payroll_adj, 0) as labor_cost,

    -- Materials from POs
    IFNULL(po.po_total, 0) as po_materials,

    -- Materials from invoice items
    IFNULL(im.invoice_materials, 0) as invoice_materials

  FROM job_details jd
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_business_units` bu ON jd.businessUnitId = bu.id

  -- Revenue from invoices
  LEFT JOIN (
    SELECT
      jobId,
      SUM(subtotal) as revenue
    FROM `kpi-auto-471020.st_raw_v2.raw_invoices`
    WHERE jobId IN (SELECT job_id FROM st_job_ids)
    GROUP BY 1
  ) inv ON jd.job_id = inv.jobId

  -- Labor from timesheets
  LEFT JOIN (
    SELECT
      jobId,
      SUM(laborCost) as tech_pay
    FROM `kpi-auto-471020.st_raw_v2.raw_timesheets`
    WHERE jobId IN (SELECT job_id FROM st_job_ids)
    GROUP BY 1
  ) labor ON jd.job_id = labor.jobId

  -- Payroll adjustments
  LEFT JOIN (
    SELECT
      jobId,
      SUM(amount) as payroll_adj
    FROM `kpi-auto-471020.st_raw_v2.raw_payroll_adjustments`
    WHERE jobId IN (SELECT job_id FROM st_job_ids)
    GROUP BY 1
  ) pa ON jd.job_id = pa.jobId

  -- Purchase Orders
  LEFT JOIN (
    SELECT
      jobId,
      SUM(total) as po_total
    FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders`
    WHERE jobId IN (SELECT job_id FROM st_job_ids)
      AND status != 'Canceled'
    GROUP BY 1
  ) po ON jd.job_id = po.jobId

  -- Invoice materials
  LEFT JOIN (
    SELECT
      i.jobId,
      SUM(CAST(JSON_VALUE(item, '$.cost') AS FLOAT64)) as invoice_materials
    FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i,
    UNNEST(JSON_QUERY_ARRAY(i.items)) as item
    WHERE i.jobId IN (SELECT job_id FROM st_job_ids)
      AND JSON_VALUE(item, '$.type') IN ('Material', 'Equipment')
    GROUP BY 1
  ) im ON jd.job_id = im.jobId
)

-- Final summary matching ServiceTitan's format
SELECT
  '=== RECONCILIATION REPORT ===' as section,
  NULL as metric,
  NULL as servicetitan_value,
  NULL as bigquery_value,
  NULL as difference,
  NULL as variance_pct
UNION ALL
SELECT
  'Week: 10/20-10/26' as section,
  'Job Count' as metric,
  '162' as servicetitan_value,
  CAST(COUNT(*) AS STRING) as bigquery_value,
  CAST(162 - COUNT(*) AS STRING) as difference,
  ROUND((COUNT(*) / 162.0 - 1) * 100, 1) as variance_pct
FROM job_costs
UNION ALL
SELECT
  'Week: 10/20-10/26' as section,
  'Total Revenue' as metric,
  '$474,562' as servicetitan_value,
  CONCAT('$', FORMAT('%,.0f', SUM(revenue))) as bigquery_value,
  CONCAT('$', FORMAT('%,.0f', 474562 - SUM(revenue))) as difference,
  ROUND((SUM(revenue) / 474562.0 - 1) * 100, 1) as variance_pct
FROM job_costs
UNION ALL
SELECT
  'Week: 10/20-10/26' as section,
  'Total Labor' as metric,
  '$171,079' as servicetitan_value,
  CONCAT('$', FORMAT('%,.0f', SUM(labor_cost))) as bigquery_value,
  CONCAT('$', FORMAT('%,.0f', 171079 - SUM(labor_cost))) as difference,
  ROUND((SUM(labor_cost) / 171079.0 - 1) * 100, 1) as variance_pct
FROM job_costs
UNION ALL
SELECT
  'Week: 10/20-10/26' as section,
  'Total Materials' as metric,
  '$105,292' as servicetitan_value,
  CONCAT('$', FORMAT('%,.0f', SUM(po_materials + invoice_materials))) as bigquery_value,
  CONCAT('$', FORMAT('%,.0f', 105292 - SUM(po_materials + invoice_materials))) as difference,
  ROUND((SUM(po_materials + invoice_materials) / 105292.0 - 1) * 100, 1) as variance_pct
FROM job_costs
UNION ALL
SELECT
  'Week: 10/20-10/26' as section,
  'GPM %' as metric,
  '41.93%' as servicetitan_value,
  CONCAT(ROUND((SUM(revenue) - SUM(labor_cost) - SUM(po_materials + invoice_materials)) / NULLIF(SUM(revenue), 0) * 100, 2), '%') as bigquery_value,
  CONCAT(ROUND(41.93 - (SUM(revenue) - SUM(labor_cost) - SUM(po_materials + invoice_materials)) / NULLIF(SUM(revenue), 0) * 100, 2), 'pp') as difference,
  NULL as variance_pct
FROM job_costs
ORDER BY
  CASE section
    WHEN '=== RECONCILIATION REPORT ===' THEN 1
    ELSE 2
  END,
  CASE metric
    WHEN 'Job Count' THEN 1
    WHEN 'Total Revenue' THEN 2
    WHEN 'Total Labor' THEN 3
    WHEN 'Total Materials' THEN 4
    WHEN 'GPM %' THEN 5
    ELSE 6
  END