-- Detailed reconciliation for the 162 ST jobs
-- Calculate all financials to match ST's numbers

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

job_financials AS (
  SELECT
    j.id as job_id,
    j.jobNumber,
    j.jobStatus,

    -- Revenue from invoices
    IFNULL(i.revenue, 0) as revenue,

    -- Labor from payroll
    IFNULL(pr.labor_cost, 0) + IFNULL(pa.payroll_adj, 0) as labor,

    -- Materials from multiple sources
    IFNULL(po.po_materials, 0) as po_materials,
    IFNULL(im.invoice_materials, 0) as invoice_materials,
    IFNULL(ret.return_credits, 0) as return_credits

  FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j

  -- Revenue
  LEFT JOIN (
    SELECT jobId, SUM(subtotal) as revenue
    FROM `kpi-auto-471020.st_raw_v2.raw_invoices`
    WHERE jobId IN (SELECT job_id FROM st_job_ids)
    GROUP BY 1
  ) i ON j.id = i.jobId

  -- Labor from payroll
  LEFT JOIN (
    SELECT jobId, SUM(amount) as labor_cost
    FROM `kpi-auto-471020.st_raw_v2.raw_payroll`
    WHERE jobId IN (SELECT job_id FROM st_job_ids)
    GROUP BY 1
  ) pr ON j.id = pr.jobId

  -- Payroll adjustments (via invoiceId)
  LEFT JOIN (
    SELECT i.jobId, SUM(pa.amount) as payroll_adj
    FROM `kpi-auto-471020.st_raw_v2.raw_payroll_adjustments` pa
    JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON pa.invoiceId = i.id
    WHERE i.jobId IN (SELECT job_id FROM st_job_ids)
    GROUP BY 1
  ) pa ON j.id = pa.jobId

  -- Purchase Orders (including tax)
  LEFT JOIN (
    SELECT jobId, SUM(total) as po_materials
    FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders`
    WHERE jobId IN (SELECT job_id FROM st_job_ids)
      AND status != 'Canceled'
    GROUP BY 1
  ) po ON j.id = po.jobId

  -- Invoice Materials
  LEFT JOIN (
    SELECT
      i.jobId,
      SUM(CAST(JSON_VALUE(item, '$.cost') AS FLOAT64)) as invoice_materials
    FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i,
    UNNEST(JSON_QUERY_ARRAY(i.items)) as item
    WHERE i.jobId IN (SELECT job_id FROM st_job_ids)
      AND JSON_VALUE(item, '$.type') IN ('Material', 'Equipment')
    GROUP BY 1
  ) im ON j.id = im.jobId

  -- Returns (credits)
  LEFT JOIN (
    SELECT jobId, -SUM(total) as return_credits
    FROM `kpi-auto-471020.st_raw_v2.raw_returns`
    WHERE jobId IN (SELECT job_id FROM st_job_ids)
    GROUP BY 1
  ) ret ON j.id = ret.jobId

  WHERE j.id IN (SELECT job_id FROM st_job_ids)
)

SELECT
  '=== OVERALL TOTALS ===' as section,
  NULL as metric,
  NULL as value
UNION ALL
SELECT
  'Metrics' as section,
  'Job Count' as metric,
  CAST(COUNT(*) AS STRING) as value
FROM job_financials
UNION ALL
SELECT
  'Metrics' as section,
  'Total Revenue' as metric,
  CONCAT('$', FORMAT('%.2f', SUM(revenue))) as value
FROM job_financials
UNION ALL
SELECT
  'Metrics' as section,
  'Total Labor' as metric,
  CONCAT('$', FORMAT('%.2f', SUM(labor))) as value
FROM job_financials
UNION ALL
SELECT
  'Metrics' as section,
  'PO Materials' as metric,
  CONCAT('$', FORMAT('%.2f', SUM(po_materials))) as value
FROM job_financials
UNION ALL
SELECT
  'Metrics' as section,
  'Invoice Materials' as metric,
  CONCAT('$', FORMAT('%.2f', SUM(invoice_materials))) as value
FROM job_financials
UNION ALL
SELECT
  'Metrics' as section,
  'Total Materials' as metric,
  CONCAT('$', FORMAT('%.2f', SUM(po_materials + invoice_materials + return_credits))) as value
FROM job_financials
UNION ALL
SELECT
  'Metrics' as section,
  'Gross Profit' as metric,
  CONCAT('$', FORMAT('%.2f', SUM(revenue - labor - po_materials - invoice_materials - return_credits))) as value
FROM job_financials
UNION ALL
SELECT
  'Metrics' as section,
  'GPM %' as metric,
  CONCAT(FORMAT('%.2f', (SUM(revenue - labor - po_materials - invoice_materials - return_credits) / NULLIF(SUM(revenue), 0)) * 100), '%') as value
FROM job_financials
UNION ALL
SELECT
  '=== VARIANCES ===' as section,
  NULL as metric,
  NULL as value
UNION ALL
SELECT
  'vs ServiceTitan' as section,
  'Revenue Variance' as metric,
  CONCAT('$', FORMAT('%.2f', SUM(revenue) - 474562)) as value
FROM job_financials
UNION ALL
SELECT
  'vs ServiceTitan' as section,
  'Labor Variance' as metric,
  CONCAT('$', FORMAT('%.2f', SUM(labor) - 171079)) as value
FROM job_financials
UNION ALL
SELECT
  'vs ServiceTitan' as section,
  'Materials Variance' as metric,
  CONCAT('$', FORMAT('%.2f', SUM(po_materials + invoice_materials + return_credits) - 105292)) as value
FROM job_financials
ORDER BY
  CASE section
    WHEN '=== OVERALL TOTALS ===' THEN 1
    WHEN 'Metrics' THEN 2
    WHEN '=== VARIANCES ===' THEN 3
    WHEN 'vs ServiceTitan' THEN 4
  END,
  CASE metric
    WHEN 'Job Count' THEN 1
    WHEN 'Total Revenue' THEN 2
    WHEN 'Total Labor' THEN 3
    WHEN 'PO Materials' THEN 4
    WHEN 'Invoice Materials' THEN 5
    WHEN 'Total Materials' THEN 6
    WHEN 'Gross Profit' THEN 7
    WHEN 'GPM %' THEN 8
    WHEN 'Revenue Variance' THEN 9
    WHEN 'Labor Variance' THEN 10
    WHEN 'Materials Variance' THEN 11
  END