-- Test: Does using PO items instead of headers close the gap?
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
)

SELECT
  'Scenario 1: PO Header + Invoice Materials' as scenario,
  ROUND(SUM(IFNULL(po_header, 0) + IFNULL(invoice_mat, 0)), 2) as total_materials,
  ROUND(105292 - SUM(IFNULL(po_header, 0) + IFNULL(invoice_mat, 0)), 2) as gap_to_st
FROM (
  SELECT
    j.job_id,
    po.header_total as po_header,
    im.invoice_materials as invoice_mat
  FROM st_job_ids j
  LEFT JOIN (
    SELECT jobId, SUM(total) as header_total
    FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders`
    WHERE jobId IN (SELECT job_id FROM st_job_ids)
      AND status != 'Canceled'
    GROUP BY 1
  ) po ON j.job_id = po.jobId
  LEFT JOIN (
    SELECT
      i.jobId,
      SUM(CAST(JSON_VALUE(item, '$.cost') AS FLOAT64)) as invoice_materials
    FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i,
    UNNEST(JSON_QUERY_ARRAY(i.items)) as item
    WHERE i.jobId IN (SELECT job_id FROM st_job_ids)
      AND JSON_VALUE(item, '$.type') IN ('Material', 'Equipment')
    GROUP BY 1
  ) im ON j.job_id = im.jobId
)

UNION ALL

SELECT
  'Scenario 2: PO Items + Invoice Materials' as scenario,
  ROUND(SUM(IFNULL(po_items, 0) + IFNULL(invoice_mat, 0)), 2) as total_materials,
  ROUND(105292 - SUM(IFNULL(po_items, 0) + IFNULL(invoice_mat, 0)), 2) as gap_to_st
FROM (
  SELECT
    j.job_id,
    poi.items_total as po_items,
    im.invoice_materials as invoice_mat
  FROM st_job_ids j
  LEFT JOIN (
    SELECT
      p.jobId,
      SUM(CAST(JSON_VALUE(item, '$.total') AS FLOAT64)) as items_total
    FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders` p,
    UNNEST(JSON_QUERY_ARRAY(p.items)) as item
    WHERE p.jobId IN (SELECT job_id FROM st_job_ids)
      AND p.status != 'Canceled'
    GROUP BY 1
  ) poi ON j.job_id = poi.jobId
  LEFT JOIN (
    SELECT
      i.jobId,
      SUM(CAST(JSON_VALUE(item, '$.cost') AS FLOAT64)) as invoice_materials
    FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i,
    UNNEST(JSON_QUERY_ARRAY(i.items)) as item
    WHERE i.jobId IN (SELECT job_id FROM st_job_ids)
      AND JSON_VALUE(item, '$.type') IN ('Material', 'Equipment')
    GROUP BY 1
  ) im ON j.job_id = im.jobId
)

UNION ALL

SELECT
  'Scenario 3: PO Items + Tax/Ship + Invoice Materials' as scenario,
  ROUND(SUM(IFNULL(po_items, 0) + IFNULL(po_tax_ship, 0) + IFNULL(invoice_mat, 0)), 2) as total_materials,
  ROUND(105292 - SUM(IFNULL(po_items, 0) + IFNULL(po_tax_ship, 0) + IFNULL(invoice_mat, 0)), 2) as gap_to_st
FROM (
  SELECT
    j.job_id,
    poi.items_total as po_items,
    pots.tax_ship as po_tax_ship,
    im.invoice_materials as invoice_mat
  FROM st_job_ids j
  LEFT JOIN (
    SELECT
      p.jobId,
      SUM(CAST(JSON_VALUE(item, '$.total') AS FLOAT64)) as items_total
    FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders` p,
    UNNEST(JSON_QUERY_ARRAY(p.items)) as item
    WHERE p.jobId IN (SELECT job_id FROM st_job_ids)
      AND p.status != 'Canceled'
    GROUP BY 1
  ) poi ON j.job_id = poi.jobId
  LEFT JOIN (
    SELECT
      jobId,
      SUM(tax + shipping) as tax_ship
    FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders`
    WHERE jobId IN (SELECT job_id FROM st_job_ids)
      AND status != 'Canceled'
    GROUP BY 1
  ) pots ON j.job_id = pots.jobId
  LEFT JOIN (
    SELECT
      i.jobId,
      SUM(CAST(JSON_VALUE(item, '$.cost') AS FLOAT64)) as invoice_materials
    FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i,
    UNNEST(JSON_QUERY_ARRAY(i.items)) as item
    WHERE i.jobId IN (SELECT job_id FROM st_job_ids)
      AND JSON_VALUE(item, '$.type') IN ('Material', 'Equipment')
    GROUP BY 1
  ) im ON j.job_id = im.jobId
)