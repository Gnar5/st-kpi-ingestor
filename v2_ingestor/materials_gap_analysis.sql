-- Comprehensive Materials Gap Analysis
-- Goal: Find the missing $6,798 in materials
-- Breaking down every possible material source

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

-- 1. Purchase Order costs (subtotal, tax, shipping, discount)
po_breakdown AS (
  SELECT
    'PO Subtotal' as component,
    SUM(subTotal) as amount,
    COUNT(*) as count
  FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders`
  WHERE jobId IN (SELECT job_id FROM st_job_ids)
    AND status != 'Canceled'
  UNION ALL
  SELECT
    'PO Tax' as component,
    SUM(tax) as amount,
    COUNT(*) as count
  FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders`
  WHERE jobId IN (SELECT job_id FROM st_job_ids)
    AND status != 'Canceled'
  UNION ALL
  SELECT
    'PO Shipping' as component,
    SUM(shipping) as amount,
    COUNT(*) as count
  FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders`
  WHERE jobId IN (SELECT job_id FROM st_job_ids)
    AND status != 'Canceled'
  UNION ALL
  SELECT
    'PO Discount (negative)' as component,
    -SUM(discount) as amount,
    COUNT(*) as count
  FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders`
  WHERE jobId IN (SELECT job_id FROM st_job_ids)
    AND status != 'Canceled'
  UNION ALL
  SELECT
    'PO Total (header)' as component,
    SUM(total) as amount,
    COUNT(*) as count
  FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders`
  WHERE jobId IN (SELECT job_id FROM st_job_ids)
    AND status != 'Canceled'
),

-- 2. Invoice materials/equipment from line items
invoice_material_breakdown AS (
  SELECT
    'Invoice Material Cost' as component,
    SUM(CAST(JSON_VALUE(item, '$.cost') AS FLOAT64)) as amount,
    COUNT(*) as count
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i,
  UNNEST(JSON_QUERY_ARRAY(i.items)) as item
  WHERE i.jobId IN (SELECT job_id FROM st_job_ids)
    AND JSON_VALUE(item, '$.type') = 'Material'
  UNION ALL
  SELECT
    'Invoice Equipment Cost' as component,
    SUM(CAST(JSON_VALUE(item, '$.cost') AS FLOAT64)) as amount,
    COUNT(*) as count
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i,
  UNNEST(JSON_QUERY_ARRAY(i.items)) as item
  WHERE i.jobId IN (SELECT job_id FROM st_job_ids)
    AND JSON_VALUE(item, '$.type') = 'Equipment'
  UNION ALL
  SELECT
    'Invoice Material Total (price)' as component,
    SUM(CAST(JSON_VALUE(item, '$.total') AS FLOAT64)) as amount,
    COUNT(*) as count
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i,
  UNNEST(JSON_QUERY_ARRAY(i.items)) as item
  WHERE i.jobId IN (SELECT job_id FROM st_job_ids)
    AND JSON_VALUE(item, '$.type') IN ('Material', 'Equipment')
),

-- 3. Other invoice item types (might include misc fees)
other_invoice_items AS (
  SELECT
    CONCAT('Invoice Type: ', item_type) as component,
    SUM(cost) as amount,
    COUNT(*) as count
  FROM (
    SELECT
      JSON_VALUE(item, '$.type') as item_type,
      CAST(JSON_VALUE(item, '$.cost') AS FLOAT64) as cost
    FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i,
    UNNEST(JSON_QUERY_ARRAY(i.items)) as item
    WHERE i.jobId IN (SELECT job_id FROM st_job_ids)
      AND JSON_VALUE(item, '$.type') NOT IN ('Material', 'Equipment', 'Service', 'Labor')
  )
  GROUP BY item_type
),

-- 4. Returns (credits that reduce material cost)
returns_breakdown AS (
  SELECT
    'Returns Subtotal (credit)' as component,
    -SUM(subTotal) as amount,
    COUNT(*) as count
  FROM `kpi-auto-471020.st_raw_v2.raw_returns`
  WHERE jobId IN (SELECT job_id FROM st_job_ids)
  UNION ALL
  SELECT
    'Returns Tax (credit)' as component,
    -SUM(tax) as amount,
    COUNT(*) as count
  FROM `kpi-auto-471020.st_raw_v2.raw_returns`
  WHERE jobId IN (SELECT job_id FROM st_job_ids)
  UNION ALL
  SELECT
    'Returns Total (credit)' as component,
    -SUM(total) as amount,
    COUNT(*) as count
  FROM `kpi-auto-471020.st_raw_v2.raw_returns`
  WHERE jobId IN (SELECT job_id FROM st_job_ids)
),

-- 5. Summary
all_components AS (
  SELECT * FROM po_breakdown
  UNION ALL
  SELECT * FROM invoice_material_breakdown
  UNION ALL
  SELECT * FROM other_invoice_items
  UNION ALL
  SELECT * FROM returns_breakdown
)

SELECT
  component,
  ROUND(IFNULL(amount, 0), 2) as amount,
  count,
  CASE
    WHEN component LIKE '%Total%' THEN '📊 TOTAL'
    WHEN component LIKE '%Tax%' OR component LIKE '%Shipping%' OR component LIKE '%Discount%' THEN '💰 Adjustment'
    WHEN component LIKE '%Returns%' THEN '↩️ Credit'
    ELSE '📦 Cost'
  END as category
FROM all_components
WHERE IFNULL(amount, 0) != 0
ORDER BY
  CASE category
    WHEN '📊 TOTAL' THEN 1
    WHEN '📦 Cost' THEN 2
    WHEN '💰 Adjustment' THEN 3
    WHEN '↩️ Credit' THEN 4
  END,
  amount DESC