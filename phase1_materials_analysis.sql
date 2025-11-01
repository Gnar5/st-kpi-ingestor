-- Phase 1: Materials Deep Dive - Find missing $11,822
-- Analyzing all potential material sources for ST's 162 jobs

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

-- 1. Purchase Orders (what we're currently counting)
po_materials AS (
  SELECT
    'Purchase Orders' as source,
    COUNT(DISTINCT p.jobId) as job_count,
    COUNT(*) as record_count,
    ROUND(SUM(p.total), 2) as total_amount,
    ROUND(SUM(p.tax), 2) as tax_amount,
    ROUND(SUM(p.shipping), 2) as shipping_amount,
    ROUND(SUM(p.subTotal), 2) as subtotal_amount
  FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders` p
  WHERE p.jobId IN (SELECT job_id FROM st_job_ids)
    AND p.status != 'Canceled'
),

-- 2. Invoice Materials/Equipment (from invoice line items)
invoice_materials AS (
  SELECT
    'Invoice Materials/Equipment' as source,
    COUNT(DISTINCT i.jobId) as job_count,
    COUNT(*) as record_count,
    ROUND(SUM(CAST(JSON_VALUE(item, '$.cost') AS FLOAT64)), 2) as total_amount,
    0 as tax_amount,
    0 as shipping_amount,
    ROUND(SUM(CAST(JSON_VALUE(item, '$.total') AS FLOAT64)), 2) as subtotal_amount
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i,
  UNNEST(JSON_QUERY_ARRAY(i.items)) as item
  WHERE i.jobId IN (SELECT job_id FROM st_job_ids)
    AND JSON_VALUE(item, '$.type') IN ('Material', 'Equipment')
),

-- 3. Check if returns exist and reduce materials
returns_materials AS (
  SELECT
    'Returns (negative)' as source,
    COUNT(DISTINCT r.jobId) as job_count,
    COUNT(*) as record_count,
    -ROUND(SUM(r.total), 2) as total_amount,
    -ROUND(SUM(r.tax), 2) as tax_amount,
    0 as shipping_amount,
    -ROUND(SUM(r.subTotal), 2) as subtotal_amount
  FROM `kpi-auto-471020.st_raw_v2.raw_returns` r
  WHERE r.jobId IN (SELECT job_id FROM st_job_ids)
),

-- 4. Check payments for any material-related costs
payment_analysis AS (
  SELECT
    'Payments (for reference)' as source,
    COUNT(DISTINCT p.invoiceId) as job_count,
    COUNT(*) as record_count,
    ROUND(SUM(p.amount), 2) as total_amount,
    0 as tax_amount,
    0 as shipping_amount,
    0 as subtotal_amount
  FROM `kpi-auto-471020.st_raw_v2.raw_payments` p
  JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON p.invoiceId = i.id
  WHERE i.jobId IN (SELECT job_id FROM st_job_ids)
)

-- Combine all sources
SELECT * FROM po_materials
UNION ALL
SELECT * FROM invoice_materials
UNION ALL
SELECT * FROM returns_materials
UNION ALL
SELECT * FROM payment_analysis
ORDER BY total_amount DESC