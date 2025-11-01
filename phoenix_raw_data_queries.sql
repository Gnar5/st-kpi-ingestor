-- Query 1: Raw Payroll data for Phoenix-Production jobs (October week)
-- This shows actual payroll entries linked to Phoenix jobs
SELECT 
  p.jobId,
  DATE(p.date) as payroll_date,
  p.employeeId,
  ROUND(p.grossPay, 2) as gross_pay,
  p.activityCodeId,
  p.hours
FROM `kpi-auto-471020.st_raw_v2.raw_payroll` p
JOIN `kpi-auto-471020.st_mart_v2.job_costing` jc ON p.jobId = jc.job_id
WHERE DATE(jc.job_start_date) BETWEEN '2025-10-20' AND '2025-10-26'
  AND jc.businessUnitNormalized = 'Phoenix-Production'
ORDER BY p.grossPay DESC
LIMIT 20;

-- Query 2: Raw Payroll Adjustments for Phoenix-Production jobs
SELECT 
  pa.jobId,
  DATE(pa.date) as adjustment_date,
  pa.employeeId,
  ROUND(pa.amount, 2) as adjustment_amount,
  pa.memo
FROM `kpi-auto-471020.st_raw_v2.raw_payroll_adjustments` pa
JOIN `kpi-auto-471020.st_mart_v2.job_costing` jc ON pa.jobId = jc.job_id
WHERE DATE(jc.job_start_date) BETWEEN '2025-10-20' AND '2025-10-26'
  AND jc.businessUnitNormalized = 'Phoenix-Production'
ORDER BY pa.amount DESC
LIMIT 20;

-- Query 3: Raw Purchase Orders for Phoenix-Production jobs
SELECT 
  po.jobId,
  po.jobNumber,
  DATE(po.date) as po_date,
  po.vendorName,
  ROUND(po.total, 2) as po_total,
  ROUND(po.tax, 2) as tax,
  ROUND(po.subTotal, 2) as subtotal,
  po.status,
  po.memo
FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders` po
JOIN `kpi-auto-471020.st_mart_v2.job_costing` jc ON po.jobId = jc.job_id
WHERE DATE(jc.job_start_date) BETWEEN '2025-10-20' AND '2025-10-26'
  AND jc.businessUnitNormalized = 'Phoenix-Production'
  AND po.status != 'Canceled'
ORDER BY po.total DESC
LIMIT 20;

-- Query 4: Summary totals from raw data
SELECT 
  'Phoenix Raw Data Totals' as source,
  ROUND(SUM(DISTINCT p.grossPay), 2) as total_payroll,
  ROUND(SUM(DISTINCT pa.amount), 2) as total_adjustments,
  ROUND(SUM(DISTINCT po.total), 2) as total_purchase_orders
FROM `kpi-auto-471020.st_raw_v2.raw_payroll` p
FULL OUTER JOIN `kpi-auto-471020.st_raw_v2.raw_payroll_adjustments` pa ON p.jobId = pa.jobId
FULL OUTER JOIN `kpi-auto-471020.st_raw_v2.raw_purchase_orders` po ON p.jobId = po.jobId
JOIN `kpi-auto-471020.st_mart_v2.job_costing` jc ON COALESCE(p.jobId, pa.jobId, po.jobId) = jc.job_id
WHERE DATE(jc.job_start_date) BETWEEN '2025-10-20' AND '2025-10-26'
  AND jc.businessUnitNormalized = 'Phoenix-Production';
