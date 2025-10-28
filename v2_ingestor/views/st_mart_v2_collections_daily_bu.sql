-- Collections Daily by Business Unit
-- Shows payments received by payment date and business unit
--
-- Business Logic:
--   - Collections = sum of payment amounts received on a given date
--   - Date based on paidOn (when payment was actually received)
--   - Aggregated by business unit (via invoice → job → business unit)
--   - Uses flattened payment splits from raw_payments
--
-- Grain: One row per payment date per business unit

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.collections_daily_bu` AS

SELECT
  DATE(p.paidOn) as payment_date,
  bu.name as business_unit,
  COUNT(DISTINCT p.paymentId) as payment_count,
  COUNT(*) as payment_split_count,
  ROUND(SUM(p.amount), 2) as total_collections,
  ROUND(AVG(p.amount), 2) as avg_payment_amount,
  ROUND(MIN(p.amount), 2) as min_payment,
  ROUND(MAX(p.amount), 2) as max_payment,
  CURRENT_TIMESTAMP() as view_created_at
FROM `kpi-auto-471020.st_raw_v2.raw_payments` p
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON p.invoiceId = i.id
LEFT JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_business_units` bu ON j.businessUnitId = bu.id
WHERE DATE(p.paidOn) IS NOT NULL
  AND bu.name IS NOT NULL
  AND p.amount IS NOT NULL
  AND p.amount > 0
GROUP BY DATE(p.paidOn), bu.name
ORDER BY payment_date DESC, bu.name;
