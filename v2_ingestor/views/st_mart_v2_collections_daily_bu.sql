-- Collections Daily by Business Unit
-- Shows payments received by payment date and business unit
--
-- Business Logic:
--   - Collections = sum of payment amounts received on a given date
--   - Date based on payment_date (when payment was actually received)
--   - Aggregated by business unit
--
-- Grain: One row per payment date per business unit
--
-- Data Source: st_raw_v2.raw_collections (ServiceTitan Collections Report via Reporting API)
--   Note: Uses Reporting API because the Payments v2 entity API does not return
--   amount, invoiceId, or businessUnitId fields needed for collections tracking.

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.collections_daily_bu` AS

SELECT
  DATE(c.payment_date) as payment_date,
  c.business_unit,
  COUNT(*) as payment_count,
  ROUND(SUM(c.amount), 2) as total_collections,
  ROUND(AVG(c.amount), 2) as avg_payment_amount,
  ROUND(MIN(c.amount), 2) as min_payment,
  ROUND(MAX(c.amount), 2) as max_payment,
  MAX(c._ingested_at) as last_updated,
  CURRENT_TIMESTAMP() as view_created_at
FROM `kpi-auto-471020.st_raw_v2.raw_collections` c
WHERE DATE(c.payment_date) IS NOT NULL
  AND c.business_unit IS NOT NULL
  AND c.amount IS NOT NULL
GROUP BY DATE(c.payment_date), c.business_unit
ORDER BY payment_date DESC, c.business_unit;
