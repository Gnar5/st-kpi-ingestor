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
-- TODO: Migrate to st_raw_v2.raw_payments once we resolve API data issue
--   Current issue: ServiceTitan Payments API returns payments with empty 'splits' arrays
--   The list endpoint doesn't populate the splits field - may need individual GET calls
--   or a different API parameter. For now using st_raw.raw_collections which comes
--   from the Collections report and has complete data.

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.collections_daily_bu` AS

SELECT
  DATE(c.payment_date) as payment_date,
  c.bu_key as business_unit,
  COUNT(*) as payment_count,
  ROUND(SUM(c.amount), 2) as total_collections,
  ROUND(AVG(c.amount), 2) as avg_payment_amount,
  ROUND(MIN(c.amount), 2) as min_payment,
  ROUND(MAX(c.amount), 2) as max_payment,
  MAX(c.updated_on) as last_updated,
  CURRENT_TIMESTAMP() as view_created_at
FROM `kpi-auto-471020.st_raw.raw_collections` c
WHERE DATE(c.payment_date) IS NOT NULL
  AND c.bu_key IS NOT NULL
  AND c.amount IS NOT NULL
GROUP BY DATE(c.payment_date), c.bu_key
ORDER BY payment_date DESC, c.bu_key;
