-- st_mart_v2.outstanding_ar_daily_bu
-- Daily outstanding accounts receivable by business unit
-- Shows AR balance by invoice date (when invoices were created)
--
-- Business Logic:
--   - Outstanding AR = Sum of invoice.balance (unpaid amount)
--   - Grouped by invoice date and business unit
--   - Shows both current outstanding balance and original invoice total
--   - Includes all invoices (paid, partially paid, and unpaid)
--
-- Note: This shows AR by invoice creation date
--       For current AR snapshot, use st_mart_v2.current_ar_by_bu
--
-- Grain: One row per date per business unit

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.outstanding_ar_daily_bu` AS

SELECT
  DATE(i.invoiceDate) as invoice_date,
  bu.name as business_unit,

  -- INVOICE COUNTS
  COUNT(i.id) as total_invoices,
  COUNT(CASE WHEN i.balance > 0 THEN 1 END) as unpaid_invoices,
  COUNT(CASE WHEN i.balance = 0 THEN 1 END) as paid_invoices,

  -- OUTSTANDING AR: Current unpaid balance
  ROUND(SUM(i.balance), 2) as outstanding_ar,

  -- INVOICE TOTALS
  ROUND(SUM(i.total), 2) as total_invoiced,

  -- PAYMENTS RECEIVED (total - balance)
  ROUND(SUM(i.total - i.balance), 2) as total_paid,

  -- COLLECTION RATE: % of invoiced amount that has been collected
  ROUND(
    SAFE_DIVIDE(
      SUM(i.total - i.balance),
      NULLIF(SUM(i.total), 0)
    ) * 100,
    2
  ) as collection_rate_percent,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
LEFT JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_business_units` bu ON j.businessUnitId = bu.id

WHERE DATE(i.invoiceDate) IS NOT NULL
  AND bu.name IS NOT NULL

GROUP BY
  DATE(i.invoiceDate),
  bu.name

ORDER BY
  invoice_date DESC,
  bu.name
;
