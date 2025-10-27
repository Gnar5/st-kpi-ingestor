-- st_mart_v2.current_ar_by_bu
-- Current outstanding accounts receivable by business unit (snapshot)
-- Shows total AR balance across all unpaid/partially paid invoices
--
-- Business Logic:
--   - Current AR = Sum of all invoice.balance > 0 across all time
--   - Grouped by business unit only (not by date)
--   - This is a point-in-time snapshot of what customers currently owe
--
-- Note: This is a snapshot view showing current state
--       For AR by invoice date, use st_mart_v2.outstanding_ar_daily_bu
--
-- Grain: One row per business unit

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.current_ar_by_bu` AS

SELECT
  bu.name as business_unit,

  -- CURRENT OUTSTANDING AR: Total owed by customers right now
  ROUND(SUM(i.balance), 2) as current_outstanding_ar,

  -- INVOICE COUNTS (with balance > 0)
  COUNT(CASE WHEN i.balance > 0 THEN 1 END) as unpaid_invoice_count,

  -- TOTAL OWED: Original invoice amounts for unpaid invoices
  ROUND(SUM(CASE WHEN i.balance > 0 THEN i.total ELSE 0 END), 2) as total_invoice_amount,

  -- PAYMENTS RECEIVED: On the unpaid invoices
  ROUND(SUM(CASE WHEN i.balance > 0 THEN i.total - i.balance ELSE 0 END), 2) as partial_payments_received,

  -- AR AGING METRICS (for reference)
  COUNT(CASE
    WHEN i.balance > 0 AND DATE_DIFF(CURRENT_DATE(), DATE(i.invoiceDate), DAY) <= 30
    THEN 1
  END) as ar_0_30_days_count,

  COUNT(CASE
    WHEN i.balance > 0 AND DATE_DIFF(CURRENT_DATE(), DATE(i.invoiceDate), DAY) BETWEEN 31 AND 60
    THEN 1
  END) as ar_31_60_days_count,

  COUNT(CASE
    WHEN i.balance > 0 AND DATE_DIFF(CURRENT_DATE(), DATE(i.invoiceDate), DAY) > 60
    THEN 1
  END) as ar_over_60_days_count,

  -- AR AGING AMOUNTS
  ROUND(SUM(CASE
    WHEN i.balance > 0 AND DATE_DIFF(CURRENT_DATE(), DATE(i.invoiceDate), DAY) <= 30
    THEN i.balance
    ELSE 0
  END), 2) as ar_0_30_days,

  ROUND(SUM(CASE
    WHEN i.balance > 0 AND DATE_DIFF(CURRENT_DATE(), DATE(i.invoiceDate), DAY) BETWEEN 31 AND 60
    THEN i.balance
    ELSE 0
  END), 2) as ar_31_60_days,

  ROUND(SUM(CASE
    WHEN i.balance > 0 AND DATE_DIFF(CURRENT_DATE(), DATE(i.invoiceDate), DAY) > 60
    THEN i.balance
    ELSE 0
  END), 2) as ar_over_60_days,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
LEFT JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_business_units` bu ON j.businessUnitId = bu.id

WHERE bu.name IS NOT NULL

GROUP BY bu.name

ORDER BY current_outstanding_ar DESC
;
