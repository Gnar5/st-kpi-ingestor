#!/bin/bash

# Check Backfill Status
# Shows row counts and date ranges for all entities

echo "=========================================="
echo "Backfill Status Report"
echo "Generated: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
echo ""

bq query --use_legacy_sql=false --format=pretty "
SELECT
  'campaigns' as entity,
  COUNT(*) as row_count,
  MIN(DATE(modifiedOn)) as earliest_date,
  MAX(DATE(modifiedOn)) as latest_date,
  DATE_DIFF(MAX(DATE(modifiedOn)), MIN(DATE(modifiedOn)), DAY) as days_of_data
FROM \`kpi-auto-471020.st_raw_v2.raw_campaigns\`
UNION ALL
SELECT
  'customers',
  COUNT(*),
  MIN(DATE(modifiedOn)),
  MAX(DATE(modifiedOn)),
  DATE_DIFF(MAX(DATE(modifiedOn)), MIN(DATE(modifiedOn)), DAY)
FROM \`kpi-auto-471020.st_raw_v2.raw_customers\`
UNION ALL
SELECT
  'locations',
  COUNT(*),
  MIN(DATE(modifiedOn)),
  MAX(DATE(modifiedOn)),
  DATE_DIFF(MAX(DATE(modifiedOn)), MIN(DATE(modifiedOn)), DAY)
FROM \`kpi-auto-471020.st_raw_v2.raw_locations\`
UNION ALL
SELECT
  'jobs',
  COUNT(*),
  MIN(DATE(modifiedOn)),
  MAX(DATE(modifiedOn)),
  DATE_DIFF(MAX(DATE(modifiedOn)), MIN(DATE(modifiedOn)), DAY)
FROM \`kpi-auto-471020.st_raw_v2.raw_jobs\`
UNION ALL
SELECT
  'invoices',
  COUNT(*),
  MIN(DATE(modifiedOn)),
  MAX(DATE(modifiedOn)),
  DATE_DIFF(MAX(DATE(modifiedOn)), MIN(DATE(modifiedOn)), DAY)
FROM \`kpi-auto-471020.st_raw_v2.raw_invoices\`
UNION ALL
SELECT
  'estimates',
  COUNT(*),
  MIN(DATE(modifiedOn)),
  MAX(DATE(modifiedOn)),
  DATE_DIFF(MAX(DATE(modifiedOn)), MIN(DATE(modifiedOn)), DAY)
FROM \`kpi-auto-471020.st_raw_v2.raw_estimates\`
UNION ALL
SELECT
  'payments',
  COUNT(*),
  MIN(DATE(modifiedOn)),
  MAX(DATE(modifiedOn)),
  DATE_DIFF(MAX(DATE(modifiedOn)), MIN(DATE(modifiedOn)), DAY)
FROM \`kpi-auto-471020.st_raw_v2.raw_payments\`
UNION ALL
SELECT
  'payroll',
  COUNT(*),
  MIN(DATE(modifiedOn)),
  MAX(DATE(modifiedOn)),
  DATE_DIFF(MAX(DATE(modifiedOn)), MIN(DATE(modifiedOn)), DAY)
FROM \`kpi-auto-471020.st_raw_v2.raw_payroll\`
ORDER BY entity
"

echo ""
echo "=========================================="
echo "Expected Full Backfill:"
echo "----------------------------------------"
echo "All entities should show earliest_date from 2020 or earlier"
echo "Currently only campaigns shows full history (2020-05-18)"
echo "=========================================="
