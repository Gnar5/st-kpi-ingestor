#!/bin/bash

echo "=========================================="
echo "Weekly Data Load Progress"
echo "=========================================="
echo ""

# Check if script is running
if ps aux | grep -q "[l]oad-specific-weeks.sh"; then
  echo "✓ Script is running (PID: $(cat load-weeks.pid 2>/dev/null || echo 'unknown'))"
else
  echo "✗ Script is not running"
fi

echo ""
echo "Last 20 lines of log:"
echo "------------------------------------------"
tail -20 load-weeks.log 2>/dev/null || echo "No log file yet"

echo ""
echo "=========================================="
echo "Current Data in BigQuery:"
echo "=========================================="

bq query --use_legacy_sql=false --format=pretty "
SELECT
  'Leads' as table_name,
  COUNT(*) as total_rows,
  MIN(DATE(job_created_on)) as earliest,
  MAX(DATE(job_created_on)) as latest,
  COUNT(DISTINCT DATE(job_created_on)) as distinct_days
FROM \`kpi-auto-471020.st_raw.raw_leads\`

UNION ALL

SELECT
  'Foreman' as table_name,
  COUNT(*) as total_rows,
  MIN(DATE(job_start)) as earliest,
  MAX(DATE(job_start)) as latest,
  COUNT(DISTINCT DATE(job_start)) as distinct_days
FROM \`kpi-auto-471020.st_raw.raw_foreman\`

UNION ALL

SELECT
  'Collections' as table_name,
  COUNT(*) as total_rows,
  MIN(DATE(payment_date)) as earliest,
  MAX(DATE(payment_date)) as latest,
  COUNT(DISTINCT DATE(payment_date)) as distinct_days
FROM \`kpi-auto-471020.st_raw.raw_collections\`

ORDER BY table_name
" 2>/dev/null

echo ""
echo "To view live logs: tail -f load-weeks.log"
echo "To stop the load: kill \$(cat load-weeks.pid)"
