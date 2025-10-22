#!/bin/bash

echo "========================================"
echo "Backfill Progress Check"
echo "Time: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"
echo ""

# Check if backfill process is running
if pgrep -f "backfill_chunked.js" > /dev/null; then
  echo "✅ Backfill process is RUNNING"
  echo ""
else
  echo "❌ Backfill process is NOT running"
  echo ""
fi

# Show current estimates count in BigQuery
echo "📊 Current BigQuery data:"
bq query --use_legacy_sql=false --format=pretty "
SELECT 
  COUNT(*) as row_count,
  MIN(DATE(modifiedOn)) as earliest_date,
  MAX(DATE(modifiedOn)) as latest_date,
  DATE_DIFF(MAX(DATE(modifiedOn)), MIN(DATE(modifiedOn)), DAY) as days_of_data
FROM \`kpi-auto-471020.st_raw_v2.raw_estimates\`
" 2>/dev/null

echo ""

# Show last 10 lines of backfill log
if [ -f backfill_log_working.txt ]; then
  echo "📝 Recent log output:"
  echo "----------------------------------------"
  tail -15 backfill_log_working.txt
else
  echo "ℹ️  No log file found yet"
fi
