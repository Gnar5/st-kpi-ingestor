#!/bin/bash

echo "=========================================="
echo "Monitoring Estimates Backfill Progress"
echo "=========================================="
echo ""

while true; do
  echo "[$(date '+%H:%M:%S')] Checking estimates count..."
  
  bq query --use_legacy_sql=false --format=csv "
    SELECT 
      COUNT(*) as row_count,
      MIN(DATE(modifiedOn)) as earliest_date,
      MAX(DATE(modifiedOn)) as latest_date
    FROM \`kpi-auto-471020.st_raw_v2.raw_estimates\`
  " 2>/dev/null | tail -n +2
  
  echo ""
  sleep 30
done
