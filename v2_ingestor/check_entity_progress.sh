#!/bin/bash

# Usage: ./check_entity_progress.sh <entity>
# Example: ./check_entity_progress.sh jobs

ENTITY=${1:-estimates}

echo "========================================"
echo "$ENTITY Backfill Progress Check"
echo "Time: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"
echo ""

# Check if backfill process is running
if pgrep -f "backfill_entity.js $ENTITY" > /dev/null; then
  echo "✅ Backfill process is RUNNING"
  echo ""
elif pgrep -f "backfill_chunked.js" > /dev/null; then
  echo "✅ Backfill process is RUNNING (estimates)"
  echo ""
else
  echo "❌ Backfill process is NOT running"
  echo ""
fi

# Show current data in BigQuery
echo "📊 Current BigQuery data for $ENTITY:"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  COUNT(*) as row_count,
  MIN(DATE(createdOn)) as earliest_created,
  MAX(DATE(createdOn)) as latest_created,
  MIN(DATE(modifiedOn)) as earliest_modified,
  MAX(DATE(modifiedOn)) as latest_modified,
  DATE_DIFF(MAX(DATE(createdOn)), MIN(DATE(createdOn)), DAY) as days_span
FROM \`kpi-auto-471020.st_raw_v2.raw_${ENTITY}\`
" 2>/dev/null

echo ""
echo "📅 Breakdown by creation year:"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  EXTRACT(YEAR FROM createdOn) as creation_year,
  COUNT(*) as record_count
FROM \`kpi-auto-471020.st_raw_v2.raw_${ENTITY}\`
WHERE createdOn IS NOT NULL
GROUP BY creation_year
ORDER BY creation_year
" 2>/dev/null
