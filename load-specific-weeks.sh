#!/bin/bash

# Load specific weeks of historical data
# Usage: ./load-specific-weeks.sh

BASE_URL="https://st-kpi-ingestor-999875365235.us-central1.run.app"

echo "=========================================="
echo "Load Specific Historical Weeks"
echo "=========================================="
echo ""

# Define the weeks you want to load
# Format: "YYYY-MM-DD:YYYY-MM-DD:Description"
WEEKS=(
  # Last 12 weeks of 2024
  "2024-10-07:2024-10-13:Week of Oct 7 2024"
  "2024-09-30:2024-10-06:Week of Sep 30 2024"
  "2024-09-23:2024-09-29:Week of Sep 23 2024"
  "2024-09-16:2024-09-22:Week of Sep 16 2024"
  "2024-09-09:2024-09-15:Week of Sep 9 2024"
  "2024-09-02:2024-09-08:Week of Sep 2 2024"
  "2024-08-26:2024-09-01:Week of Aug 26 2024"
  "2024-08-19:2024-08-25:Week of Aug 19 2024"
  "2024-08-12:2024-08-18:Week of Aug 12 2024"
  "2024-08-05:2024-08-11:Week of Aug 5 2024"
  "2024-07-29:2024-08-04:Week of Jul 29 2024"
  "2024-07-22:2024-07-28:Week of Jul 22 2024"

  # Same weeks from 2023 for YoY comparison
  "2023-10-09:2023-10-15:Week of Oct 9 2023"
  "2023-10-02:2023-10-08:Week of Oct 2 2023"
  "2023-09-25:2023-10-01:Week of Sep 25 2023"
  "2023-09-18:2023-09-24:Week of Sep 18 2023"
  "2023-09-11:2023-09-17:Week of Sep 11 2023"
  "2023-09-04:2023-09-10:Week of Sep 4 2023"
  "2023-08-28:2023-09-03:Week of Aug 28 2023"
  "2023-08-21:2023-08-27:Week of Aug 21 2023"
  "2023-08-14:2023-08-20:Week of Aug 14 2023"
  "2023-08-07:2023-08-13:Week of Aug 7 2023"
  "2023-07-31:2023-08-06:Week of Jul 31 2023"
  "2023-07-24:2023-07-30:Week of Jul 24 2023"
)

# Which reports to load
REPORTS=("leads" "foreman" "collections")

for WEEK in "${WEEKS[@]}"; do
  IFS=':' read -r FROM TO DESC <<< "$WEEK"

  echo "=========================================="
  echo "$DESC"
  echo "Date range: $FROM to $TO"
  echo "=========================================="

  for REPORT in "${REPORTS[@]}"; do
    echo "Loading $REPORT..."

    RESPONSE=$(curl -s -w "\n%{http_code}" "$BASE_URL/ingest/$REPORT?from=$FROM&to=$TO")
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | head -n-1)

    if [ "$HTTP_CODE" = "200" ]; then
      echo "✓ $REPORT completed: $BODY"
    else
      echo "✗ $REPORT failed (HTTP $HTTP_CODE): $BODY"
    fi

    # Small delay between reports to avoid rate limits
    sleep 2
  done

  echo ""
  echo "Waiting 30 seconds before next week..."
  sleep 30
  echo ""
done

echo "=========================================="
echo "Running dedupe on all tables..."
echo "=========================================="

for REPORT in "${REPORTS[@]}"; do
  echo "Deduping $REPORT..."
  curl -X POST "$BASE_URL/dedupe/$REPORT"
  echo ""
done

echo "=========================================="
echo "Complete!"
echo "=========================================="
echo ""
echo "Check loaded data:"
echo "  bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM \`kpi-auto-471020.st_raw.raw_leads\`'"
echo "  bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM \`kpi-auto-471020.st_raw.raw_foreman\`'"
echo "  bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM \`kpi-auto-471020.st_raw.raw_collections\`'"
