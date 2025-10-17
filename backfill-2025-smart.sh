#!/bin/bash

# Smart backfill script that checks BigQuery first and only loads missing 2025 data
# This ensures we have complete 2024 + 2025 data for YoY comparison

BASE_URL="https://st-kpi-ingestor-999875365235.us-central1.run.app"
LOG_FILE="backfill-2025-smart.log"
PROJECT="kpi-auto-471020"

# All weeks we need (12 from 2025 + 12 from 2024 for YoY)
ALL_WEEKS=(
  # 12 weeks from 2025 (current year)
  "2025-10-07:2025-10-13:2025"
  "2025-09-30:2025-10-06:2025"
  "2025-09-23:2025-09-29:2025"
  "2025-09-16:2025-09-22:2025"
  "2025-09-09:2025-09-15:2025"
  "2025-09-02:2025-09-08:2025"
  "2025-08-26:2025-09-01:2025"
  "2025-08-19:2025-08-25:2025"
  "2025-08-12:2025-08-18:2025"
  "2025-08-05:2025-08-11:2025"
  "2025-07-29:2025-08-04:2025"
  "2025-07-22:2025-07-28:2025"

  # 12 weeks from 2024 (for YoY comparison)
  "2024-10-07:2024-10-13:2024"
  "2024-09-30:2024-10-06:2024"
  "2024-09-23:2024-09-29:2024"
  "2024-09-16:2024-09-22:2024"
  "2024-09-09:2024-09-15:2024"
  "2024-09-02:2024-09-08:2024"
  "2024-08-26:2024-09-01:2024"
  "2024-08-19:2024-08-25:2024"
  "2024-08-12:2024-08-18:2024"
  "2024-08-05:2024-08-11:2024"
  "2024-07-29:2024-08-04:2024"
  "2024-07-22:2024-07-28:2024"
)

echo "=========================================="
echo "Smart Backfill - Loading Missing 2024 & 2025 Data"
echo "Started at: $(date)"
echo "=========================================="
echo ""

# Function to check if a date range exists in BigQuery
check_date_range() {
  local table=$1
  local date_column=$2
  local from_date=$3
  local to_date=$4
  local bu_filter=$5

  local query="SELECT COUNT(*) as cnt FROM \`${PROJECT}.st_raw.${table}\`
               WHERE DATE(${date_column}) >= '${from_date}'
               AND DATE(${date_column}) <= '${to_date}'"

  if [ -n "$bu_filter" ]; then
    query="${query} AND bu_key = '${bu_filter}'"
  fi

  local count=$(bq query --use_legacy_sql=false --format=csv --quiet "$query" | tail -n 1)
  echo "$count"
}

echo "=========================================="
echo "LEADS - Checking and loading 2024 & 2025 data"
echo "=========================================="

for WEEK in "${ALL_WEEKS[@]}"; do
  IFS=':' read -r FROM TO YEAR <<< "$WEEK"

  # Check if this week has data
  COUNT=$(check_date_range "raw_leads" "job_created_on" "$FROM" "$TO" "")

  if [ "$COUNT" -lt 10 ]; then
    echo "  Loading $FROM to $TO ($YEAR - found $COUNT rows, needs refresh)" | tee -a "$LOG_FILE"
    RESPONSE=$(curl -s "$BASE_URL/ingest/leads?from=$FROM&to=$TO")
    echo "  ✓ Completed: $RESPONSE" | tee -a "$LOG_FILE"
    sleep 3
  else
    echo "  ✓ Skipping $FROM to $TO ($YEAR - already has $COUNT rows)" | tee -a "$LOG_FILE"
  fi
done

echo ""
echo "=========================================="
echo "FOREMAN - Checking and loading 2024 & 2025 data"
echo "=========================================="

for WEEK in "${ALL_WEEKS[@]}"; do
  IFS=':' read -r FROM TO YEAR <<< "$WEEK"

  COUNT=$(check_date_range "raw_foreman" "job_start" "$FROM" "$TO" "")

  if [ "$COUNT" -lt 10 ]; then
    echo "  Loading $FROM to $TO ($YEAR - found $COUNT rows, needs refresh)" | tee -a "$LOG_FILE"
    RESPONSE=$(curl -s "$BASE_URL/ingest/foreman?from=$FROM&to=$TO")
    echo "  ✓ Completed: $RESPONSE" | tee -a "$LOG_FILE"
    sleep 3
  else
    echo "  ✓ Skipping $FROM to $TO ($YEAR - already has $COUNT rows)" | tee -a "$LOG_FILE"
  fi
done

echo ""
echo "=========================================="
echo "COLLECTIONS - Checking and loading 2024 & 2025 data"
echo "=========================================="

for WEEK in "${ALL_WEEKS[@]}"; do
  IFS=':' read -r FROM TO YEAR <<< "$WEEK"

  COUNT=$(check_date_range "raw_collections" "payment_date" "$FROM" "$TO" "")

  if [ "$COUNT" -lt 10 ]; then
    echo "  Loading $FROM to $TO ($YEAR - found $COUNT rows, needs refresh)" | tee -a "$LOG_FILE"
    RESPONSE=$(curl -s "$BASE_URL/ingest/collections?from=$FROM&to=$TO")
    echo "  ✓ Completed: $RESPONSE" | tee -a "$LOG_FILE"
    sleep 3
  else
    echo "  ✓ Skipping $FROM to $TO ($YEAR - already has $COUNT rows)" | tee -a "$LOG_FILE"
  fi
done

echo ""
echo "=========================================="
echo "DAILY WBR - Checking and loading 2024 & 2025 data by BU"
echo "=========================================="

# Sales BUs for Daily WBR
BUS=(
  "Andy's Painting-Sales"
  "Commercial-AZ-Sales"
  "Guaranteed Painting-Sales"
  "Nevada-Sales"
  "Phoenix-Sales"
  "Tucson-Sales"
)

for BU in "${BUS[@]}"; do
  BU_ENCODED=$(echo "$BU" | sed 's/ /%20/g' | sed "s/'/%27/g")

  echo ""
  echo "Checking $BU..."

  for WEEK in "${ALL_WEEKS[@]}"; do
    IFS=':' read -r FROM TO YEAR <<< "$WEEK"

    # For Daily WBR, check by bu_name
    COUNT=$(bq query --use_legacy_sql=false --format=csv --quiet \
      "SELECT COUNT(*) FROM \`${PROJECT}.st_raw.raw_daily_wbr_v2\`
       WHERE bu_name = '${BU}'
       AND DATE(event_date) >= '${FROM}'
       AND DATE(event_date) <= '${TO}'" | tail -n 1)

    if [ "$COUNT" -lt 3 ]; then
      echo "  Loading $FROM to $TO for $BU ($YEAR - found $COUNT rows)" | tee -a "$LOG_FILE"
      RESPONSE=$(curl -s "$BASE_URL/ingest/daily_wbr?bu=$BU_ENCODED&from=$FROM&to=$TO")
      echo "  ✓ Completed: $RESPONSE" | tee -a "$LOG_FILE"
      sleep 3
    else
      echo "  ✓ Skipping $FROM to $TO ($YEAR - already has $COUNT rows)" | tee -a "$LOG_FILE"
    fi
  done

  echo "Waiting 30 seconds before next BU..."
  sleep 30
done

echo ""
echo "=========================================="
echo "Smart Backfill Completed at: $(date)"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Run dedupes for all tables"
echo "2. Verify data against ServiceTitan"
echo ""
