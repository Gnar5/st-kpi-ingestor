#!/bin/bash

# Load Daily WBR data - ONE BU AT A TIME through all weeks
# This approach loads all 24 weeks for one BU before moving to the next BU

BASE_URL="https://st-kpi-ingestor-999875365235.us-central1.run.app"
LOG_FILE="load-daily-wbr-by-bu.log"

# Sales Business Units
BUS=(
  "Andy's Painting-Sales"
  "Commercial-AZ-Sales"
  "Guaranteed Painting-Sales"
  "Nevada-Sales"
  "Phoenix-Sales"
  "Tucson-Sales"
)

# Array of week ranges (from:to:description)
WEEKS=(
  # Last 12 weeks of 2024 (most recent first)
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

echo "Starting Daily WBR data load at $(date)" | tee -a "$LOG_FILE"
echo "Loading by BU: ${#BUS[@]} BUs × ${#WEEKS[@]} weeks = $((${#BUS[@]} * ${#WEEKS[@]})) total calls" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Loop through each BU first
for BU in "${BUS[@]}"; do
  BU_ENCODED=$(echo "$BU" | sed 's/ /%20/g' | sed "s/'/%27/g")

  echo "==========================================" | tee -a "$LOG_FILE"
  echo "LOADING ALL WEEKS FOR: $BU" | tee -a "$LOG_FILE"
  echo "==========================================" | tee -a "$LOG_FILE"
  echo "" | tee -a "$LOG_FILE"

  # Loop through all weeks for this BU
  for WEEK in "${WEEKS[@]}"; do
    IFS=':' read -r FROM TO DESC <<< "$WEEK"

    echo "  $DESC ($FROM to $TO)" | tee -a "$LOG_FILE"
    RESPONSE=$(curl -s "$BASE_URL/ingest/daily_wbr?bu=$BU_ENCODED&from=$FROM&to=$TO")
    echo "  ✓ Completed: $RESPONSE" | tee -a "$LOG_FILE"

    # Small delay between week requests for same BU
    sleep 3
  done

  echo "" | tee -a "$LOG_FILE"
  echo "✅ Completed all 24 weeks for $BU" | tee -a "$LOG_FILE"
  echo "Waiting 60 seconds before next BU..." | tee -a "$LOG_FILE"
  echo "" | tee -a "$LOG_FILE"
  sleep 60
done

echo "==========================================" | tee -a "$LOG_FILE"
echo "Daily WBR data load completed at $(date)" | tee -a "$LOG_FILE"
echo "Loaded ${#BUS[@]} BUs × ${#WEEKS[@]} weeks" | tee -a "$LOG_FILE"
echo "==========================================" | tee -a "$LOG_FILE"
