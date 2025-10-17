#!/bin/bash

# Resume Daily WBR data load - ONLY load incomplete BUs
# Skips: Andy's Painting-Sales, Commercial-AZ-Sales (already complete)
# Loads: Guaranteed, Nevada, Phoenix, Tucson

BASE_URL="https://st-kpi-ingestor-999875365235.us-central1.run.app"
LOG_FILE="load-daily-wbr-resume.log"

# ONLY the BUs that need to be completed
BUS=(
  "Guaranteed Painting-Sales"
  "Nevada-Sales"
  "Phoenix-Sales"
  "Tucson-Sales"
)

# Array of week ranges (from:to:description)
WEEKS=(
  # Last 12 weeks of 2025 (most recent first)
  "2025-10-07:2025-10-13:Week of Oct 7 2025"
  "2025-09-30:2025-10-06:Week of Sep 30 2025"
  "2025-09-23:2025-09-29:Week of Sep 23 2025"
  "2025-09-16:2025-09-22:Week of Sep 16 2025"
  "2025-09-09:2025-09-15:Week of Sep 9 2025"
  "2025-09-02:2025-09-08:Week of Sep 2 2025"
  "2025-08-26:2025-09-01:Week of Aug 26 2025"
  "2025-08-19:2025-08-25:Week of Aug 19 2025"
  "2025-08-12:2025-08-18:Week of Aug 12 2025"
  "2025-08-05:2025-08-11:Week of Aug 5 2025"
  "2025-07-29:2025-08-04:Week of Jul 29 2025"
  "2025-07-22:2025-07-28:Week of Jul 22 2025"

  # Same weeks from 2024 for YoY comparison
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
)

echo "=========================================="
echo "RESUME Daily WBR data load at $(date)" | tee -a "$LOG_FILE"
echo "Loading ONLY incomplete BUs: ${#BUS[@]} BUs × ${#WEEKS[@]} weeks = $((${#BUS[@]} * ${#WEEKS[@]})) total calls" | tee -a "$LOG_FILE"
echo "Skipping: Andy's Painting-Sales, Commercial-AZ-Sales (already complete)" | tee -a "$LOG_FILE"
echo "==========================================" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Loop through each BU
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
echo "Daily WBR RESUME load completed at $(date)" | tee -a "$LOG_FILE"
echo "Loaded ${#BUS[@]} BUs × ${#WEEKS[@]} weeks" | tee -a "$LOG_FILE"
echo "==========================================" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "SUMMARY:" | tee -a "$LOG_FILE"
echo "  ✅ Andy's Painting-Sales - Already complete (skipped)" | tee -a "$LOG_FILE"
echo "  ✅ Commercial-AZ-Sales - Already complete (skipped)" | tee -a "$LOG_FILE"
echo "  ✅ Guaranteed Painting-Sales - NOW complete" | tee -a "$LOG_FILE"
echo "  ✅ Nevada-Sales - NOW complete" | tee -a "$LOG_FILE"
echo "  ✅ Phoenix-Sales - NOW complete" | tee -a "$LOG_FILE"
echo "  ✅ Tucson-Sales - NOW complete" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "Next step: Run dedupe with:" | tee -a "$LOG_FILE"
echo "  curl -X POST https://st-kpi-ingestor-999875365235.us-central1.run.app/dedupe/daily_wbr" | tee -a "$LOG_FILE"
