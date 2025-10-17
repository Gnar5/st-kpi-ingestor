≈#!/bin/bash
set -euo pipefail
SERVICE="https://st-kpi-ingestor-999875365235.us-central1.run.app"
BUS=("Guaranteed Painting-Sales" "Andy's Painting-Sales" "Nevada-Sales" "Tucson-Sales" "Phoenix-Sales" "Commercial-AZ-Sales")
DAYS=400
TZ_AREA="America/Phoenix"
TODAY=$(TZ="$TZ_AREA" date +%F)

backfill_one() {
  local BU="$1"
  local LOG="backfill_${BU// /_}.log"
  echo "=== Starting backfill for $BU ===" | tee -a "$LOG"
  for (( i=DAYS; i>=1; i-- )); do
    TO_DATE=$(date -d "$TODAY - $i days" +%F)
    echo "Ingesting $BU for $TO_DATE" | tee -a "$LOG"
    curl -s "$SERVICE/ingest/daily_wbr?bu=$BU&days=1&to=$TO_DATE" >/dev/null || true
    sleep 2
  done
  echo "=== Finished $BU ===" | tee -a "$LOG"
}

# Kick off two at a time
for (( idx=0; idx<${#BUS[@]}; idx+=2 )); do
  backfill_one "${BUS[$idx]}" &
  if [[ $((idx+1)) -lt ${#BUS[@]} ]]; then
    backfill_one "${BUS[$idx+1]}" &
  fi
  wait   # wait for the pair to finish before starting next pair
done
echo "All done."
