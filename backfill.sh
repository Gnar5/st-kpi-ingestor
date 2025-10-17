#!/bin/bash
set -euo pipefail

SERVICE="https://st-kpi-ingestor-999875365235.us-central1.run.app"

# Exact BU keys (from /debug/wbr_keys)
BUS=(
 "Guaranteed Painting-Sales"
  "Andy's Painting-Sales"
  "Nevada-Sales"
  "Tucson-Sales"
  "Phoenix-Sales"
  "Commercial-AZ-Sales"
)

DAYS=400
TZ_AREA="America/Phoenix"

# Pin 'today' once in Phoenix time so it doesn't move while we run
TODAY=$(TZ="$TZ_AREA" date +%F)

echo "Pinned 'today' in $TZ_AREA => $TODAY"
echo "Backfilling last $DAYS days (not including today) for: ${BUS[*]}"

for BU in "${BUS[@]}"; do
  echo "=== Starting backfill for $BU ==="
  for (( i=DAYS; i>=1; i-- )); do
    TO_DATE=$(date -d "$TODAY - $i days" +%F)

    # Guardrail: never go past today
    if [[ "$TO_DATE" > "$TODAY" ]]; then
      echo "Skipping future date $TO_DATE"
      continue
    fi

    echo "Ingesting $BU for $TO_DATE"
    curl -s "$SERVICE/ingest/daily_wbr?bu=$BU&days=1&to=$TO_DATE" >/dev/null || true
    sleep 2
  done
  echo "=== Finished $BU ==="
done

echo "All done."
