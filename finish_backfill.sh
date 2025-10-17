#!/bin/bash
SERVICE="https://st-kpi-ingestor-999875365235.us-central1.run.app"
BUS=("Phoenix-Sales" "Commercial-AZ-Sales")

for bu in "${BUS[@]}"; do
  echo "=== Backfilling $bu ==="
  for i in {1..400}; do
    date=$(date -d "today - $i days" +%Y-%m-%d)
    curl -s "$SERVICE/ingest/daily_wbr?bu=$bu&days=1&to=$date" >/dev/null 2>&1
    sleep 2
  done
done
