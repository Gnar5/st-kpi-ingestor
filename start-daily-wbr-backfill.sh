#!/bin/bash

# Simple script to start Daily WBR backfill
# Recommended: Start with 90 days first, then extend if needed
# Usage: ./start-daily-wbr-backfill.sh [days]

DAYS=${1:-90}
REGION="us-central1"
JOB_NAME="st-kpi-backfill"

echo "=========================================="
echo "Starting Daily WBR Backfill"
echo "=========================================="
echo ""
echo "Days: $DAYS"
echo "Window: 1 day (required for Daily WBR)"
echo ""
echo "Estimated time: $((DAYS / 10)) - $((DAYS / 5)) hours"
echo "API calls: ~$((DAYS * 6)) (6 BUs × $DAYS days)"
echo ""
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo "Cancelled"
  exit 0
fi

echo "Starting backfill job..."
gcloud run jobs execute $JOB_NAME \
  --region $REGION \
  --args="${DAYS},--only=daily_wbr,--window=1" \
  --async

echo ""
echo "=========================================="
echo "Daily WBR backfill started!"
echo "=========================================="
echo ""
echo "Monitor at:"
echo "  https://console.cloud.google.com/run/jobs/details/us-central1/st-kpi-backfill/executions"
echo ""
echo "If it times out (1 hour limit), just re-run this script"
echo "It will resume where it left off thanks to checkpointing"
echo ""
echo "To check progress:"
echo "  bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM \`kpi-auto-471020.st_raw.raw_daily_wbr_v2\`'"
