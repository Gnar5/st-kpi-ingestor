#!/bin/bash

# Run backfill for each report separately to avoid rate limits
# Usage: ./run-backfill-separate.sh [days]

DAYS=${1:-365}
REGION="us-central1"
JOB_NAME="st-kpi-backfill"

echo "Running backfill for ${DAYS} days, one report at a time..."
echo ""

# Array of reports to backfill
REPORTS=("leads" "foreman" "collections" "daily_wbr")

for REPORT in "${REPORTS[@]}"; do
  echo "============================================"
  echo "Starting backfill for: $REPORT"
  echo "============================================"

  gcloud run jobs execute $JOB_NAME \
    --region $REGION \
    --update-env-vars BACKFILL_DAYS=$DAYS \
    --args="--only=$REPORT" \
    --wait

  EXIT_CODE=$?

  if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ $REPORT backfill completed successfully"
  else
    echo "✗ $REPORT backfill failed with exit code $EXIT_CODE"
    echo "Check logs: gcloud logging read \"resource.type=cloud_run_job AND resource.labels.job_name=$JOB_NAME\" --limit 50"

    read -p "Continue with next report? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
      echo "Stopping backfill process"
      exit 1
    fi
  fi

  echo ""
  echo "Waiting 60 seconds before next report to avoid rate limits..."
  sleep 60
  echo ""
done

echo "============================================"
echo "All backfills complete!"
echo "============================================"
echo ""
echo "To verify data, run:"
echo "  bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM \`kpi-auto-471020.st_raw.raw_leads\`'"
echo "  bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM \`kpi-auto-471020.st_raw.raw_foreman\`'"
echo "  bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM \`kpi-auto-471020.st_raw.raw_collections\`'"
echo "  bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM \`kpi-auto-471020.st_raw.raw_daily_wbr_v2\`'"
