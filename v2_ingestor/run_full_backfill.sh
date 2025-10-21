#!/bin/bash

# Full Backfill Script - Sequential Execution
# This runs each entity one at a time to avoid BigQuery payload size limits

set -e  # Exit on error

# Get Cloud Run service URL
echo "Getting Cloud Run service URL..."
SERVICE_URL=$(gcloud run services describe st-v2-ingestor \
  --region=us-central1 \
  --format='value(status.url)')

if [ -z "$SERVICE_URL" ]; then
  echo "ERROR: Could not get Cloud Run service URL"
  exit 1
fi

echo "Service URL: $SERVICE_URL"
echo ""
echo "=========================================="
echo "Starting Sequential Full Backfill"
echo "=========================================="
echo ""

# Function to run entity and wait
run_entity() {
  local entity=$1
  local wait_time=$2

  echo "----------------------------------------"
  echo "Entity: $entity"
  echo "Started: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "----------------------------------------"

  curl -s "$SERVICE_URL/ingest/$entity?mode=full" | jq '.'

  echo ""
  echo "Completed: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "Waiting ${wait_time}s before next entity..."
  echo ""

  sleep $wait_time
}

# Small entities first (fastest)
run_entity "campaigns" 10
run_entity "locations" 10
run_entity "customers" 15

# Medium entities
run_entity "estimates" 30
run_entity "jobs" 30

# Large entities (may take 5-10 minutes each)
echo "=========================================="
echo "Starting Large Entities (10-15min each)"
echo "=========================================="
echo ""

run_entity "invoices" 60
run_entity "payments" 60
run_entity "payroll" 60

echo "=========================================="
echo "Full Backfill Complete!"
echo "Completed: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Verify data with: ./check_backfill_status.sh"
echo "2. Deploy KPI marts: bq query < st_mart_v2_kpis.sql"
echo "3. Set up Cloud Scheduler for daily incremental sync"
