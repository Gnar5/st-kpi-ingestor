#!/bin/bash

# Run Daily WBR backfill in smaller chunks to avoid timeouts
# This breaks 365 days into multiple 30-day jobs
# Usage: ./backfill-daily-wbr-yearly.sh

REGION="us-central1"
JOB_NAME="st-kpi-backfill"
TOTAL_DAYS=365
CHUNK_SIZE=30

echo "=========================================="
echo "Daily WBR 1-Year Backfill Strategy"
echo "=========================================="
echo ""
echo "Breaking ${TOTAL_DAYS} days into ${CHUNK_SIZE}-day chunks"
echo "This will create multiple backfill jobs that can resume"
echo ""

# Calculate number of chunks needed
NUM_CHUNKS=$(( (TOTAL_DAYS + CHUNK_SIZE - 1) / CHUNK_SIZE ))

echo "Will run ${NUM_CHUNKS} separate backfill jobs"
echo ""

for (( i=0; i<NUM_CHUNKS; i++ )); do
  START_DAY=$(( TOTAL_DAYS - (i+1) * CHUNK_SIZE ))
  if [ $START_DAY -lt 0 ]; then
    START_DAY=0
  fi

  DAYS_IN_CHUNK=$(( TOTAL_DAYS - i * CHUNK_SIZE ))
  if [ $DAYS_IN_CHUNK -gt $CHUNK_SIZE ]; then
    DAYS_IN_CHUNK=$CHUNK_SIZE
  fi

  echo "=========================================="
  echo "Chunk $((i+1))/$NUM_CHUNKS: Backfilling ${DAYS_IN_CHUNK} days"
  echo "Date range: $(date -d "${START_DAY} days ago" +%Y-%m-%d) to $(date -d "$((START_DAY + DAYS_IN_CHUNK)) days ago" +%Y-%m-%d)"
  echo "=========================================="

  # Run the backfill job
  gcloud run jobs execute $JOB_NAME \
    --region $REGION \
    --args="${DAYS_IN_CHUNK},--only=daily_wbr,--window=1" \
    --async

  EXECUTION_ID=$?

  echo "Started execution with ID: $EXECUTION_ID"
  echo ""

  # Wait 5 minutes between chunks to stagger the load
  if [ $i -lt $((NUM_CHUNKS - 1)) ]; then
    echo "Waiting 5 minutes before starting next chunk..."
    sleep 300
  fi
done

echo "=========================================="
echo "All Daily WBR backfill jobs started!"
echo "=========================================="
echo ""
echo "To monitor progress:"
echo "  gcloud run jobs executions list --job $JOB_NAME --region $REGION"
echo ""
echo "To view logs:"
echo "  gcloud logging tail \"resource.type=cloud_run_job AND resource.labels.job_name=$JOB_NAME\""
echo ""
echo "Note: Each chunk will take 1-2 hours due to rate limits"
echo "Total estimated time: 12-24 hours for all chunks"
