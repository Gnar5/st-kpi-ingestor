#!/bin/bash

# Create YoY (Year-over-Year) schedulers that run Mondays at 1:00 AM Arizona time
# These will pull the same week from last year for comparison

BASE_URL="https://st-kpi-ingestor-999875365235.us-central1.run.app"
REGION="us-central1"
TIMEZONE="America/Phoenix"

# Calculate date range for last year (52 weeks ago, 7-day window)
# We'll use a helper Cloud Run endpoint or script to calculate this dynamically
# For now, we'll create schedulers that pull from a specific offset

echo "Creating YoY schedulers..."

# Leads YoY - Monday 1:00 AM
gcloud scheduler jobs create http leads-yoy-ingest \
  --location=$REGION \
  --schedule="0 1 * * 1" \
  --time-zone=$TIMEZONE \
  --uri="$BASE_URL/ingest/leads?days=7&offset_days=365" \
  --http-method=GET \
  --attempt-deadline=600s \
  --description="Weekly YoY ingestion for Leads (same week last year)"

echo "✓ Leads YoY scheduler created"

# Foreman YoY - Monday 1:05 AM
gcloud scheduler jobs create http foreman-yoy-ingest \
  --location=$REGION \
  --schedule="5 1 * * 1" \
  --time-zone=$TIMEZONE \
  --uri="$BASE_URL/ingest/foreman?days=7&offset_days=365" \
  --http-method=GET \
  --attempt-deadline=600s \
  --description="Weekly YoY ingestion for Foreman (same week last year)"

echo "✓ Foreman YoY scheduler created"

# Collections YoY - Monday 1:10 AM
gcloud scheduler jobs create http collections-yoy-ingest \
  --location=$REGION \
  --schedule="10 1 * * 1" \
  --time-zone=$TIMEZONE \
  --uri="$BASE_URL/ingest/collections?days=7&offset_days=365" \
  --http-method=GET \
  --attempt-deadline=600s \
  --description="Weekly YoY ingestion for Collections (same week last year)"

echo "✓ Collections YoY scheduler created"

# Daily WBR YoY schedulers - Monday 1:15 AM onwards (staggered by 10 minutes each)
BUS=(
  "Andy's Painting-Sales:15"
  "Commercial-AZ-Sales:25"
  "Guaranteed Painting-Sales:35"
  "Nevada-Sales:45"
  "Phoenix-Sales:55"
  "Tucson-Sales:65"
)

for entry in "${BUS[@]}"; do
  IFS=':' read -r BU MINUTE <<< "$entry"
  BU_ENCODED=$(echo "$BU" | sed 's/ /%20/g' | sed "s/'/%27/g")
  JOB_NAME=$(echo "wbr-yoy-${BU}" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9-]/-/g')
  
  # Calculate hour and minute (handle overflow past 60 minutes)
  HOUR=$((1 + MINUTE / 60))
  MIN=$((MINUTE % 60))
  
  gcloud scheduler jobs create http "$JOB_NAME" \
    --location=$REGION \
    --schedule="$MIN $HOUR * * 1" \
    --time-zone=$TIMEZONE \
    --uri="$BASE_URL/ingest/daily_wbr?bu=$BU_ENCODED&days=7&offset_days=365" \
    --http-method=GET \
    --attempt-deadline=600s \
    --description="Weekly YoY ingestion for Daily WBR - $BU (same week last year)"
  
  echo "✓ Daily WBR YoY scheduler created for $BU"
done

echo ""
echo "=========================================="
echo "All YoY schedulers created successfully!"
echo "=========================================="
echo ""
echo "Schedule summary (all times in America/Phoenix):"
echo "  Monday 1:00 AM - Leads YoY"
echo "  Monday 1:05 AM - Foreman YoY"
echo "  Monday 1:10 AM - Collections YoY"
echo "  Monday 1:15 AM - Daily WBR YoY (Andy's)"
echo "  Monday 1:25 AM - Daily WBR YoY (Commercial-AZ)"
echo "  Monday 1:35 AM - Daily WBR YoY (Guaranteed)"
echo "  Monday 1:45 AM - Daily WBR YoY (Nevada)"
echo "  Monday 1:55 AM - Daily WBR YoY (Phoenix)"
echo "  Monday 2:05 AM - Daily WBR YoY (Tucson)"
echo ""
