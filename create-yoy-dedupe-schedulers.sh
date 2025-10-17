#!/bin/bash

# Create YoY dedupe schedulers that run after all YoY ingestion completes
# These run on Monday mornings after the YoY data has been loaded

BASE_URL="https://st-kpi-ingestor-999875365235.us-central1.run.app"
REGION="us-central1"
TIMEZONE="America/Phoenix"

echo "Creating YoY dedupe schedulers..."

# Leads YoY dedupe - Monday 2:10 AM (after all YoY ingests complete)
gcloud scheduler jobs create http leads-yoy-dedupe \
  --location=$REGION \
  --schedule="10 2 * * 1" \
  --time-zone=$TIMEZONE \
  --uri="$BASE_URL/dedupe/leads" \
  --http-method=POST \
  --attempt-deadline=600s \
  --description="YoY dedupe for Leads (Monday 2:10am after YoY ingestion)"

echo "✓ Leads YoY dedupe scheduler created"

# Foreman YoY dedupe - Monday 2:15 AM
gcloud scheduler jobs create http foreman-yoy-dedupe \
  --location=$REGION \
  --schedule="15 2 * * 1" \
  --time-zone=$TIMEZONE \
  --uri="$BASE_URL/dedupe/foreman" \
  --http-method=POST \
  --attempt-deadline=600s \
  --description="YoY dedupe for Foreman (Monday 2:15am after YoY ingestion)"

echo "✓ Foreman YoY dedupe scheduler created"

# Collections YoY dedupe - Monday 2:20 AM
gcloud scheduler jobs create http collections-yoy-dedupe \
  --location=$REGION \
  --schedule="20 2 * * 1" \
  --time-zone=$TIMEZONE \
  --uri="$BASE_URL/dedupe/collections" \
  --http-method=POST \
  --attempt-deadline=600s \
  --description="YoY dedupe for Collections (Monday 2:20am after YoY ingestion)"

echo "✓ Collections YoY dedupe scheduler created"

# Daily WBR YoY dedupe - Monday 2:25 AM (after all 6 BUs complete)
gcloud scheduler jobs create http wbr-yoy-dedupe \
  --location=$REGION \
  --schedule="25 2 * * 1" \
  --time-zone=$TIMEZONE \
  --uri="$BASE_URL/dedupe/daily_wbr" \
  --http-method=POST \
  --attempt-deadline=600s \
  --description="YoY dedupe for Daily WBR (Monday 2:25am after all BU YoY ingestion)"

echo "✓ Daily WBR YoY dedupe scheduler created"

echo ""
echo "=========================================="
echo "All YoY dedupe schedulers created!"
echo "=========================================="
echo ""
echo "YoY Dedupe schedule (all times in America/Phoenix):"
echo "  Monday 2:10 AM - Leads YoY dedupe"
echo "  Monday 2:15 AM - Foreman YoY dedupe"
echo "  Monday 2:20 AM - Collections YoY dedupe"
echo "  Monday 2:25 AM - Daily WBR YoY dedupe"
echo ""
