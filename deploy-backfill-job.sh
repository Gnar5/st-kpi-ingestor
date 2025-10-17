#!/bin/bash

# Deploy backfill as Cloud Run Job
# Usage: ./deploy-backfill-job.sh

PROJECT_ID="kpi-auto-471020"
REGION="us-central1"
JOB_NAME="st-kpi-backfill"

echo "Building and deploying backfill job..."

gcloud run jobs deploy $JOB_NAME \
  --source . \
  --dockerfile Dockerfile.backfill \
  --region $REGION \
  --project $PROJECT_ID \
  --memory 1Gi \
  --cpu 1 \
  --max-retries 0 \
  --task-timeout 3600s \
  --set-env-vars BACKFILL_DAYS=365 \
  --set-env-vars BACKFILL_WINDOW=7

echo ""
echo "============================================"
echo "Backfill job deployed successfully!"
echo ""
echo "To run the backfill job:"
echo "  gcloud run jobs execute $JOB_NAME --region $REGION"
echo ""
echo "To run with custom days (e.g., 30 days):"
echo "  gcloud run jobs execute $JOB_NAME --region $REGION --update-env-vars BACKFILL_DAYS=30"
echo ""
echo "To run full 2 years:"
echo "  gcloud run jobs execute $JOB_NAME --region $REGION --update-env-vars BACKFILL_DAYS=730"
echo ""
echo "To check job status:"
echo "  gcloud run jobs executions list --job $JOB_NAME --region $REGION"
echo ""
echo "To view logs:"
echo "  gcloud run jobs executions describe EXECUTION_NAME --region $REGION"
echo "============================================"
