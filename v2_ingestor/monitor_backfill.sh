#!/bin/bash

# Monitor Async Backfill Progress
# Shows recent Cloud Run logs

echo "=========================================="
echo "Monitoring Cloud Run Backfill Logs"
echo "Press Ctrl+C to stop monitoring"
echo "=========================================="
echo ""

# Simple log tailing without fancy formatting that causes gcloud to crash
gcloud run services logs read st-v2-ingestor \
  --region=us-central1 \
  --limit=100

echo ""
echo "=========================================="
echo "To check data status, run:"
echo "./check_backfill_status.sh"
echo "=========================================="
