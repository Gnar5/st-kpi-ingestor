#!/bin/bash
# ServiceTitan v2 Ingestor - Quick Setup Script
# This script helps set up the local development environment

set -e

echo "========================================="
echo "ServiceTitan v2 Ingestor - Setup"
echo "========================================="
echo ""

# Check Node.js version
echo "Checking Node.js version..."
NODE_VERSION=$(node -v | cut -d'v' -f2 | cut -d'.' -f1)
if [ "$NODE_VERSION" -lt 20 ]; then
    echo "❌ Error: Node.js version 20 or higher required"
    echo "   Current version: $(node -v)"
    exit 1
fi
echo "✅ Node.js version OK: $(node -v)"
echo ""

# Check if .env exists
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo "⚠️  Please edit .env with your ServiceTitan credentials"
    echo ""
else
    echo "✅ .env file already exists"
    echo ""
fi

# Install dependencies
echo "Installing dependencies..."
npm install
echo "✅ Dependencies installed"
echo ""

# Check gcloud CLI
if command -v gcloud &> /dev/null; then
    echo "✅ gcloud CLI found: $(gcloud version | head -1)"
    GCLOUD_PROJECT=$(gcloud config get-value project 2>/dev/null)
    if [ -n "$GCLOUD_PROJECT" ]; then
        echo "   Current project: $GCLOUD_PROJECT"
    else
        echo "   No project set. Run: gcloud config set project kpi-auto-471020"
    fi
else
    echo "⚠️  gcloud CLI not found (optional for local development)"
    echo "   Install from: https://cloud.google.com/sdk/docs/install"
fi
echo ""

# Check bq CLI
if command -v bq &> /dev/null; then
    echo "✅ BigQuery CLI found"
else
    echo "⚠️  BigQuery CLI not found (optional for local development)"
fi
echo ""

echo "========================================="
echo "Setup Complete!"
echo "========================================="
echo ""
echo "Next steps:"
echo ""
echo "1. Edit .env with your credentials:"
echo "   nano .env"
echo ""
echo "2. Create BigQuery datasets (if not exists):"
echo "   bq mk --dataset --location=US kpi-auto-471020:st_raw_v2"
echo "   bq mk --dataset --location=US kpi-auto-471020:st_stage_v2"
echo "   bq mk --dataset --location=US kpi-auto-471020:st_mart_v2"
echo "   bq mk --dataset --location=US kpi-auto-471020:st_logs_v2"
echo ""
echo "3. Create BigQuery tables:"
echo "   bq query --use_legacy_sql=false < bigquery_schemas.sql"
echo ""
echo "4. Start the service:"
echo "   npm start"
echo ""
echo "5. Test endpoints:"
echo "   curl http://localhost:8080/health"
echo "   curl http://localhost:8080/ingest/campaigns"
echo ""
echo "For deployment to Cloud Run, see: DEPLOYMENT_GUIDE.md"
echo ""
