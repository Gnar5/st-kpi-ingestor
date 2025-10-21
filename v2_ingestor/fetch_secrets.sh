#!/bin/bash
# Fetch ServiceTitan credentials from Secret Manager and create .env

echo "Fetching secrets from Google Secret Manager..."

ST_TENANT_ID=$(gcloud secrets versions access latest --secret=ST_TENANT_ID 2>/dev/null)
ST_CLIENT_ID=$(gcloud secrets versions access latest --secret=ST_CLIENT_ID 2>/dev/null)
ST_CLIENT_SECRET=$(gcloud secrets versions access latest --secret=ST_CLIENT_SECRET 2>/dev/null)
ST_APP_KEY=$(gcloud secrets versions access latest --secret=ST_APP_KEY 2>/dev/null)

if [ -z "$ST_TENANT_ID" ]; then
    echo "❌ Error: Could not fetch ST_TENANT_ID from Secret Manager"
    echo "Make sure you're authenticated: gcloud auth application-default login"
    exit 1
fi

cat > .env << ENVFILE
# ServiceTitan API Configuration
ST_CLIENT_ID=$ST_CLIENT_ID
ST_CLIENT_SECRET=$ST_CLIENT_SECRET
ST_TENANT_ID=$ST_TENANT_ID
ST_APP_KEY=$ST_APP_KEY

# BigQuery Configuration
BQ_PROJECT_ID=kpi-auto-471020
BQ_DATASET_RAW=st_raw_v2
BQ_DATASET_STAGE=st_stage_v2
BQ_DATASET_MART=st_mart_v2
BQ_DATASET_LOGS=st_logs_v2

# Application Configuration
PORT=8080
NODE_ENV=development
LOG_LEVEL=info

# Rate Limiting
MAX_CONCURRENT_REQUESTS=5
RATE_LIMIT_PER_SECOND=10

# Sync Configuration
SYNC_MODE=incremental
FULL_SYNC_DAY=sunday
LOOKBACK_DAYS=7
ENVFILE

echo "✅ .env file created successfully!"
echo ""
echo "Credentials fetched:"
echo "  ST_TENANT_ID: ${ST_TENANT_ID:0:10}..."
echo "  ST_CLIENT_ID: ${ST_CLIENT_ID:0:10}..."
echo "  ST_APP_KEY: ${ST_APP_KEY:0:10}..."
echo ""
echo "You can now run: npm start"
