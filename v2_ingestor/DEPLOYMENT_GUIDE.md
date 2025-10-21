# Deployment Guide - ServiceTitan v2 Ingestor

Complete step-by-step guide for deploying the v2 ingestor to Google Cloud Run.

---

## Pre-Deployment Checklist

- [ ] ServiceTitan API credentials obtained
- [ ] Google Cloud Project ID: `kpi-auto-471020`
- [ ] `gcloud` CLI installed and authenticated
- [ ] BigQuery API enabled
- [ ] Cloud Run API enabled
- [ ] Cloud Scheduler API enabled
- [ ] Billing enabled on GCP project

---

## Step 1: Local Setup & Testing

### 1.1 Clone and Install

```bash
cd v2_ingestor
npm install
```

### 1.2 Configure Environment

```bash
cp .env.example .env
```

Edit `.env`:
```bash
ST_CLIENT_ID=your_actual_client_id
ST_CLIENT_SECRET=your_actual_secret
ST_TENANT_ID=your_tenant_id
ST_APP_KEY=your_app_key

BQ_PROJECT_ID=kpi-auto-471020
BQ_DATASET_RAW=st_raw_v2
BQ_DATASET_STAGE=st_stage_v2
BQ_DATASET_MART=st_mart_v2
BQ_DATASET_LOGS=st_logs_v2
```

### 1.3 Create BigQuery Datasets

```bash
bq mk --dataset --location=US kpi-auto-471020:st_raw_v2
bq mk --dataset --location=US kpi-auto-471020:st_stage_v2
bq mk --dataset --location=US kpi-auto-471020:st_mart_v2
bq mk --dataset --location=US kpi-auto-471020:st_logs_v2
```

### 1.4 Create BigQuery Tables

```bash
bq query --use_legacy_sql=false < bigquery_schemas.sql
```

Or create programmatically on first run (tables auto-create).

### 1.5 Test Locally

```bash
npm start
```

In another terminal:
```bash
# Health check
curl http://localhost:8080/health

# Test single entity
curl http://localhost:8080/ingest/campaigns

# Test all entities (sequential)
curl http://localhost:8080/ingest-all
```

**Verify**:
- Check logs for successful authentication
- Check BigQuery tables for data
- Query `st_logs_v2.ingestion_logs` for run metadata

---

## Step 2: Deploy to Cloud Run

### 2.1 Set GCP Project

```bash
gcloud config set project kpi-auto-471020
gcloud config set run/region us-central1
```

### 2.2 Enable Required APIs

```bash
gcloud services enable run.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
```

### 2.3 Deploy Service

**Option A: Direct deployment (uses Cloud Build)**
```bash
gcloud run deploy st-v2-ingestor \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars="ST_CLIENT_ID=${ST_CLIENT_ID},ST_CLIENT_SECRET=${ST_CLIENT_SECRET},ST_TENANT_ID=${ST_TENANT_ID},ST_APP_KEY=${ST_APP_KEY},BQ_PROJECT_ID=kpi-auto-471020,BQ_DATASET_RAW=st_raw_v2,BQ_DATASET_STAGE=st_stage_v2,BQ_DATASET_MART=st_mart_v2,BQ_DATASET_LOGS=st_logs_v2,NODE_ENV=production,LOG_LEVEL=info" \
  --memory 2Gi \
  --cpu 2 \
  --timeout 3600 \
  --max-instances 5 \
  --min-instances 0 \
  --concurrency 10
```

**Option B: Build and deploy separately**
```bash
# Build container
gcloud builds submit --tag gcr.io/kpi-auto-471020/st-v2-ingestor

# Deploy
gcloud run deploy st-v2-ingestor \
  --image gcr.io/kpi-auto-471020/st-v2-ingestor \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars="ST_CLIENT_ID=${ST_CLIENT_ID},...(same as above)" \
  --memory 2Gi \
  --cpu 2 \
  --timeout 3600 \
  --max-instances 5 \
  --min-instances 0
```

### 2.4 Verify Deployment

```bash
# Get service URL
SERVICE_URL=$(gcloud run services describe st-v2-ingestor --region us-central1 --format 'value(status.url)')

echo "Service URL: $SERVICE_URL"

# Test health endpoint
curl $SERVICE_URL/health

# Test ingestion
curl "$SERVICE_URL/ingest/campaigns"
```

---

## Step 3: Configure Secrets (Recommended)

Instead of passing credentials via environment variables, use Secret Manager:

### 3.1 Create Secrets

```bash
echo -n "$ST_CLIENT_SECRET" | gcloud secrets create st-client-secret --data-file=-
echo -n "$ST_APP_KEY" | gcloud secrets create st-app-key --data-file=-
```

### 3.2 Grant Access to Cloud Run Service Account

```bash
# Get service account
SA=$(gcloud run services describe st-v2-ingestor --region us-central1 --format 'value(spec.template.spec.serviceAccountName)')

# If no service account, create one
gcloud iam service-accounts create st-v2-ingestor --display-name "ServiceTitan v2 Ingestor"
SA="st-v2-ingestor@kpi-auto-471020.iam.gserviceaccount.com"

# Grant secret access
gcloud secrets add-iam-policy-binding st-client-secret \
  --member="serviceAccount:$SA" \
  --role="roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding st-app-key \
  --member="serviceAccount:$SA" \
  --role="roles/secretmanager.secretAccessor"
```

### 3.3 Update Service to Use Secrets

```bash
gcloud run services update st-v2-ingestor \
  --region us-central1 \
  --update-secrets=ST_CLIENT_SECRET=st-client-secret:latest,ST_APP_KEY=st-app-key:latest \
  --set-env-vars="ST_CLIENT_ID=${ST_CLIENT_ID},ST_TENANT_ID=${ST_TENANT_ID},BQ_PROJECT_ID=kpi-auto-471020,..."
```

---

## Step 4: Set Up Cloud Scheduler

### 4.1 Create Scheduler Jobs

**Jobs (every 2 hours)**
```bash
gcloud scheduler jobs create http jobs-sync-v2 \
  --location=us-central1 \
  --schedule="0 */2 * * *" \
  --uri="$SERVICE_URL/ingest/jobs" \
  --http-method=GET \
  --time-zone="America/Los_Angeles" \
  --description="Sync jobs from ServiceTitan every 2 hours"
```

**Invoices (every 2 hours)**
```bash
gcloud scheduler jobs create http invoices-sync-v2 \
  --location=us-central1 \
  --schedule="0 */2 * * *" \
  --uri="$SERVICE_URL/ingest/invoices" \
  --http-method=GET \
  --time-zone="America/Los_Angeles"
```

**Estimates (every 2 hours)**
```bash
gcloud scheduler jobs create http estimates-sync-v2 \
  --location=us-central1 \
  --schedule="0 */2 * * *" \
  --uri="$SERVICE_URL/ingest/estimates" \
  --http-method=GET \
  --time-zone="America/Los_Angeles"
```

**Payments (every 2 hours)**
```bash
gcloud scheduler jobs create http payments-sync-v2 \
  --location=us-central1 \
  --schedule="0 */2 * * *" \
  --uri="$SERVICE_URL/ingest/payments" \
  --http-method=GET \
  --time-zone="America/Los_Angeles"
```

**Customers (every 6 hours)**
```bash
gcloud scheduler jobs create http customers-sync-v2 \
  --location=us-central1 \
  --schedule="0 */6 * * *" \
  --uri="$SERVICE_URL/ingest/customers" \
  --http-method=GET \
  --time-zone="America/Los_Angeles"
```

**Locations (every 6 hours)**
```bash
gcloud scheduler jobs create http locations-sync-v2 \
  --location=us-central1 \
  --schedule="0 */6 * * *" \
  --uri="$SERVICE_URL/ingest/locations" \
  --http-method=GET \
  --time-zone="America/Los_Angeles"
```

**Payroll (daily at 8 AM)**
```bash
gcloud scheduler jobs create http payroll-sync-v2 \
  --location=us-central1 \
  --schedule="0 8 * * *" \
  --uri="$SERVICE_URL/ingest/payroll" \
  --http-method=GET \
  --time-zone="America/Los_Angeles"
```

**Campaigns (daily at midnight)**
```bash
gcloud scheduler jobs create http campaigns-sync-v2 \
  --location=us-central1 \
  --schedule="0 0 * * *" \
  --uri="$SERVICE_URL/ingest/campaigns" \
  --http-method=GET \
  --time-zone="America/Los_Angeles"
```

**All entities in parallel (daily at 2 AM)**
```bash
gcloud scheduler jobs create http all-entities-sync-v2 \
  --location=us-central1 \
  --schedule="0 2 * * *" \
  --uri="$SERVICE_URL/ingest-all?parallel=true" \
  --http-method=GET \
  --time-zone="America/Los_Angeles" \
  --attempt-deadline=3600s
```

**Weekly full sync (Sunday at 3 AM)**
```bash
gcloud scheduler jobs create http weekly-full-sync-v2 \
  --location=us-central1 \
  --schedule="0 3 * * 0" \
  --uri="$SERVICE_URL/ingest-all?mode=full&parallel=true" \
  --http-method=GET \
  --time-zone="America/Los_Angeles" \
  --attempt-deadline=7200s
```

### 4.2 Pause All Schedulers (Initial Setup)

```bash
gcloud scheduler jobs pause jobs-sync-v2 --location=us-central1
gcloud scheduler jobs pause invoices-sync-v2 --location=us-central1
# ... repeat for all
```

### 4.3 Test Manually

```bash
gcloud scheduler jobs run jobs-sync-v2 --location=us-central1
```

### 4.4 Resume Schedulers After Validation

```bash
gcloud scheduler jobs resume jobs-sync-v2 --location=us-central1
gcloud scheduler jobs resume invoices-sync-v2 --location=us-central1
# ... repeat for all
```

---

## Step 5: Monitoring & Alerts

### 5.1 View Logs

**Cloud Logging**:
```bash
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=st-v2-ingestor" \
  --limit 50 \
  --format json
```

**Filter by severity**:
```bash
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=st-v2-ingestor AND severity>=ERROR" \
  --limit 20
```

### 5.2 Query Ingestion Logs

```sql
-- Recent runs
SELECT entity_type, start_time, status, records_inserted, duration_ms
FROM `kpi-auto-471020.st_logs_v2.ingestion_logs`
WHERE DATE(start_time) = CURRENT_DATE()
ORDER BY start_time DESC;

-- Failed runs
SELECT entity_type, start_time, error_message
FROM `kpi-auto-471020.st_logs_v2.ingestion_logs`
WHERE status = 'failed'
  AND start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY start_time DESC;

-- Success rate by entity
SELECT
  entity_type,
  COUNT(*) AS total_runs,
  COUNTIF(status = 'success') AS successful_runs,
  ROUND(COUNTIF(status = 'success') / COUNT(*) * 100, 2) AS success_rate_pct
FROM `kpi-auto-471020.st_logs_v2.ingestion_logs`
WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY entity_type;
```

### 5.3 Set Up Alerting (Optional)

Create alert policy for failed runs:

```bash
# Via gcloud (or use Cloud Console UI)
gcloud alpha monitoring policies create \
  --notification-channels=YOUR_CHANNEL_ID \
  --display-name="V2 Ingestor - Failed Runs" \
  --condition-display-name="Failed ingestion runs" \
  --condition-threshold-value=1 \
  --condition-threshold-duration=300s \
  --aggregation-alignment-period=300s
```

---

## Step 6: Cost Optimization

### 6.1 Review Cloud Run Settings

```bash
gcloud run services describe st-v2-ingestor --region us-central1
```

Optimize:
- `--min-instances=0` (scale to zero when idle)
- `--max-instances=5` (prevent runaway costs)
- `--memory=2Gi` (adequate for most workloads)
- `--cpu=2` (balance speed vs cost)
- `--concurrency=10` (handle multiple requests per instance)

### 6.2 Monitor Costs

**Cloud Run costs**:
```bash
gcloud billing projects describe kpi-auto-471020
```

**BigQuery storage**:
```sql
SELECT
  table_schema,
  table_name,
  ROUND(size_bytes / POW(10, 9), 2) AS size_gb,
  row_count
FROM `kpi-auto-471020.st_raw_v2.__TABLES__`
ORDER BY size_bytes DESC;
```

---

## Step 7: Parallel Operation with V1

### Run V1 and V2 Side-by-Side

**V1 Service**: `st-kpi-ingestor` (existing)
**V2 Service**: `st-v2-ingestor` (new)

**V1 Datasets**: `st_raw`, `st_stage`, `st_mart`
**V2 Datasets**: `st_raw_v2`, `st_stage_v2`, `st_mart_v2`

**V1 Schedulers**: `wbr-sync`, `leads-sync`, etc.
**V2 Schedulers**: `jobs-sync-v2`, `invoices-sync-v2`, etc.

**Both can run simultaneously without conflict.**

### Validation Period

Run both for 2-4 weeks:
- Compare record counts
- Validate data accuracy
- Monitor performance
- Fix any issues in V2

---

## Step 8: Cutover to V2

### When V2 is validated:

1. **Pause V1 schedulers**:
   ```bash
   gcloud scheduler jobs pause wbr-sync --location=us-central1
   gcloud scheduler jobs pause leads-sync --location=us-central1
   # ... all v1 jobs
   ```

2. **Update Looker Studio dashboards** to point to `st_mart_v2`

3. **Monitor V2** for 1 week

4. **Decommission V1**:
   ```bash
   gcloud run services delete st-kpi-ingestor --region us-central1
   gcloud scheduler jobs delete wbr-sync --location=us-central1
   # ... all v1 resources
   ```

5. **(Optional) Archive V1 datasets**:
   ```bash
   bq cp st_raw.raw_leads st_archive.raw_leads_v1
   # ... then delete st_raw if no longer needed
   ```

---

## Rollback Plan

If V2 has issues:

1. **Pause V2 schedulers**
2. **Resume V1 schedulers**
3. **Revert dashboard changes**
4. **Investigate and fix V2**
5. **Retry cutover**

---

## Troubleshooting Deployment

### Issue: Authentication Failed

**Error**: `ServiceTitan authentication failed: 401`

**Fix**:
```bash
# Verify credentials
echo $ST_CLIENT_ID
echo $ST_TENANT_ID

# Update Cloud Run env vars
gcloud run services update st-v2-ingestor \
  --region us-central1 \
  --set-env-vars="ST_CLIENT_ID=correct_value,ST_CLIENT_SECRET=correct_value"
```

### Issue: BigQuery Permission Denied

**Error**: `BigQuery insert failed: 403 Permission denied`

**Fix**:
```bash
# Grant BQ permissions to service account
SA="st-v2-ingestor@kpi-auto-471020.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding kpi-auto-471020 \
  --member="serviceAccount:$SA" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding kpi-auto-471020 \
  --member="serviceAccount:$SA" \
  --role="roles/bigquery.jobUser"
```

### Issue: Cloud Run Timeout

**Error**: `Timeout after 3600s`

**Fix**:
```bash
gcloud run services update st-v2-ingestor \
  --region us-central1 \
  --timeout=7200
```

### Issue: Out of Memory

**Error**: `Container killed due to memory limit`

**Fix**:
```bash
gcloud run services update st-v2-ingestor \
  --region us-central1 \
  --memory=4Gi
```

---

## Post-Deployment Checklist

- [ ] Service deployed successfully
- [ ] Health endpoint responds: `curl $SERVICE_URL/health`
- [ ] Test ingestion works: `curl $SERVICE_URL/ingest/campaigns`
- [ ] BigQuery tables populated
- [ ] Scheduler jobs created and paused
- [ ] Manual test of each scheduler successful
- [ ] Logs show no errors
- [ ] Alerts configured (optional)
- [ ] Documentation updated
- [ ] Team notified

---

## Maintenance

### Update Service

```bash
# Make code changes
git commit -am "Update logic"

# Redeploy
gcloud run deploy st-v2-ingestor --source . --region us-central1
```

### View Service Revisions

```bash
gcloud run revisions list --service st-v2-ingestor --region us-central1
```

### Rollback to Previous Revision

```bash
gcloud run services update-traffic st-v2-ingestor \
  --region us-central1 \
  --to-revisions=st-v2-ingestor-00001-abc=100
```

---

**Deployment complete! The v2 ingestor is now running in production.**
