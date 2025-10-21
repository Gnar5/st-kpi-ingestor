# ServiceTitan v2 Ingestor - Historical Data Backfill Guide

**Current Data Status:** ⚠️ ONLY LAST 7 DAYS LOADED
**Date Range Tested:** October 14-21, 2025 (1 week)
**Total Records:** 2,684 entity records + 239 reference records

---

## Current State

### What We Have Now

Based on our test runs using **incremental mode** (modifiedSince filter), we currently have:

| Entity | Date Range | Record Count | Status |
|--------|-----------|--------------|--------|
| **Jobs** | Oct 14-21, 2025 | 158 | ✅ Last 7 days only |
| **Invoices** | Oct 14-21, 2025 | 550 | ✅ Last 7 days only |
| **Estimates** | Oct 14-21, 2025 | 350 | ✅ Last 7 days only |
| **Payments** | Oct 14-21, 2025 | 179 | ✅ Last 7 days only |
| **Payroll** | Sep 30 - Oct 26, 2025 | 534 | ✅ ~4 weeks |
| **Customers** | Oct 14-21, 2025 | 260 | ✅ Last 7 days only |
| **Locations** | Oct 14-21, 2025 | 289 | ✅ Last 7 days only |
| **Campaigns** | Oct 14-21, 2025 | 3 | ✅ Last 7 days only |

### What's Missing

❌ **Historical data before October 14, 2025**

This means:
- KPI marts will only show data for the past week
- Weekly rollups will be incomplete
- Year-to-date comparisons won't work
- Trend analysis is limited

---

## Why Only 7 Days?

The v2 ingestor uses **incremental sync** by default:
1. First run: `modifiedSince = 7 days ago` (default lookback)
2. Subsequent runs: `modifiedSince = last_sync_time`

This is perfect for **ongoing operations** but requires a **backfill** for historical data.

---

## Backfill Strategy

### Option 1: Full Refresh (Recommended for Initial Load)

**Best for:** Getting all historical data in one shot

**Pros:**
- Gets ALL data ServiceTitan has
- Simplest approach
- No date calculation needed

**Cons:**
- Slower (may take 10-30 minutes depending on data volume)
- Higher API usage

**How to Run:**

```bash
# Backfill ALL entities
curl "https://st-v2-ingestor-xxxxx.run.app/ingest-all?mode=full&parallel=true"

# Or backfill specific entities
curl "https://st-v2-ingestor-xxxxx.run.app/ingest/jobs?mode=full"
curl "https://st-v2-ingestor-xxxxx.run.app/ingest/invoices?mode=full"
curl "https://st-v2-ingestor-xxxxx.run.app/ingest/estimates?mode=full"
# ... etc for each entity
```

**Expected Results:**
- Jobs: ~5,000-50,000 records (depending on company age)
- Invoices: ~10,000-100,000 records
- Estimates: ~5,000-50,000 records
- Payments: ~5,000-50,000 records
- Payroll: ~50,000-500,000 records
- Customers: ~1,000-10,000 records
- Locations: ~1,000-10,000 records
- Campaigns: ~10-100 records

---

### Option 2: Windowed Backfill (For Very Large Datasets)

**Best for:** Companies with >1M records per entity (unlikely for painting contractors)

**Pros:**
- Safer for huge datasets
- Can pause/resume
- Easier to troubleshoot

**Cons:**
- More complex
- Requires custom script

**How to Run:**

```javascript
// backfill_by_month.js
const months = [
  '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01',
  '2024-05-01', '2024-06-01', '2024-07-01', '2024-08-01',
  '2024-09-01', '2024-10-01', '2024-11-01', '2024-12-01',
  '2025-01-01', '2025-02-01', '2025-03-01', '2025-04-01',
  '2025-05-01', '2025-06-01', '2025-07-01', '2025-08-01',
  '2025-09-01', '2025-10-01'
];

const baseUrl = 'https://st-v2-ingestor-xxxxx.run.app';
const entity = 'jobs';

for (let i = 0; i < months.length - 1; i++) {
  const startDate = months[i];
  const endDate = months[i + 1];

  console.log(`Backfilling ${entity} from ${startDate} to ${endDate}...`);

  // This would require modifying the ingestor to accept date ranges
  // Current implementation doesn't support this - use full refresh instead
  await fetch(`${baseUrl}/ingest/${entity}?mode=full`);

  await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5s between
}
```

**Note:** The current v2 ingestor doesn't support custom date ranges. You'd need to:
1. Use full refresh (Option 1), OR
2. Modify the ingestor to accept `modifiedOnOrAfter` and `modifiedBefore` parameters

---

### Option 3: Use ServiceTitan Export + BigQuery Load (Fastest for Huge Datasets)

**Best for:** Multi-million record datasets or one-time migration

**Pros:**
- Bypasses API rate limits
- Fastest for huge volumes
- Direct to BigQuery

**Cons:**
- Requires ServiceTitan data export feature
- Manual process
- May not match v2 schema exactly

**How to Run:**

```bash
# 1. Request data export from ServiceTitan
#    (Settings → Data Export → Request Export)

# 2. Download CSV/JSON files

# 3. Load to BigQuery
bq load \
  --source_format=CSV \
  --autodetect \
  kpi-auto-471020:st_raw_v2.raw_jobs_temp \
  gs://your-bucket/jobs_export.csv

# 4. Transform to match v2 schema
bq query --use_legacy_sql=false "
  INSERT INTO \`kpi-auto-471020.st_raw_v2.raw_jobs\`
  SELECT
    id,
    jobNumber,
    -- ... map all fields ...
    CURRENT_TIMESTAMP() AS _ingested_at,
    'servicetitan_export' AS _ingestion_source
  FROM \`kpi-auto-471020.st_raw_v2.raw_jobs_temp\`
"

# 5. Drop temp table
bq rm -f -t kpi-auto-471020:st_raw_v2.raw_jobs_temp
```

---

## Recommended Backfill Plan

### For Your Company (Assuming Medium-Sized Painting Contractor)

**Total Estimated Time:** 15-30 minutes
**Total Estimated Records:** 50,000-500,000

**Step-by-Step:**

#### 1. Run Full Refresh Locally First (Optional but Recommended)

```bash
# Start local server
cd v2_ingestor
npm start

# Test full refresh on ONE small entity
curl "http://localhost:8081/ingest/campaigns?mode=full"

# Check results
bq query --use_legacy_sql=false "
  SELECT COUNT(*), MIN(createdOn), MAX(createdOn)
  FROM \`kpi-auto-471020.st_raw_v2.raw_campaigns\`
"

# If looks good, proceed to production
```

#### 2. Deploy to Cloud Run

```bash
cd v2_ingestor

# Deploy (this pushes the code we just committed)
gcloud run deploy st-v2-ingestor \
  --source . \
  --region us-central1 \
  --platform managed \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 3600 \
  --set-env-vars PROJECT_ID=kpi-auto-471020

# Note the URL from output
# e.g., https://st-v2-ingestor-xxxxx-uc.a.run.app
```

#### 3. Run Full Backfill on All Entities

```bash
# Set your Cloud Run URL
SERVICE_URL="https://st-v2-ingestor-xxxxx-uc.a.run.app"

# Option A: All at once (parallel)
curl "$SERVICE_URL/ingest-all?mode=full&parallel=true"

# Option B: One by one (safer, easier to monitor)
curl "$SERVICE_URL/ingest/campaigns?mode=full"       # ~1 min
curl "$SERVICE_URL/ingest/customers?mode=full"       # ~2 min
curl "$SERVICE_URL/ingest/locations?mode=full"       # ~2 min
curl "$SERVICE_URL/ingest/jobs?mode=full"            # ~5 min
curl "$SERVICE_URL/ingest/estimates?mode=full"       # ~5 min
curl "$SERVICE_URL/ingest/invoices?mode=full"        # ~5 min
curl "$SERVICE_URL/ingest/payments?mode=full"        # ~5 min
curl "$SERVICE_URL/ingest/payroll?mode=full"         # ~10 min
```

#### 4. Verify Backfill Completeness

```bash
bq query --use_legacy_sql=false "
SELECT
  'jobs' AS entity,
  COUNT(*) AS total_records,
  MIN(DATE(completedOn)) AS earliest_date,
  MAX(DATE(completedOn)) AS latest_date,
  DATE_DIFF(MAX(DATE(completedOn)), MIN(DATE(completedOn)), DAY) AS days_of_data
FROM \`kpi-auto-471020.st_raw_v2.raw_jobs\`
WHERE completedOn IS NOT NULL

UNION ALL

SELECT
  'invoices',
  COUNT(*),
  MIN(DATE(createdOn)),
  MAX(DATE(createdOn)),
  DATE_DIFF(MAX(DATE(createdOn)), MIN(DATE(createdOn)), DAY)
FROM \`kpi-auto-471020.st_raw_v2.raw_invoices\`
WHERE createdOn IS NOT NULL

UNION ALL

SELECT
  'estimates',
  COUNT(*),
  MIN(DATE(createdOn)),
  MAX(DATE(createdOn)),
  DATE_DIFF(MAX(DATE(createdOn)), MIN(DATE(createdOn)), DAY)
FROM \`kpi-auto-471020.st_raw_v2.raw_estimates\`
WHERE createdOn IS NOT NULL

UNION ALL

SELECT
  'payroll',
  COUNT(*),
  MIN(DATE(date)),
  MAX(DATE(date)),
  DATE_DIFF(MAX(DATE(date)), MIN(DATE(date)), DAY)
FROM \`kpi-auto-471020.st_raw_v2.raw_payroll\`
WHERE date IS NOT NULL

ORDER BY entity
"
```

**Expected Results (Example Company):**
```
entity     | total_records | earliest_date | latest_date | days_of_data
-----------|---------------|---------------|-------------|-------------
estimates  | 45,230        | 2020-01-01    | 2025-10-21  | 2,119 days
invoices   | 67,891        | 2020-01-01    | 2025-10-21  | 2,119 days
jobs       | 52,345        | 2020-01-01    | 2025-10-21  | 2,119 days
payroll    | 234,567       | 2020-01-01    | 2025-10-26  | 2,124 days
```

#### 5. Refresh Reference Dimensions

```bash
# Backfill reference data (should be quick)
curl "$SERVICE_URL/ingest-ref-all"
```

#### 6. Run KPI Mart SQL

```bash
# Open BigQuery Console
# https://console.cloud.google.com/bigquery?project=kpi-auto-471020

# Paste entire contents of st_mart_v2_kpis.sql
# Click "Run"
# Wait ~2-3 minutes
```

#### 7. Verify KPI Marts Have Historical Data

```bash
bq query --use_legacy_sql=false "
SELECT
  week_start,
  bu_rollup,
  leads,
  estimates,
  total_booked,
  produced
FROM \`kpi-auto-471020.st_mart_v2.kpi_weekly_by_bu\`
WHERE bu_rollup = 'PHX'
  AND week_start >= '2025-01-01'
ORDER BY week_start DESC
LIMIT 20
"
```

**Expected:** Should see 40+ weeks of 2025 data

---

## After Backfill: Set Up Incremental Sync

Once backfill is complete, switch to **incremental mode** for daily updates:

### Create Cloud Scheduler Jobs

```bash
PROJECT_ID="kpi-auto-471020"
REGION="us-central1"
SERVICE_URL="https://st-v2-ingestor-xxxxx-uc.a.run.app"

# Jobs - Daily at 2:00 AM
gcloud scheduler jobs create http st-v2-jobs-daily \
  --project=$PROJECT_ID \
  --location=$REGION \
  --schedule="0 2 * * *" \
  --time-zone="America/Phoenix" \
  --uri="$SERVICE_URL/ingest/jobs?mode=incremental" \
  --http-method=GET

# Invoices - Daily at 2:05 AM
gcloud scheduler jobs create http st-v2-invoices-daily \
  --project=$PROJECT_ID \
  --location=$REGION \
  --schedule="5 2 * * *" \
  --time-zone="America/Phoenix" \
  --uri="$SERVICE_URL/ingest/invoices?mode=incremental" \
  --http-method=GET

# Estimates - Daily at 2:10 AM
gcloud scheduler jobs create http st-v2-estimates-daily \
  --project=$PROJECT_ID \
  --location=$REGION \
  --schedule="10 2 * * *" \
  --time-zone="America/Phoenix" \
  --uri="$SERVICE_URL/ingest/estimates?mode=incremental" \
  --http-method=GET

# Payments - Daily at 2:15 AM
gcloud scheduler jobs create http st-v2-payments-daily \
  --project=$PROJECT_ID \
  --location=$REGION \
  --schedule="15 2 * * *" \
  --time-zone="America/Phoenix" \
  --uri="$SERVICE_URL/ingest/payments?mode=incremental" \
  --http-method=GET

# Payroll - Daily at 2:20 AM
gcloud scheduler jobs create http st-v2-payroll-daily \
  --project=$PROJECT_ID \
  --location=$REGION \
  --schedule="20 2 * * *" \
  --time-zone="America/Phoenix" \
  --uri="$SERVICE_URL/ingest/payroll?mode=incremental" \
  --http-method=GET

# Customers - Daily at 2:25 AM
gcloud scheduler jobs create http st-v2-customers-daily \
  --project=$PROJECT_ID \
  --location=$REGION \
  --schedule="25 2 * * *" \
  --time-zone="America/Phoenix" \
  --uri="$SERVICE_URL/ingest/customers?mode=incremental" \
  --http-method=GET

# Locations - Daily at 2:30 AM
gcloud scheduler jobs create http st-v2-locations-daily \
  --project=$PROJECT_ID \
  --location=$REGION \
  --schedule="30 2 * * *" \
  --time-zone="America/Phoenix" \
  --uri="$SERVICE_URL/ingest/locations?mode=incremental" \
  --http-method=GET

# Campaigns - Daily at 2:35 AM
gcloud scheduler jobs create http st-v2-campaigns-daily \
  --project=$PROJECT_ID \
  --location=$REGION \
  --schedule="35 2 * * *" \
  --time-zone="America/Phoenix" \
  --uri="$SERVICE_URL/ingest/campaigns?mode=incremental" \
  --http-method=GET

# Reference dimensions - Daily at 3:00 AM
gcloud scheduler jobs create http st-ref-all-daily \
  --project=$PROJECT_ID \
  --location=$REGION \
  --schedule="0 3 * * *" \
  --time-zone="America/Phoenix" \
  --uri="$SERVICE_URL/ingest-ref-all" \
  --http-method=GET
```

---

## Monitoring Backfill Progress

### Check Cloud Run Logs

```bash
# View recent logs
gcloud run services logs read st-v2-ingestor \
  --region=us-central1 \
  --limit=100

# Follow logs in real-time
gcloud run services logs tail st-v2-ingestor \
  --region=us-central1
```

### Check BigQuery Ingestion Logs

```sql
-- View recent runs
SELECT
  entity_type,
  start_time,
  end_time,
  status,
  records_fetched,
  records_inserted,
  ROUND(duration_ms/1000, 2) AS duration_sec
FROM `kpi-auto-471020.st_logs_v2.ingestion_logs`
WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY start_time DESC;
```

### Check Sync State

```sql
-- View last sync time for each entity
SELECT
  entity_type,
  last_sync_time,
  status,
  record_count
FROM `kpi-auto-471020.st_logs_v2.sync_state`
ORDER BY entity_type;
```

---

## Troubleshooting

### Issue: Backfill Timeout

**Symptom:** Cloud Run returns 504 Gateway Timeout
**Cause:** Full refresh takes longer than Cloud Run timeout (60 min default)

**Solution:**
```bash
# Increase timeout to max (60 min)
gcloud run services update st-v2-ingestor \
  --timeout=3600 \
  --region=us-central1

# Or run backfill for one entity at a time
curl "$SERVICE_URL/ingest/jobs?mode=full"  # Wait for completion
curl "$SERVICE_URL/ingest/invoices?mode=full"  # Then next one
```

### Issue: Rate Limit Errors

**Symptom:** Logs show "429 Too Many Requests"
**Cause:** ServiceTitan API rate limit (10 req/sec)

**Solution:**
- The ingestor already has rate limiting built-in
- If still hitting limits, run entities one at a time instead of parallel
- Add delay between entity backfills: `sleep 60 && curl ...`

### Issue: Duplicate Records

**Symptom:** More records than expected, KPIs seem inflated

**Cause:** Running full refresh multiple times without clearing old data

**Solution:**
```sql
-- Check for duplicates
SELECT
  id,
  COUNT(*) AS duplicate_count
FROM `kpi-auto-471020.st_raw_v2.raw_jobs`
GROUP BY id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- If duplicates found, the MERGE upsert should have prevented this
-- But you can manually deduplicate:
CREATE OR REPLACE TABLE `kpi-auto-471020.st_raw_v2.raw_jobs` AS
SELECT * EXCEPT(row_num)
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) AS row_num
  FROM `kpi-auto-471020.st_raw_v2.raw_jobs`
)
WHERE row_num = 1;
```

---

## Post-Backfill Checklist

- [ ] All entities show data going back to company start date
- [ ] KPI weekly rollup shows multiple years of data
- [ ] No duplicate records in raw tables
- [ ] Cloud Scheduler jobs created for daily incremental sync
- [ ] Reference dimensions refreshing daily
- [ ] Looker Studio connected and showing historical trends
- [ ] Team trained on new KPI dashboards
- [ ] Old manual ST report process documented as backup

---

## Estimated Costs

**One-Time Backfill:**
- BigQuery Storage: ~$2-5 for 1-2 years of data (500K-2M records)
- BigQuery Queries: ~$5-10 for initial KPI mart creation
- Cloud Run: $0 (well within free tier for one-time run)
- **Total: $7-15 one-time**

**Ongoing Daily Sync:**
- BigQuery Storage: ~$0.02/GB/month (~$0.50/month)
- BigQuery Queries: ~$1-2/month (KPI views are free until queried)
- Cloud Run: $0 (free tier covers daily 5-minute runs)
- **Total: ~$1.50-2.50/month**

---

**Next Step:** Deploy to Cloud Run and run full backfill! 🚀
