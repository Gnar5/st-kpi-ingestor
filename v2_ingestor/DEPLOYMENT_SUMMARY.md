# ServiceTitan v2 Ingestor - Deployment Summary

**Date:** 2025-10-22
**Status:** PRODUCTION DEPLOYED & AUTOMATED
**Service URL:** https://st-v2-ingestor-999875365235.us-central1.run.app

---

## Deployment Status

### Cloud Run Service
- **Service Name:** st-v2-ingestor
- **Region:** us-central1
- **Latest Revision:** st-v2-ingestor-00009-kv8
- **Memory:** 1Gi
- **CPU:** 1
- **Timeout:** 3600s (1 hour)
- **Max Instances:** 10
- **Authentication:** Allow unauthenticated (for scheduler access)

### Environment Configuration
```
BQ_PROJECT_ID=kpi-auto-471020
BQ_DATASET_RAW=st_raw_v2
BQ_DATASET_STAGE=st_stage_v2
BQ_DATASET_MART=st_mart_v2
BQ_DATASET_LOGS=st_logs_v2
ST_CLIENT_ID=cid.0sx24a627mi8qx2wsuwbo4c68
ST_CLIENT_SECRET=cs1.k1jvmtimzb87n3phzewn1hbs662sncs6kr16nliwe9jwmbbpyh
ST_TENANT_ID=636913317
ST_APP_KEY=ak1.ustiqwarpotilgkmx5dhqzu6k
NODE_ENV=production
LOG_LEVEL=info
SYNC_MODE=incremental
LOOKBACK_DAYS=7
MAX_CONCURRENT_REQUESTS=5
RATE_LIMIT_PER_SECOND=10
```

---

## Automated Daily Syncs (Cloud Scheduler)

All jobs run in **America/Phoenix** timezone (Arizona time, no DST).

| Job Name | Schedule | Time (AZ) | Entity | Mode |
|----------|----------|-----------|--------|------|
| v2-sync-jobs-daily | `0 2 * * *` | 2:00 AM | Jobs | Incremental |
| v2-sync-invoices-daily | `15 2 * * *` | 2:15 AM | Invoices | Incremental |
| v2-sync-estimates-daily | `30 2 * * *` | 2:30 AM | Estimates | Incremental |
| v2-sync-payments-daily | `45 2 * * *` | 2:45 AM | Payments | Incremental |
| v2-sync-payroll-daily | `0 3 * * *` | 3:00 AM | Payroll | Incremental |
| v2-sync-customers-daily | `15 3 * * *` | 3:15 AM | Customers | Incremental |

**All jobs ENABLED and will run daily starting tonight.**

---

## API Endpoints

### Health Check
```bash
GET https://st-v2-ingestor-999875365235.us-central1.run.app/health
```

Response:
```json
{
  "status": "healthy",
  "service": "st-v2-ingestor",
  "version": "2.0.0",
  "timestamp": "2025-10-23T00:25:22.354Z"
}
```

### Manual Sync Endpoints

Trigger manual syncs for any entity:

```bash
# Incremental sync (default - syncs last 7 days)
GET /ingest/{entity}?mode=incremental

# Full sync (re-sync all data from 2020-01-01)
GET /ingest/{entity}?mode=full
```

**Available Entities:**
- `jobs`
- `invoices`
- `estimates`
- `payments`
- `payroll`
- `customers`
- `locations`
- `campaigns`

**Example:**
```bash
curl "https://st-v2-ingestor-999875365235.us-central1.run.app/ingest/jobs?mode=incremental"
```

Success Response:
```json
{
  "success": true,
  "entity": "jobs",
  "mode": "incremental",
  "recordsProcessed": 72,
  "runId": "a9370b9e-da67-4333-ba5d-d11d52fd3e1d",
  "duration": 10996
}
```

---

## Production Fixes Deployed

This deployment includes all critical fixes validated on 2025-10-22:

### 1. Invoice jobId Extraction Fix
**File:** `src/ingestors/invoices.js:43`
**Fix:** Extract jobId from nested API response (`invoice.job?.id`)
**Impact:** 99.07% job coverage (171,222 / 172,833 invoices)
**Result:** All Production BU KPIs now operational

### 2. Total Booked Timezone Fix
**File:** `create_kpi_mart.sql` (Total Booked CTE)
**Fix:** Use `DATE(soldOn, 'America/Phoenix')` instead of UTC
**Impact:** Fixed $9,920 discrepancy for Nevada
**Result:** ALL THREE BUs exact matches with ST UI:
- Phoenix: $30,241.51 (exact)
- Tucson: $4,844.58 (exact)
- Nevada: $27,150.00 (exact)

### 3. Leads Definition Fix
**File:** `create_kpi_mart.sql` (Leads CTE)
**Fix:** Case-insensitive `LOWER(jobTypeName) LIKE '%estimate%'`
**Impact:** Captures all estimate types (not just "Estimate")
**Result:** Accurate lead counts matching ST UI definition

### 4. Repository Cleanup
**Removed:**
- 10 test files (`test_*.js`)
- 3 obsolete backfill scripts
- 2 duplicate SQL files
- Log files

**Added:**
- `PRODUCTION_VERIFICATION.md` - Oct 2025 metrics
- `FINAL_VALIDATION_REPORT.md` - Timezone fix analysis
- `validate_leads_fix.sql` - Validation queries
- `diagnostics_total_booked.sql` - Multi-basis diagnostics

---

## Data Status

### Raw Tables (st_raw_v2)
- **Jobs:** 226,588 records (2020-present)
- **Invoices:** 172,833 records (99.07% job coverage)
- **Estimates:** 220,464 records
- **Payments:** 192,856 records
- **Payroll:** 8,845 records
- **Customers:** 74,124 records

### Production BU KPIs - OPERATIONAL
**October 2025 Production Revenue:** $1,620,581

| Business Unit | $ Produced | Avg GPM |
|---------------|------------|---------|
| Phoenix-Production | $761,263 | 30.04% |
| Tucson-Production | $311,339 | 42.21% |
| Andy's Painting-Production | $162,722 | 32.68% |
| Commercial-AZ-Production | $147,058 | 16.45% |
| Nevada-Production | $134,617 | 43.54% |
| Guaranteed Painting-Production | $103,582 | 28.22% |

### Sales BU KPIs - VALIDATED
**Validated Against ServiceTitan UI (Aug 18, 2025):**
- Phoenix-Sales: Exact match
- Tucson-Sales: Exact match
- Nevada-Sales: Exact match

---

## Monitoring & Logs

### View Cloud Run Logs
```bash
gcloud run services logs read st-v2-ingestor --region=us-central1 --limit=50
```

### View Scheduler Job Execution History
```bash
gcloud scheduler jobs describe v2-sync-jobs-daily --location=us-central1
```

### View BigQuery Ingestion Logs
```sql
SELECT *
FROM `kpi-auto-471020.st_logs_v2.ingestion_logs`
ORDER BY start_time DESC
LIMIT 10;
```

### Check Sync State
```sql
SELECT *
FROM `kpi-auto-471020.st_logs_v2.sync_state`
ORDER BY updated_at DESC;
```

---

## Looker Connection

**Connect Looker to:**
```
Dataset: kpi-auto-471020.st_mart_v2
Table: daily_kpis
```

This table contains all 10 KPIs by business unit and date:
1. Leads (lead_count)
2. Total Booked (total_booked)
3. Dollars Produced (dollars_produced)
4. GPM % (gpm_percent)
5. Dollars Collected (dollars_collected)
6. # Estimates (num_estimates)
7. Close Rate % (close_rate_percent)
8. Future Bookings (future_bookings)
9. Warranty % (warranty_percent)
10. Outstanding A/R (outstanding_ar)

**Sample Query:**
```sql
SELECT
  event_date,
  business_unit,
  lead_count,
  total_booked,
  dollars_produced,
  gpm_percent
FROM `kpi-auto-471020.st_mart_v2.daily_kpis`
WHERE event_date >= CURRENT_DATE('America/Phoenix') - 30
  AND business_unit = 'Phoenix-Sales'
ORDER BY event_date DESC;
```

---

## Maintenance Tasks

### Rebuild KPI Mart (After Schema Changes)
```bash
bq query --use_legacy_sql=false < create_kpi_mart.sql
```

### Manual Full Sync (If Needed)
```bash
# Sync all entities (use sparingly - hits API rate limits)
for entity in jobs invoices estimates payments payroll customers; do
  curl "https://st-v2-ingestor-999875365235.us-central1.run.app/ingest/$entity?mode=full"
  sleep 60  # Rate limit protection
done
```

### Pause/Resume Scheduler Jobs
```bash
# Pause all v2 syncs
gcloud scheduler jobs pause v2-sync-jobs-daily --location=us-central1
gcloud scheduler jobs pause v2-sync-invoices-daily --location=us-central1
# ... (repeat for other jobs)

# Resume all v2 syncs
gcloud scheduler jobs resume v2-sync-jobs-daily --location=us-central1
gcloud scheduler jobs resume v2-sync-invoices-daily --location=us-central1
# ... (repeat for other jobs)
```

---

## Troubleshooting

### Issue: Scheduler job fails with 4xx/5xx
**Check:**
1. Cloud Run service logs: `gcloud run services logs read st-v2-ingestor --region=us-central1 --limit=50`
2. Environment variables are set correctly
3. ServiceTitan API credentials are valid

### Issue: No new records ingested
**Check:**
1. Verify `LOOKBACK_DAYS=7` is appropriate for your sync frequency
2. Check `st_logs_v2.sync_state` table for last sync time
3. Confirm ServiceTitan has new data in that date range

### Issue: BigQuery quota exceeded
**Solution:**
- Reduce `MAX_CONCURRENT_REQUESTS` (currently 5)
- Reduce `RATE_LIMIT_PER_SECOND` (currently 10)
- Add delays between scheduler jobs

---

## Next Steps (Optional Enhancements)

1. **Add Monitoring Dashboard**
   - Set up Cloud Monitoring dashboard for sync metrics
   - Configure alerts for failed syncs

2. **Implement Validation Suite**
   - Create nightly validation jobs comparing BQ vs ST UI
   - Build RED/YELLOW/GREEN stoplight dashboard

3. **Add More Entities**
   - Purchase Orders (code exists: `src/ingestors/purchase_orders.js`)
   - Returns (code exists: `src/ingestors/returns.js`)
   - Add to scheduler once tested

4. **Optimize Performance**
   - Implement connection pooling
   - Add Redis cache for frequently accessed data
   - Batch scheduler jobs for off-peak hours

---

## Git Repository

**GitHub:** https://github.com/Gnar5/st-kpi-ingestor
**Latest Commit:** 1e3e782 - Production fixes: Invoice jobId extraction, KPI definitions, and cleanup

---

**Deployment Completed:** 2025-10-23 00:26 UTC
**Deployed By:** Claude Code + Caleb
**Status:** PRODUCTION READY - ALL SYSTEMS OPERATIONAL
