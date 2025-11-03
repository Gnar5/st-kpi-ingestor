# Data Sync & Refresh Strategy

## Overview

This document explains how data flows through the system, when syncs run, and how views refresh.

---

## 🏗️ Architecture

```
ServiceTitan API (Source of Truth)
         ↓
    Ingestors (Node.js scripts)
         ↓
Raw Tables (st_raw_v2.*) ← Partitioned by date
         ↓
Dimension Tables (st_dim_v2.*) ← Denormalized for performance
         ↓
Stage Views (st_stage.*) ← Business logic layer
         ↓
Mart Views (st_mart_v2.*) ← Aggregated KPIs
```

---

## 📅 Daily Sync Schedule (All Phoenix Timezone)

### Phase 1: Raw Data Sync (2:00 AM - 3:00 AM)
```
2:00 AM  ← jobs (7-day lookback)
2:10 AM  ← appointments (7-day lookback)
2:15 AM  ← invoices (7-day lookback)
2:30 AM  ← estimates (180-day lookback) ⭐ UPDATED
2:45 AM  ← payments (7-day lookback)
3:00 AM  ← payroll (7-day lookback)
3:15 AM  ← customers (7-day lookback)
```

### Phase 2: Dimension Rebuild (3:00 AM UTC = 8:00 PM Phoenix)
```
3:00 AM UTC (8:00 PM Phoenix) ← dim_jobs rebuild
```
⚠️ **ISSUE:** This runs BEFORE the daily raw data syncs (in UTC time)

### Phase 3: View Refresh
```
Views refresh automatically when queried (they're virtual, not materialized)
```

---

## 🔄 How Views Work

### BigQuery Views Are Virtual Queries

**What this means:**
- Views are NOT stored data tables
- They're just SQL queries that run when you query them
- Data is always fresh (reflects latest raw table data)
- No manual refresh needed
- Higher query cost (scans raw tables every time)

**Example:**
```sql
-- When you query this:
SELECT * FROM st_mart_v2.leads_daily WHERE kpi_date = '2025-10-20'

-- BigQuery actually runs:
SELECT ... FROM st_stage.leads_jobs WHERE lead_date = '2025-10-20'
-- Which in turn runs:
SELECT ... FROM st_dim_v2.dim_jobs WHERE ...
-- Which reads from:
st_raw_v2.raw_jobs
```

### Data Freshness Guarantee

After daily syncs complete (~3:15 AM Phoenix):
1. ✅ Raw tables have fresh data
2. ⚠️ dim_jobs is STALE (rebuilds at 8PM, before raw syncs)
3. ⚠️ Views using dim_jobs show yesterday's dimension data
4. ✅ Views NOT using dim_jobs are immediately fresh

---

## ⚠️ Current Issues

### Issue 1: dim_jobs Rebuild Timing ❌

**Current Schedule:**
- dim_jobs: 3:00 AM UTC = 8:00 PM Phoenix (previous day)
- Raw syncs: 2:00-3:15 AM Phoenix = 9:00-10:15 AM UTC

**Problem:**
- dim_jobs rebuilds 10+ hours BEFORE raw data syncs
- Views using dim_jobs (opportunities, leads) show stale dimension data until 8PM

**Fix:**
```bash
# Update dim_jobs to run AFTER raw syncs
gcloud scheduler jobs update http v2-rebuild-dim-jobs-daily \
  --location=us-central1 \
  --schedule="0 4 * * *" \
  --time-zone="America/Phoenix"
```

This changes it to 4:00 AM Phoenix (after all raw syncs finish)

### Issue 2: Estimates Monthly Backfill Running Hourly ❌

**Current Schedule:**
```
estimates-monthly-backfill: 0 * * * * (America/Denver) ← EVERY HOUR!
```

**Problem:**
- Set to run every hour (`0 * * * *`)
- Should be monthly or disabled
- Wasting Cloud Run costs

**Options:**

1. **Disable it** (we now have 180-day daily lookback):
```bash
gcloud scheduler jobs pause estimates-monthly-backfill --location=us-central1
```

2. **Change to monthly** (first day of month at 2AM):
```bash
gcloud scheduler jobs update http estimates-monthly-backfill \
  --location=us-central1 \
  --schedule="0 2 1 * *" \
  --time-zone="America/Phoenix"
```

**Recommendation:** Disable it. The 180-day lookback on daily sync makes monthly backfill unnecessary.

---

## 🎯 Recommended Sync Strategy

### Option A: Keep Current (Simple) ✅ RECOMMENDED

**Pros:**
- No changes needed (except fixing issues above)
- Views always fresh after daily sync
- Simple to understand and maintain

**Cons:**
- Views can be slow on large datasets
- Higher query costs

**Action Items:**
1. Fix dim_jobs timing → 4:00 AM Phoenix
2. Disable monthly backfill
3. Done!

### Option B: Add Materialized Views (Advanced)

**What are Materialized Views?**
- Pre-computed and stored query results
- Much faster to query
- Need manual or scheduled refresh

**How to implement:**
```sql
-- Create materialized view (refreshed manually)
CREATE MATERIALIZED VIEW `kpi-auto-471020.st_mart_v2.leads_daily_mv` AS
SELECT * FROM `kpi-auto-471020.st_mart_v2.leads_daily`;

-- Refresh it (must run this after data syncs)
CALL BQ.REFRESH_MATERIALIZED_VIEW('kpi-auto-471020.st_mart_v2.leads_daily_mv');
```

**Add to Cloud Scheduler:**
```bash
# Refresh all materialized views at 5 AM (after dim_jobs rebuild)
gcloud scheduler jobs create http refresh-materialized-views \
  --location=us-central1 \
  --schedule="0 5 * * *" \
  --time-zone="America/Phoenix" \
  --uri="https://your-service-url/refresh-views" \
  --http-method=GET
```

**Pros:**
- 10-100x faster query performance
- Lower query costs
- Better user experience

**Cons:**
- More complex
- Data delayed until refresh runs
- Additional maintenance

### Option C: Scheduled Query to Tables (Middle Ground)

**Concept:**
- Run a query nightly to write results to a table
- Table is partitioned by date for efficiency

**Example:**
```sql
-- BigQuery Scheduled Query (runs at 5 AM daily)
INSERT INTO `kpi-auto-471020.st_mart_v2.leads_daily_table`
SELECT * FROM `kpi-auto-471020.st_mart_v2.leads_daily`
WHERE kpi_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);
```

**Pros:**
- Fast queries (reading from table, not view)
- Can partition table for efficiency
- Simple scheduled query UI in BigQuery

**Cons:**
- Data delayed until scheduled query runs
- Need to manage table updates

---

## 📊 Current View Performance

### Views That Use dim_jobs (Affected by rebuild timing):
- `st_stage.opportunity_jobs` ← Uses dim_jobs for businessUnitNormalized
- `st_stage.leads_jobs` ← Uses dim_jobs
- `st_mart_v2.leads_daily` ← Depends on leads_jobs
- `st_mart_v2.leads_daily_bu` ← Depends on leads_jobs
- `st_mart_v2.opportunity_daily` ← Depends on opportunity_jobs

### Views That Don't Use dim_jobs (Always fresh):
- `st_mart_v2.total_sales_daily` ← Joins raw_estimates + dim_jobs ⚠️
- `st_mart_v2.total_sales_daily_bu` ← Joins raw_estimates + dim_jobs ⚠️

**Note:** Total sales views also use dim_jobs for businessUnitNormalized, so they're affected by the timing issue too.

---

## 🔧 Quick Fixes (High Priority)

### 1. Fix dim_jobs Timing

```bash
gcloud scheduler jobs update http v2-rebuild-dim-jobs-daily \
  --location=us-central1 \
  --schedule="0 4 * * *" \
  --time-zone="America/Phoenix"
```

**Validates:**
```bash
gcloud scheduler jobs describe v2-rebuild-dim-jobs-daily \
  --location=us-central1 \
  --format="value(schedule,timeZone)"
# Expected: 0 4 * * *  America/Phoenix
```

### 2. Disable Hourly Estimates Backfill

```bash
gcloud scheduler jobs pause estimates-monthly-backfill \
  --location=us-central1
```

**Validates:**
```bash
gcloud scheduler jobs describe estimates-monthly-backfill \
  --location=us-central1 \
  --format="value(state)"
# Expected: PAUSED
```

---

## 📈 Monitoring Recommendations

### 1. Create Sync Status Dashboard

**Query to check last sync times:**
```sql
SELECT
  entityType,
  MAX(syncTime) as last_sync,
  MAX(recordsProcessed) as records_processed,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(syncTime), HOUR) as hours_since_sync
FROM `kpi-auto-471020.st_logs_v2.sync_state`
GROUP BY entityType
ORDER BY last_sync DESC;
```

### 2. Set Up Alerts

**When to alert:**
- Sync hasn't run in > 25 hours
- Sync failed with error
- Record count dropped significantly (>50% vs. previous day)

### 3. Validate Daily

**Quick validation query:**
```sql
-- Check yesterday's KPIs exist
SELECT
  COUNT(*) as views_with_data
FROM (
  SELECT 1 FROM `kpi-auto-471020.st_mart_v2.leads_daily`
  WHERE kpi_date = CURRENT_DATE() - 1

  UNION ALL

  SELECT 1 FROM `kpi-auto-471020.st_mart_v2.total_sales_daily`
  WHERE kpi_date = CURRENT_DATE() - 1

  UNION ALL

  SELECT 1 FROM `kpi-auto-471020.st_stage.opportunity_jobs`
  WHERE opportunity_date = CURRENT_DATE() - 1
);
-- Expected: 3 (all views have yesterday's data)
```

---

## 🎓 Summary

### Current State ✅
- Daily syncs at 2-3 AM Phoenix
- Estimates have 180-day lookback (catches late sales)
- Views auto-refresh (virtual queries)
- WBR metrics validated: 99-100% match

### Issues to Fix ⚠️
1. dim_jobs runs too early (8 PM vs 4 AM needed)
2. Monthly backfill running hourly (disable it)

### Recommendations
1. **Immediate:** Fix dim_jobs timing + disable hourly backfill
2. **Short term:** Monitor sync health, validate daily
3. **Long term:** Consider materialized views if query performance becomes an issue

### For Questions
- View refresh: Automatic (virtual views)
- Data freshness: After 4 AM Phoenix (once dim_jobs fixed)
- Manual refresh: Not needed (views are virtual)
- Backfill: Not needed (180-day lookback covers it)
