# BigQuery Structure Documentation

**Date:** October 17, 2025
**Project:** kpi-auto-471020

## Active Datasets (Current Production)

### `st_raw` - Raw ServiceTitan Data
Raw data ingested directly from ServiceTitan API reports.

**Active Tables:**
- `raw_leads` - Lead/estimate data (5,900 rows, last updated Oct 15)
- `raw_collections` - Payment collection data (5,744 rows, last updated Oct 16)
- `raw_foreman` - Job cost and production data (6,992 rows, last updated Oct 15)
- `raw_future_bookings` - Forward-looking scheduled jobs
- `raw_daily_wbr_consolidated` - ✨ **NEW!** Consolidated WBR data (fixes overlap issue)
  - Replaces: `raw_daily_wbr_v2` (old per-BU approach with 7-day rolling windows)
  - Contains: All BUs in single API call, exact date ranges, no overlap

### `st_kpi_mart` - Aggregated KPI Mart (Primary Dashboard Source)
Clean, aggregated data used for Looker dashboards and reporting.

**Active Tables:**
- `kpi_daily_consolidated` - **PRIMARY TABLE** (1,915 rows, updated Oct 16)
  - Main source for daily KPI dashboard
  - Joins all metrics: leads, collections, WBR, foreman
- `leads_daily_fact` - Daily lead counts by BU
- `collections_daily_fact` - Daily collection amounts by BU
- `wbr_daily_fact` - Daily WBR metrics (estimates, close rate, sales)
- `foreman_daily_fact` - Daily job cost and production metrics

**Reference Tables:**
- `dim_branch` - Business unit mapping
- `dim_date` / `dim_calendar` - Date dimension tables

**Views:**
- `kpi_weekly` - Weekly rollup view
- `kpi_weekly_yoy` - Year-over-year comparison
- `kpi_daily_regional_consolidated` - Regional view
- `wbr_weekly_yoy` - WBR year-over-year
- `fin_weekly_with_ly` - Financial weekly with last year

---

## Archived Datasets (Backups)

### `archive_backup_20251017` - Safe Backups from Oct 17, 2025
Old tables from previous iterations, kept for safety before cleanup.

**Contents:**
- `st_mart_fact_kpi_daily` - Old daily KPI fact table from st_mart
- `st_mart_foreman_weekly_snapshots` - Old weekly foreman snapshots
- `st_mart_foreman_weekly_snapshots_clean` - Cleaned version
- `kpi_mart_fact_kpi_daily` - Old daily KPI fact table from kpi_mart

**When to Delete:** After confirming all dashboards/queries use `st_kpi_mart` tables instead

---

## Deprecated Datasets (To Be Evaluated)

### `st_mart` - Old Mart (Last Modified: Sept 27)
Appears to be an older iteration of the KPI mart. Contains:
- `fact_kpi_daily` - Duplicates functionality of `st_kpi_mart.kpi_daily_consolidated`
- `v_foreman_weekly_clean` - View for cleaned foreman data
- `v_kpi_daily_enriched` - Enriched daily KPI view

**Status:** ⚠️ Evaluate if any active queries use this, then consider removing

### `kpi_mart` - Old Mart (Last Modified: Oct 16, but only 83 rows)
Another iteration with minimal data:
- `fact_kpi_daily` - Only 83 rows vs 1,915 in st_kpi_mart
- `v_dashboard_yoy` - YoY dashboard view

**Status:** ⚠️ Likely obsolete, backed up to archive

### `st_kpi` - Old KPI Dataset
**Status:** ⚠️ Appears unused, evaluate for removal

### `st_stage` - Staging Dataset
**Status:** ❓ May be used for ETL staging, needs review

### `st_ref` - Reference Dataset
**Status:** ❓ May contain lookup tables, needs review

---

## Recommended Next Steps

### 1. Verify Active Usage (Before Any Deletions)
```bash
# Check if any queries in the last 30 days used deprecated datasets
# (Run this in BigQuery Console > Query History)
```

### 2. Update All Queries/Dashboards
Ensure all Looker dashboards and scheduled queries point to:
- Raw data: `st_raw.*`
- Mart data: `st_kpi_mart.*`

### 3. After Verification (1-2 weeks)
Once you've confirmed nothing is using the old datasets:
```bash
# Delete deprecated datasets
bq rm -r -f kpi-auto-471020:st_mart
bq rm -r -f kpi-auto-471020:kpi_mart
bq rm -r -f kpi-auto-471020:st_kpi
```

### 4. Clean Up Old WBR Data
The old `raw_daily_wbr_v2` has overlapping data (7-day rolling windows). Options:
- Keep it for historical reference but don't query it
- Truncate it and re-backfill with consolidated approach
- Delete after backfilling with `raw_daily_wbr_consolidated`

---

## Data Flow

```
ServiceTitan API
       ↓
   st_raw.*              (Raw ingestion)
       ↓
st_kpi_mart.*_fact       (Dedupe + aggregate daily)
       ↓
st_kpi_mart.kpi_daily_consolidated  (Join all metrics)
       ↓
    Looker Dashboard
```

---

## Important Notes

- **Backup Created:** October 17, 2025 in `archive_backup_20251017`
- **New WBR Approach:** Use `raw_daily_wbr_consolidated` for accurate date ranges
- **Primary Dashboard Table:** `st_kpi_mart.kpi_daily_consolidated`
- **Cost Optimization:** Consider deleting archived data after 30 days if unused

---

## Contacts & References

- **GitHub Repo:** https://github.com/Gnar5/st-kpi-ingestor
- **Ingestion Endpoints:** https://st-kpi-ingestor-999875365235.us-central1.run.app
- **ServiceTitan Consolidated Report ID:** 397555674
