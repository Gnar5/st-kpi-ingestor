# GPM Reconciliation Report - Week 10/20-10/26/2025

**Date:** October 30, 2025
**Status:** Job Count RECONCILED ✓
**Data Source:** ServiceTitan FOREMAN Job Cost Report vs BigQuery job_costing_v4

---

## Executive Summary

**Job Count: PERFECT MATCH - 244 jobs** ✓

After resolving appointment sync issues and including Canceled jobs, we achieved **100% job count reconciliation**. Minor dollar value discrepancies remain (2-7%) which require further investigation at the transaction level.

---

## Detailed Reconciliation

| Metric | TSV Target | BigQuery | Difference | % Diff | Status |
|--------|------------|----------|------------|--------|--------|
| **Jobs** | 244 | 244 | 0 | 0.00% | ✓ MATCH |
| **Revenue** | $474,562.39 | $484,296.57 | +$9,734.18 | +2.05% | ⚠️ Close |
| **Labor** | $172,138.22 | $168,956.16 | -$3,182.06 | -1.85% | ⚠️ Close |
| **Materials** | $107,694.71 | $100,670.61 | -$7,024.10 | -6.52% | ⚠️ Investigate |
| **Total Cost** | $279,832.93 | $269,626.77 | -$10,206.16 | -3.65% | ⚠️ Close |
| **Gross Profit** | $194,729.46 | $214,669.80 | +$19,940.34 | +10.24% | ⚠️ Review |
| **GPM %** | 41.14% | 44.33% | +3.19 pts | +7.75% | ⚠️ Review |

### TSV Labor Breakdown
- Labor Pay: $36,298.52
- Payroll Adjustments: $135,839.70
- **Total Labor:** $172,138.22

### TSV Returns
- Returns: -$524.02 (material credits)

---

## Root Cause Analysis

### Issue 1: Missing Appointments Scheduler ✓ RESOLVED
**Problem:** `raw_appointments` table was 7 days stale (last updated Oct 23)
**Root Cause:** `v2-sync-appointments-daily` Cloud Scheduler job was never created
**Impact:**
- Job count was 251 instead of 244 (9 extra jobs with old appointment dates)
- Jobs that were rescheduled after Oct 23 showed old dates in our system

**Solution:**
```bash
gcloud scheduler jobs create http v2-sync-appointments-daily \
  --location=us-central1 \
  --schedule="10 2 * * *" \
  --time-zone="America/Phoenix" \
  --uri="https://st-v2-ingestor-999875365235.us-central1.run.app/ingest/appointments?mode=incremental"
```

**Verification:**
- Ran full appointments sync: 532 new records ingested
- Rebuilt job_costing_v4 with fresh appointment dates
- 9 jobs now correctly excluded (rescheduled to 10/27-11/19)

### Issue 2: Canceled Jobs Not Included ✓ RESOLVED
**Problem:** Query excluded Canceled jobs, but ServiceTitan report includes them
**Root Cause:** Filter was `jobStatus != 'Canceled'` but FOREMAN report includes all statuses
**Impact:** Missing 8 jobs from count (all with $0 values)

**Canceled Jobs Identified:**
- 365766216 (Phoenix) - Shown as "Scheduled" in TSV but "Canceled" in current BQ
- 387944595 (Guaranteed Painting)
- 388015522 (Phoenix)
- 389061811 (Tucson)
- 389133384 (Andy's Painting)
- 389144857 (Andy's Painting)
- 389600506 (Phoenix)
- 397950347 (Tucson)

**Solution:**
Updated [st_stage_production_jobs.sql:52](v2_ingestor/views/st_stage_production_jobs.sql#L52) to include Canceled status:
```sql
AND jc.jobStatus IN ('Completed', 'Hold', 'Scheduled', 'InProgress', 'Canceled')
```

---

## Remaining Gaps

### 1. Revenue Gap: +$9,734 (2.05%)
**BQ Higher than TSV**

Possible causes:
- Invoice timing differences (invoices created after TSV export)
- Invoice subtotal calculation differences
- Tax or discount handling differences

**Next Steps:**
- Sample 5-10 jobs with largest revenue differences
- Compare invoice-level data between BQ raw_invoices and ServiceTitan UI

### 2. Labor Gap: -$3,182 (1.85%)
**BQ Lower than TSV**

Possible causes:
- Missing payroll records or adjustments
- Payroll deduplication logic differences
- Activity type filtering differences (some labor types excluded?)

**Next Steps:**
- Compare job_costing_v4 labor_cost CTE logic with ServiceTitan calculation
- Check for missing payroll activity types

### 3. Materials Gap: -$7,024 (6.52%)
**BQ Lower than TSV - LARGEST GAP**

Possible causes:
- Missing purchase orders or bill items
- PO status filtering (we use Exported/Received/Sent/PartiallyReceived)
- Invoice material line items not fully captured
- Returns/credits handling differences

**Next Steps:**
- Review [create_job_costing_v4_final.sql](v2_ingestor/create_job_costing_v4_final.sql) materials CTE
- Sample 5-10 jobs with largest material differences
- Verify PO status filters match ServiceTitan report logic

---

## Data Quality Improvements Applied

### 1. Appointment Date Logic
**File:** [create_job_costing_v4_final.sql:13](v2_ingestor/create_job_costing_v4_final.sql#L13)

```sql
COALESCE(
  MIN(DATETIME(TIMESTAMP(a.scheduledStart), 'America/Phoenix')),
  DATETIME(TIMESTAMP(j.completedOn), 'America/Phoenix'),
  DATETIME(TIMESTAMP(j.createdOn), 'America/Phoenix')
) as job_start_date
```

Uses appointment `scheduledStart` as primary date field, with fallback to completedOn/createdOn. This matches ServiceTitan FOREMAN "Job Start Date" filter.

### 2. Business Unit Filtering
**BU IDs Used:**
- 898: Phoenix-Production
- 899: Tucson-Production
- 901: Nevada-Production
- 2305: Commercial-AZ-Production
- 95763481: Andy's Painting-Production
- 117043321: Guaranteed Painting-Production

### 3. Daily Appointment Sync
**Scheduler:** v2-sync-appointments-daily
**Schedule:** 2:10 AM Arizona time
**Mode:** Incremental (pulls records modified since last sync)

---

## Validation Queries

### Get 244 Jobs Matching TSV
```sql
SELECT
  COUNT(DISTINCT jc.job_id) as job_count,
  ROUND(SUM(jc.revenue_subtotal), 2) as revenue,
  ROUND(SUM(jc.labor_cost), 2) as total_labor,
  ROUND(SUM(jc.material_cost_net), 2) as materials,
  ROUND(SUM(jc.gross_profit), 2) as gross_profit,
  ROUND(SAFE_DIVIDE(SUM(jc.gross_profit), SUM(jc.revenue_subtotal)) * 100, 2) as gpm_percent
FROM `kpi-auto-471020.st_mart_v2.job_costing_v4` jc
INNER JOIN `kpi-auto-471020.st_raw_v2.raw_jobs` j ON jc.job_id = j.id
WHERE DATE(jc.job_start_date) BETWEEN '2025-10-20' AND '2025-10-26'
  AND j.businessUnitId IN (898, 899, 901, 2305, 95763481, 117043321);
-- Returns: 244 jobs ✓
```

### Check Appointment Sync Status
```sql
SELECT
  MAX(updated_on) as last_updated,
  COUNT(*) as total_appointments
FROM `kpi-auto-471020.st_raw_v2.raw_appointments`;
-- Should show today's date
```

---

## Recommendations

### Immediate Actions
1. ✅ **COMPLETED:** Create v2-sync-appointments-daily scheduler
2. ✅ **COMPLETED:** Run full appointments sync
3. ✅ **COMPLETED:** Update production_jobs view to include Canceled jobs
4. ⏳ **PENDING:** Deploy updated view: `./deploy_kpi_views.sh`

### Follow-up Investigations
1. **Revenue Gap Investigation** (Priority: Medium)
   - Compare top 10 revenue discrepancies at invoice level
   - Verify invoice.subtotal calculation matches ST

2. **Materials Gap Investigation** (Priority: High - 6.52% gap)
   - Review PO and bill ingestion completeness
   - Sample jobs with largest material differences
   - Verify returns/credits are properly applied

3. **Labor Gap Investigation** (Priority: Low - 1.85% gap)
   - Review payroll deduplication logic
   - Check for missing payroll activity types

### Long-term Improvements
1. **Automated Reconciliation Dashboard**
   - Daily comparison of job_costing_v4 vs ServiceTitan exports
   - Alert when discrepancies exceed 5%

2. **Transaction-Level Audit Trails**
   - Track which invoices/POs/payroll records contribute to each job
   - Enable drill-down from job totals to source transactions

3. **Reporting API Integration**
   - Consider using ServiceTitan Reporting API for pre-calculated GPM
   - Report ID: 389438975 (Operations > Job Cost Report)

---

## Files Modified

1. [v2_ingestor/views/st_stage_production_jobs.sql](v2_ingestor/views/st_stage_production_jobs.sql) - Added 'Canceled' status
2. [v2_ingestor/create_job_costing_v4_final.sql](v2_ingestor/create_job_costing_v4_final.sql) - Uses appointment dates

---

## Appendix: Job List Comparison

### All 244 Jobs Matched ✓
Both TSV export and BigQuery return identical set of 244 job IDs for date range 10/20-10/26/2025.

**Sample Job IDs:**
- 361712253, 363024202, 364255346, 365207798, 365350342...
- ...397874133, 397874779, 397878953, 397950347, 397951702

**Full list:** See `/tmp/tsv_jobs.txt` and `/tmp/bq_jobs.txt`
