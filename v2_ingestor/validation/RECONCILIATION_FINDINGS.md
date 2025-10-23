# 100% Accuracy Achieved - Reconciliation Findings

**Date:** 2025-10-23
**Validation Period:** 2025-08-18 to 2025-08-24
**Status:** ✅ 100% EXACT MATCH across all 12 Business Units (6 Sales + 6 Production)

---

## Executive Summary

Through forensic reconciliation against ServiceTitan exports, we identified and fixed two critical field mapping issues that were causing discrepancies between BigQuery KPIs and ServiceTitan reports. After fixes, all KPIs now match ServiceTitan exports with **0% variance**.

---

## Root Causes Identified

### 1. Production "Dollars Produced" - Job Status Filter Issue

**Problem:**
Phoenix Production showed $207,980.78 in BigQuery but $232,891.98 in ServiceTitan FOREMAN report ($24,911.20 gap).

**Root Cause:**
ServiceTitan's FOREMAN Job Cost report includes jobs with status `IN ('Completed', 'Hold')`, but our KPI mart was filtering only `jobStatus = 'Completed'`.

**Investigation:**
```sql
SELECT jobStatus, COUNT(*), SUM(revenue_subtotal)
FROM job_costing
WHERE businessUnitNormalized = 'Phoenix-Production'
  AND DATE(job_start_date) BETWEEN '2025-08-18' AND '2025-08-24'
GROUP BY jobStatus

-- Results:
-- Completed: $207,980.78
-- Hold: $24,911.20
-- Total: $232,891.98 ✅ EXACT ST MATCH
```

Job #147344566 with status="Hold" contained exactly $24,911.20, accounting for the entire gap.

**Fix:**
Updated [create_kpi_mart_v2.sql:94](../create_kpi_mart_v2.sql#L94):
```sql
-- Before:
AND jc.jobStatus = 'Completed'

-- After:
AND jc.jobStatus IN ('Completed', 'Hold')  -- ServiceTitan includes both
```

---

### 2. Sales "Total Booked" - Wrong Date Field

**Problem:**
Massive discrepancies across all Sales BUs:
- Phoenix: BQ $166,666.92 vs ST $116,551.26 (+43%)
- Commercial-AZ: BQ $45,283.00 vs ST $119,803.60 (-62%)
- Tucson: BQ $68,476.53 vs ST $89,990.11 (-24%)

**Root Cause:**
ServiceTitan's "BU Sales - API" report uses `estimate.soldOn` date (when estimate was marked as Sold), but our KPI mart was using `job.createdOn` date (when job was created in the system).

**Investigation:**
```sql
-- Test using soldOn date instead of job createdOn
SELECT businessUnitNormalized, SUM(COALESCE(total, subTotal))
FROM raw_estimates e
JOIN dim_jobs j ON e.jobId = j.id
WHERE e.status = 'Sold'
  AND DATE(e.soldOn) BETWEEN '2025-08-18' AND '2025-08-24'
GROUP BY businessUnitNormalized

-- Results: PERFECT MATCH with ServiceTitan!
```

**Fix:**
Updated [create_kpi_mart_v2.sql:8-45](../create_kpi_mart_v2.sql#L8-L45):
```sql
-- Before:
SELECT DATE(j.createdOn) AS event_date
FROM dim_jobs j
LEFT JOIN raw_estimates e ON j.id = e.jobId

-- After:
SELECT DATE(e.soldOn) AS event_date  -- ServiceTitan uses estimate soldOn
FROM raw_estimates e
JOIN dim_jobs j ON e.jobId = j.id
WHERE e.status = 'Sold' AND e.soldOn IS NOT NULL
```

---

## Final Validation Results

All 12 Business Units now show **0% variance** with ServiceTitan:

### Production KPIs (FOREMAN Job Cost Report)
| Business Unit | ServiceTitan | BigQuery | Status |
|--------------|-------------|----------|--------|
| Phoenix-Production | $232,891.98 | $232,891.98 | ✅ EXACT |
| Tucson-Production | $83,761.16 | $83,761.16 | ✅ EXACT |
| Nevada-Production | $23,975.00 | $23,975.00 | ✅ EXACT |
| Andy's Painting-Production | $53,752.56 | $53,752.56 | ✅ EXACT |
| Commercial-AZ-Production | $77,345.25 | $77,345.25 | ✅ EXACT |
| Guaranteed Painting-Production | $30,472.30 | $30,472.30 | ✅ EXACT |

### Sales KPIs (BU Sales - API Report)
| Business Unit | ServiceTitan | BigQuery | Status |
|--------------|-------------|----------|--------|
| Phoenix-Sales | $116,551.26 | $116,551.26 | ✅ EXACT |
| Tucson-Sales | $89,990.11 | $89,990.11 | ✅ EXACT |
| Nevada-Sales | $105,890.00 | $105,890.00 | ✅ EXACT |
| Andy's Painting-Sales | $30,896.91 | $30,896.91 | ✅ EXACT |
| Commercial-AZ-Sales | $119,803.60 | $119,803.60 | ✅ EXACT |
| Guaranteed Painting-Sales | $26,067.40 | $26,067.40 | ✅ EXACT |

---

## Field Mapping Reference

### ServiceTitan FOREMAN Job Cost Report
- **Report Name:** FOREMAN Job Cost
- **Date Field:** Job Start Date (first scheduled appointment from `raw_appointments`)
- **Job Status Filter:** Completed AND Hold
- **Revenue Field:** Job invoice subtotal
- **BigQuery Equivalent:**
  ```sql
  DATE(job_costing.job_start_date)
  WHERE jobStatus IN ('Completed', 'Hold')
  ```

### ServiceTitan BU Sales - API Report
- **Report Name:** BU Sales - API
- **Date Field:** Estimate Sold Date (`estimate.soldOn`)
- **Status Filter:** Sold estimates only
- **Revenue Field:** Estimate total (or subTotal if total is NULL)
- **BigQuery Equivalent:**
  ```sql
  DATE(estimates.soldOn)
  WHERE status = 'Sold' AND soldOn IS NOT NULL
  ```

---

## Files Modified

1. [v2_ingestor/create_kpi_mart_v2.sql](../create_kpi_mart_v2.sql)
   - Line 10-45: Changed Sales KPIs to use `estimate.soldOn` instead of `job.createdOn`
   - Line 94: Changed Production filter to include 'Hold' status jobs

2. [v2_ingestor/validation/reconciliation_report.sql](./reconciliation_report.sql)
   - Comprehensive validation query for monthly reconciliation
   - Includes expected values and variance calculation

3. [v2_ingestor/validation/reconciliation_phoenix_production.sql](./reconciliation_phoenix_production.sql)
   - Forensic analysis query that identified the Phoenix Hold job gap

---

## Monthly Validation Process

To validate future periods:

1. Export ServiceTitan reports:
   - FOREMAN Job Cost (for Production)
   - BU Sales - API (for Sales)

2. Update validation SQL with expected values:
   ```bash
   # Edit the CASE statements in reconciliation_report.sql
   bq query --use_legacy_sql=false < validation/reconciliation_report.sql
   ```

3. Review results - all should show "✅ EXACT MATCH"

4. If variances appear:
   - Check for new job statuses being introduced
   - Verify date range matches exactly
   - Use anti-join pattern from `reconciliation_phoenix_production.sql` to identify specific jobs

---

## Technical Architecture

### Data Flow
```
ServiceTitan API
    ↓
Raw Tables (st_raw_v2.*)
    ↓
Dimension Tables (st_dim_v2.dim_jobs)
    ↓
Job Costing Table (st_mart_v2.job_costing)
    ↓
KPI Mart View (st_mart_v2.daily_kpis)
    ↓
Regional KPI View (st_mart_v2.regional_kpis)
    ↓
Looker Dashboard
```

### Key Tables
- `st_raw_v2.raw_appointments` - Scheduled dates (job start date)
- `st_raw_v2.raw_invoices` - Revenue data
- `st_raw_v2.raw_payroll` - Labor costs
- `st_raw_v2.raw_purchase_orders` - Material costs
- `st_raw_v2.raw_estimates` - Sales estimates (soldOn date critical!)
- `st_mart_v2.job_costing` - Composite table combining all sources
- `st_mart_v2.daily_kpis` - Primary KPI view (points Looker here)

---

## Success Metrics

✅ **100% Production Accuracy** - All 6 Production BUs match FOREMAN exactly
✅ **100% Sales Accuracy** - All 6 Sales BUs match BU Sales report exactly
✅ **Validation Framework** - Automated SQL reports for monthly validation
✅ **Field Mapping Documentation** - Clear mapping between ST reports and BQ fields
✅ **Forensic Methodology** - Anti-join pattern for identifying specific discrepancies

---

## Next Steps

1. ✅ Deploy updated KPI mart to production (already completed)
2. ✅ Validate regional_kpis view in Looker (working correctly)
3. 🔄 Monitor for data quality issues in future periods
4. 🔄 Document any new ServiceTitan job statuses or report changes

---

## Contact

For questions about this reconciliation:
- KPI Mart SQL: [create_kpi_mart_v2.sql](../create_kpi_mart_v2.sql)
- Validation Reports: [validation/](.)
- Deployment: Cloud Run service `st-v2-ingestor`
