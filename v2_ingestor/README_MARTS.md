# ServiceTitan KPI Marts - Weekly Business Reporting

**Status:** Production-Ready
**Version:** 1.0.0
**Dataset:** `st_mart_v2`
**Purpose:** Replace manual ServiceTitan report workflows with durable BigQuery KPI marts

---

## Overview

This mart layer transforms raw ServiceTitan entity data into weekly KPI rollups that match your legacy report workflows. No more manual report downloads - query `kpi_weekly_by_bu` for instant, up-to-date business metrics.

### What's Included

**10 KPI Metrics:**
1. **Leads** - Unique customers who requested estimates (SALES)
2. **Estimates** - Count of estimates created (SALES)
3. **Total Booked** - Revenue from invoices (SALES)
4. **Produced** - Revenue from completed jobs (PRODUCTION)
5. **G.P.M.** - Gross Profit Margin after labor (PRODUCTION)
6. **Collected** - Cash collected via payments (PRODUCTION)
7. **Warranty %** - Warranty work as % of production (PRODUCTION)
8. **Outstanding A/R** - Unpaid invoices ≥ $10 (PRODUCTION)
9. **Future Bookings** - Scheduled work in next 12 months (PRODUCTION)
10. **Estimates Scheduled** - NEW: Count of estimates scheduled (PRODUCTION)

---

## Quick Start

### 1. Deploy the Marts

```bash
# Open BigQuery Console
https://console.cloud.google.com/bigquery?project=kpi-auto-471020

# Paste contents of st_mart_v2_kpis.sql and click "Run"
# The script will:
# - Create st_ref_v2 and st_mart_v2 datasets
# - Auto-discover Business Unit IDs
# - Build reference tables (BU rollups, job type lists)
# - Create daily helper views
# - Create KPI daily views
# - Create weekly rollup view
```

###2. Verify Setup

```sql
-- Check BU auto-discovery results
SELECT * FROM `kpi-auto-471020.st_ref_v2.dim_bu_rollup`
ORDER BY bu_group, bu_rollup;

-- View latest weekly KPIs
SELECT *
FROM `kpi-auto-471020.st_mart_v2.kpi_weekly_by_bu`
WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
ORDER BY week_start DESC, bu_rollup;
```

### 3. Connect to Looker Studio

1. Go to [Looker Studio](https://lookerstudio.google.com/)
2. Create → Data Source → BigQuery
3. Select `kpi-auto-471020.st_mart_v2.kpi_weekly_by_bu`
4. Create report with:
   - Date Range Control (week_start)
   - Filter Control (bu_rollup)
   - Scorecard for each KPI
   - Time series charts

---

## Business Unit Auto-Discovery

### How It Works

The SQL automatically discovers Business Unit IDs by:

1. **Scanning** `dim_business_units` reference table for name→ID pairs
2. **Matching** against your configured BU names (exact match after TRIM)
3. **Creating** `dim_bu_rollup` with discovered mappings

### Rollup Mapping

**SALES Business Units:**
| BU Name | Rollup Code | Discovered |
|---------|-------------|------------|
| Andy's Painting-Sales | ANDYS | ✅ |
| Commercial-AZ-Sales | COMM_AZ | ✅ |
| Guaranteed Painting-Sales | GUAR_TX | ⚠️ Not found |
| Nevada-Sales | NEV | ⚠️ Not found |
| Phoenix-Sales | PHX | ⚠️ Not found |
| Z-DO NOT USE - West - Sales | PHX | ⚠️ Not found |

**PRODUCTION Business Units:**
| BU Name | Rollup Code | Discovered |
|---------|-------------|------------|
| Andy's Painting-Production | ANDYS | ✅ |
| Commercial-AZ-Production | COMM_AZ | ⚠️ Not found |
| Guaranteed Painting-Production | GUAR_TX | ⚠️ Not found |
| Nevada-Production | NEV | ⚠️ Not found |
| Phoenix-Production | PHX | ✅ |
| Z-DO NOT USE - West- Production | PHX | ⚠️ Not found |

**Note:** "Z-DO NOT USE" BUs are intentionally merged into PHX rollup.

### Override Discovered IDs

If auto-discovery finds the wrong ID or you need to manually add a BU:

```sql
-- Update existing row
UPDATE `kpi-auto-471020.st_ref_v2.dim_bu_rollup`
SET businessUnitId = 12345678,
    discovered = TRUE
WHERE bu_name = 'Phoenix-Sales';

-- Insert new row
INSERT INTO `kpi-auto-471020.st_ref_v2.dim_bu_rollup`
  (businessUnitId, bu_name, bu_group, bu_rollup, discovered, created_at)
VALUES
  (87654321, 'New BU Name', 'SALES', 'NEW_ROLLUP', FALSE, CURRENT_TIMESTAMP());
```

---

## KPI Definitions

### 1. Leads (SALES)

**Definition:** Count of unique customers who requested estimates

**Business Rules:**
- SALES BUs only
- Customer name does NOT contain "test"
- Job type LIKE "%estimate%" AND NOT LIKE "%comm%"
- Count DISTINCT customerId

**SQL Source:**
```sql
SELECT *
FROM `kpi-auto-471020.st_mart_v2.kpi_leads_daily`
WHERE event_date >= '2025-01-01';
```

---

### 2. Estimates (SALES)

**Definition:** Count of estimates created

**Business Rules:**
- SALES BUs only
- Job type must be in DAILY_WBR_SALES_ALLOW list:
  - ESTIMATE- WINDOW WASHING
  - Estimate
  - Cabinets
  - ESTIMATE-RES-EXT
  - ESTIMATE-RES-INT
  - _(etc... see jobtype_lists table)_

**SQL Source:**
```sql
SELECT *
FROM `kpi-auto-471020.st_mart_v2.kpi_estimates_daily`
WHERE event_date >= '2025-01-01';
```

---

### 3. Total Booked (SALES)

**Definition:** Total invoice revenue for SALES BUs

**Business Rules:**
- SALES BUs only
- Sum of invoice.total
- Optionally filtered to allowed estimate job types

**SQL Source:**
```sql
SELECT *
FROM `kpi-auto-471020.st_mart_v2.kpi_booked_daily`
WHERE event_date >= '2025-01-01';
```

---

### 4. $ Produced (PRODUCTION)

**Definition:** Revenue from completed jobs

**Business Rules:**
- PRODUCTION BUs only
- Job types NOT in FOREMAN_PROD_EXCLUDE list:
  - PM Inspection
  - Safety Inspection
  - Window/Solar Washing
- produced = SUM(COALESCE(invoice_total_by_job, jobSubtotal))

**SQL Source:**
```sql
SELECT *
FROM `kpi-auto-471020.st_mart_v2.kpi_produced_daily`
WHERE event_date >= '2025-01-01';
```

---

### 5. G.P.M. (Gross Profit Margin) (PRODUCTION)

**Definition:** (Produced - Labor Cost) / Produced

**Business Rules:**
- PRODUCTION BUs only
- Labor cost from payroll table
- **TODO:** Materials cost not yet available - GPM only includes labor

**Formula:**
```
GPM = (Produced - Labor Cost) / Produced
```

**SQL Source:**
```sql
SELECT *
FROM `kpi-auto-471020.st_mart_v2.kpi_gpm_daily`
WHERE event_date >= '2025-01-01';
```

**Important Note:** This is labor-only GPM. Once materials data is available, update the view to include:
```sql
labor_cost + materials_cost
```

---

### 6. $ Collected (PRODUCTION)

**Definition:** Cash collected via payments

**Business Rules:**
- PRODUCTION BUs only
- Sum of payment.amount

**SQL Source:**
```sql
SELECT *
FROM `kpi-auto-471020.st_mart_v2.kpi_collected_daily`
WHERE event_date >= '2025-01-01';
```

---

### 7. Warranty % of Production (PRODUCTION)

**Definition:** Warranty work as percentage of total production

**Business Rules:**
- PRODUCTION BUs only
- Warranty jobs identified by WARRANTY_INCLUDE list:
  - Warranty
  - Touchup
- Formula: warranty_produced / total_produced

**SQL Source:**
```sql
SELECT *
FROM `kpi-auto-471020.st_mart_v2.kpi_warranty_pct_daily`
WHERE event_date >= '2025-01-01';
```

---

### 8. Future Bookings (PRODUCTION)

**Definition:** Value of jobs scheduled in next 12 months

**Business Rules:**
- PRODUCTION BUs only
- scheduledStartOn between current Monday and +1 year
- Exclude FOREMAN_PROD_EXCLUDE job types
- Sum of jobSubtotal

**SQL Source:**
```sql
SELECT *
FROM `kpi-auto-471020.st_mart_v2.kpi_future_bookings_daily`;
```

**Note:** This is a snapshot metric - shows current bookings as of query date

---

### 9. Outstanding A/R (PRODUCTION)

**Definition:** Unpaid invoice balances

**Business Rules:**
- PRODUCTION BUs only
- ar_balance >= $10
- locationName != 'Name'
- ar_balance = invoice_total - payments

**SQL Source:**
```sql
SELECT *
FROM `kpi-auto-471020.st_mart_v2.kpi_outstanding_ar_daily`;
```

**Note:** This is a snapshot metric - shows current AR as of query date

---

### 10. Estimates Scheduled (NEW KPI)

**Definition:** Count of estimate jobs scheduled in period

**Business Rules:**
- Job type ILIKE "%estimate%"
- Counted by scheduledStartOn date (NOT createdOn)
- Available in both daily and weekly views

**SQL Source (Daily):**
```sql
SELECT *
FROM `kpi-auto-471020.st_mart_v2.kpi_estimates_scheduled_daily`
WHERE event_date >= '2025-01-01';
```

**SQL Source (Weekly):**
```sql
SELECT *
FROM `kpi-auto-471020.st_mart_v2.kpi_estimates_scheduled_weekly`
WHERE week_start >= '2025-01-01';
```

**Why This Matters:** Tracks scheduling activity separately from estimate creation, showing booking lead time.

---

## Weekly Rollup View

### kpi_weekly_by_bu

**Purpose:** Single view with all KPIs rolled up by week and BU

**Schema:**
```sql
bu_rollup STRING           -- ANDYS, COMM_AZ, GUAR_TX, NEV, PHX
week_start DATE            -- Monday of each week
leads INT64                -- From kpi_leads_daily
estimates INT64            -- From kpi_estimates_daily
total_booked FLOAT64       -- From kpi_booked_daily
produced FLOAT64           -- From kpi_produced_daily
gpm_ratio FLOAT64          -- From kpi_gpm_daily
collected FLOAT64          -- From kpi_collected_daily
warranty_pct FLOAT64       -- From kpi_warranty_pct_daily
outstanding_ar FLOAT64     -- Snapshot from kpi_outstanding_ar_daily
future_bookings FLOAT64    -- Snapshot from kpi_future_bookings_daily
estimates_scheduled INT64  -- From kpi_estimates_scheduled_weekly
```

**Example Query:**
```sql
-- Last 12 weeks for all BUs
SELECT *
FROM `kpi-auto-471020.st_mart_v2.kpi_weekly_by_bu`
WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
ORDER BY week_start DESC, bu_rollup;

-- Year-to-date for specific BU
SELECT *
FROM `kpi-auto-471020.st_mart_v2.kpi_weekly_by_bu`
WHERE bu_rollup = 'ANDYS'
  AND week_start >= DATE_TRUNC(CURRENT_DATE(), YEAR)
ORDER BY week_start DESC;

-- Compare BUs for specific week
SELECT *
FROM `kpi-auto-471020.st_mart_v2.kpi_weekly_by_bu`
WHERE week_start = '2025-10-20'  -- Monday of week
ORDER BY produced DESC;
```

---

## Looker Studio Dashboard

### Recommended Layout

**Page 1: Executive Overview**
- Date Range Control (week_start)
- Filter: bu_rollup
- Scorecards:
  - Leads (current week vs prior week)
  - Estimates (current week vs prior week)
  - Total Booked (current week vs prior week)
  - Produced (current week vs prior week)
- Time Series Chart: All metrics over time

**Page 2: Sales Metrics**
- Focus on SALES BUs
- Lead Conversion Funnel: Leads → Estimates → Booked
- Conversion Rates:
  - Estimates / Leads
  - Booked / Estimates
- Trend lines by BU

**Page 3: Production Metrics**
- Focus on PRODUCTION BUs
- Production vs Collection (stacked bars)
- GPM Trend Line
- Warranty % Trend Line
- Future Bookings snapshot

**Page 4: Financial Health**
- Outstanding A/R by BU (table)
- A/R Aging (requires additional views - TODO)
- Collection Efficiency: Collected / Produced

### Connection Steps

1. **Create Data Source**
   - Looker Studio → Create → Data Source
   - Connector: BigQuery
   - Project: kpi-auto-471020
   - Dataset: st_mart_v2
   - Table: kpi_weekly_by_bu

2. **Configure Fields**
   - week_start: Type = Date
   - All numeric fields: Type = Number, Aggregation = Sum
   - gpm_ratio, warranty_pct: Type = Percent

3. **Create Report**
   - Use data source
   - Add Date Range control (week_start)
   - Add Filter control (bu_rollup)
   - Add charts

### Sample Chart Configurations

**Time Series (Produced):**
- Dimension: week_start
- Metric: produced
- Breakdown: bu_rollup
- Sort: week_start ASC

**Scorecard (Leads):**
- Metric: leads
- Comparison: Prior period
- Date range: Current week

**Table (All KPIs):**
- Dimensions: bu_rollup, week_start
- Metrics: All numeric fields
- Sort: week_start DESC

---

## Data Refresh Schedule

### Current Setup

- **Raw data ingestion:** Daily at 2 AM (entity ingestors)
- **Reference data:** Daily at 3 AM (dim_business_units, dim_technicians)
- **Marts:** Views query raw data in real-time - no refresh needed!

### Materialized Tables (Optional Performance Optimization)

If views become slow, materialize the daily KPIs:

```sql
-- Example: Materialize kpi_produced_daily
CREATE TABLE `kpi-auto-471020.st_mart_v2.kpi_produced_daily_mat`
PARTITION BY event_date
CLUSTER BY bu_rollup
AS
SELECT * FROM `kpi-auto-471020.st_mart_v2.kpi_produced_daily`;

-- Schedule daily refresh via Cloud Scheduler
-- curl to Cloud Run function that runs:
-- DELETE FROM kpi_produced_daily_mat WHERE event_date >= CURRENT_DATE() - 7;
-- INSERT INTO kpi_produced_daily_mat SELECT * FROM kpi_produced_daily WHERE event_date >= CURRENT_DATE() - 7;
```

---

## Troubleshooting

### Issue: Missing Business Units in kpi_weekly_by_bu

**Symptom:** Some BUs show 0 for all metrics

**Causes:**
1. BU not discovered (businessUnitId = NULL)
2. No data in source tables for that BU
3. BU filters excluding data

**Solution:**
```sql
-- Check discovery status
SELECT * FROM `kpi-auto-471020.st_ref_v2.dim_bu_rollup`
WHERE discovered = FALSE;

-- Manually set BU ID
UPDATE `kpi-auto-471020.st_ref_v2.dim_bu_rollup`
SET businessUnitId = <correct_id>, discovered = TRUE
WHERE bu_name = '<bu_name>';

-- Verify data exists in raw tables
SELECT businessUnitId, COUNT(*)
FROM `kpi-auto-471020.st_raw_v2.raw_jobs`
GROUP BY businessUnitId
ORDER BY COUNT(*) DESC;
```

### Issue: KPI values don't match legacy reports

**Symptom:** Numbers different from manual ST reports

**Causes:**
1. Date range mismatch (week boundaries)
2. Filter logic differences
3. Missing job types in allow/exclude lists
4. Business Unit mapping errors

**Solution:**
```sql
-- Compare source record counts
-- Legacy: Leads report for 2025-10-14 to 2025-10-20
-- Mart: week_start = 2025-10-20 (Monday)

SELECT event_date, COUNT(DISTINCT customerId)
FROM `kpi-auto-471020.st_mart_v2.v_estimates_daily`
WHERE event_date BETWEEN '2025-10-14' AND '2025-10-20'
  AND LOWER(customerName) NOT LIKE '%test%'
  AND LOWER(jobTypeName) LIKE '%estimate%'
  AND LOWER(jobTypeName) NOT LIKE '%comm%'
GROUP BY event_date
ORDER BY event_date;

-- Check job type list coverage
SELECT DISTINCT jobTypeName
FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
JOIN `kpi-auto-471020.st_ref_v2.dim_bu_rollup` bu
  ON e.businessUnitId = bu.businessUnitId
WHERE bu.bu_group = 'SALES'
  AND jobTypeName NOT IN (
    SELECT jobTypeName FROM `kpi-auto-471020.st_ref_v2.jobtype_lists`
    WHERE list = 'DAILY_WBR_SALES_ALLOW'
  )
ORDER BY jobTypeName;
```

### Issue: Null values in GPM calculation

**Symptom:** gpm_ratio = 0 or NULL

**Causes:**
1. No payroll data for production jobs
2. Division by zero (produced = 0)
3. Date mismatch between jobs and payroll

**Solution:**
```sql
-- Check payroll coverage
SELECT
  DATE(j.completedOn) AS job_date,
  COUNT(DISTINCT j.jobNumber) AS jobs_completed,
  COUNT(DISTINCT p.jobNumber) AS jobs_with_payroll,
  SUM(SAFE_CAST(p.amount AS FLOAT64)) AS total_labor_cost
FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_payroll` p
  ON j.jobNumber = p.jobNumber
WHERE DATE(j.completedOn) >= '2025-10-01'
GROUP BY job_date
ORDER BY job_date DESC;
```

### Issue: Future Bookings or Outstanding A/R not updating

**Symptom:** Values seem stale

**Cause:** These are snapshot metrics - they reflect current state, not historical

**Solution:** This is expected behavior. These metrics show:
- **Future Bookings:** What's scheduled starting from current Monday
- **Outstanding A/R:** Current unpaid balance

If you need historical tracking:
```sql
-- Create snapshot table
CREATE TABLE `kpi-auto-471020.st_mart_v2.kpi_ar_snapshots`
PARTITION BY snapshot_date
AS
SELECT
  CURRENT_DATE() AS snapshot_date,
  *
FROM `kpi-auto-471020.st_mart_v2.kpi_outstanding_ar_daily`;

-- Schedule daily INSERT via Cloud Scheduler
```

---

## Adding New KPIs

### Example: Close Rate

**Step 1:** Create daily view
```sql
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.kpi_close_rate_daily` AS
WITH estimates_by_day AS (
  SELECT event_date, bu_rollup, COUNT(*) AS estimates
  FROM `kpi-auto-471020.st_mart_v2.kpi_estimates_daily`
  GROUP BY event_date, bu_rollup
),
sold_estimates AS (
  SELECT
    DATE(soldOn) AS event_date,
    bu.bu_rollup,
    COUNT(*) AS sold
  FROM `kpi-auto-471020.st_raw_v2.raw_estimates` e
  JOIN `kpi-auto-471020.st_ref_v2.dim_bu_rollup` bu
    ON e.businessUnitId = bu.businessUnitId
  WHERE soldOn IS NOT NULL
  GROUP BY event_date, bu_rollup
)
SELECT
  COALESCE(e.bu_rollup, s.bu_rollup) AS bu_rollup,
  COALESCE(e.event_date, s.event_date) AS event_date,
  COALESCE(e.estimates, 0) AS estimates,
  COALESCE(s.sold, 0) AS sold,
  SAFE_DIVIDE(COALESCE(s.sold, 0), NULLIF(COALESCE(e.estimates, 0), 0)) AS close_rate
FROM estimates_by_day e
FULL OUTER JOIN sold_estimates s USING (bu_rollup, event_date);
```

**Step 2:** Add to weekly rollup
```sql
-- Modify kpi_weekly_by_bu to include:
close_rate_weekly AS (
  SELECT
    bu_rollup,
    DATE_TRUNC(event_date, WEEK(MONDAY)) AS week_start,
    SAFE_DIVIDE(SUM(sold), NULLIF(SUM(estimates), 0)) AS close_rate
  FROM `kpi-auto-471020.st_mart_v2.kpi_close_rate_daily`
  GROUP BY bu_rollup, week_start
)

-- Add to final SELECT:
COALESCE(cr.close_rate, 0) AS close_rate
```

---

## Maintenance

### Weekly Review Checklist

- [ ] Verify all BUs have data in `kpi_weekly_by_bu`
- [ ] Check for any BUs with `discovered = FALSE`
- [ ] Validate KPIs match legacy reports (spot check)
- [ ] Review GPM for anomalies
- [ ] Check A/R growth trend

### Monthly Reconciliation

- [ ] Compare monthly totals to accounting system
- [ ] Audit job type lists - add any new types
- [ ] Review BU mappings for org changes
- [ ] Check for orphaned data (jobs without BUs)

### Alerts to Set Up

```sql
-- Alert: GPM below 20%
SELECT * FROM `kpi-auto-471020.st_mart_v2.kpi_gpm_daily`
WHERE gpm_ratio < 0.20
  AND event_date >= CURRENT_DATE() - 7;

-- Alert: A/R growing > 10% week-over-week
WITH ar_growth AS (
  SELECT
    bu_rollup,
    week_start,
    outstanding_ar,
    LAG(outstanding_ar) OVER (PARTITION BY bu_rollup ORDER BY week_start) AS prev_week_ar,
    SAFE_DIVIDE(
      outstanding_ar - LAG(outstanding_ar) OVER (PARTITION BY bu_rollup ORDER BY week_start),
      NULLIF(LAG(outstanding_ar) OVER (PARTITION BY bu_rollup ORDER BY week_start), 0)
    ) AS ar_growth_pct
  FROM `kpi-auto-471020.st_mart_v2.kpi_weekly_by_bu`
)
SELECT * FROM ar_growth
WHERE ar_growth_pct > 0.10
  AND week_start = DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY));
```

---

## FAQs

**Q: Why are some BUs missing from the weekly rollup?**
A: Check `dim_bu_rollup.discovered` - if FALSE, the BU name wasn't found in the reference table. Manually set the businessUnitId.

**Q: Can I add more BUs to the rollup mapping?**
A: Yes! Edit the `target_bus` CTE in st_mart_v2_kpis.sql and re-run. Or manually INSERT into `dim_bu_rollup`.

**Q: How do I change the DAILY_WBR_SALES_ALLOW list?**
A: INSERT or DELETE from `st_ref_v2.jobtype_lists` where list = 'DAILY_WBR_SALES_ALLOW'.

**Q: Why is GPM different from what I calculated manually?**
A: The mart currently only includes labor cost (from payroll). Materials cost is not yet available. Once you have materials data, update `kpi_gpm_daily` to include it.

**Q: Can I query a specific day instead of weekly rollup?**
A: Yes! Use the `kpi_*_daily` views directly. The weekly rollup is just for convenience.

**Q: How do I export to CSV for Excel?**
A: BigQuery Console → Query Results → Download → CSV. Or use `bq extract` command.

**Q: Can I schedule automated PDF reports?**
A: Yes! Use Looker Studio "Schedule email" or Cloud Scheduler + Puppeteer to generate PDFs.

---

## Support & Feedback

**Questions?**
- Check the main [README.md](README.md) for v2 ingestor architecture
- Review [README_REF.md](README_REF.md) for reference dimensions

**Found a Bug?**
- Verify SQL in `st_mart_v2_kpis.sql` is up-to-date
- Check raw data quality in `st_raw_v2.*` tables
- Test individual daily views before blaming weekly rollup

**Feature Requests?**
- Add new KPIs using the template in "Adding New KPIs" section
- Submit pull request or document in team wiki

---

**Generated:** 2025-10-21
**Version:** 1.0.0
**Maintainer:** ST KPI Ingestor v2 Team
**Last Updated:** Initial release with Estimates Scheduled KPI
