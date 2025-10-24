# Standard KPI Views Documentation

This directory contains SQL view definitions for three core KPI categories: **Opportunities/Close Rate**, **Leads**, and **Completed Estimates**.

## Overview

All views follow a consistent two-tier architecture:
- **Stage Views** (`st_stage.*`): Row-level data filtered and transformed from raw tables
- **Mart Views** (`st_mart_v2.*`): Daily aggregations by business unit for reporting

## Quick Start

### Deploy All Views
```bash
cd /Users/calebpena/Desktop/repos/st-kpi-ingestor/v2_ingestor
./deploy_kpi_views.sh
```

### Validate Deployment
```bash
bq query --use_legacy_sql=false < validation/validate_kpi_views.sql
```

---

## 1. Opportunities / Close Rate

### Business Logic
- **Sales Opportunity** = any job with ≥ 1 estimate
- **Closed Opportunity** = any job with ≥ 1 sold estimate
- **Opportunity Date** = earliest `soldOn` timestamp if sold estimate exists; otherwise use job's `completedOn`
- **Close Rate** = Closed Opportunities / Sales Opportunities × 100
- **Timezone**: All dates converted to America/Phoenix before aggregation

### Views

#### `st_stage.opportunity_jobs`
**Grain:** One row per job with at least one estimate

**Key Fields:**
- `job_id`, `customer_id`, `business_unit_id`
- `estimate_count`, `sold_estimate_count`
- `opportunity_date` (DATE in Phoenix timezone)
- `is_sales_opportunity` (BOOLEAN)
- `is_closed_opportunity` (BOOLEAN)

**Sample Query:**
```sql
SELECT
  opportunity_date,
  business_unit_id,
  COUNT(*) as jobs_with_estimates,
  SUM(CASE WHEN is_closed_opportunity THEN 1 ELSE 0 END) as closed
FROM `kpi-auto-471020.st_stage.opportunity_jobs`
WHERE opportunity_date >= '2025-01-01'
GROUP BY opportunity_date, business_unit_id
ORDER BY opportunity_date DESC
LIMIT 10;
```

#### `st_mart_v2.opportunity_daily`
**Grain:** One row per date per business unit

**Key Fields:**
- `kpi_date` (DATE)
- `business_unit_id`, `business_unit_name`
- `sales_opportunities` (INT)
- `closed_opportunities` (INT)
- `close_rate_percent` (FLOAT)
- `unique_customers`, `total_estimates`, `total_sold_estimates`

**Sample Query:**
```sql
SELECT
  kpi_date,
  business_unit_name,
  sales_opportunities,
  closed_opportunities,
  close_rate_percent
FROM `kpi-auto-471020.st_mart_v2.opportunity_daily`
WHERE kpi_date >= '2025-10-01'
ORDER BY kpi_date DESC, business_unit_name
LIMIT 20;
```

#### `st_stage.estimate_with_opportunity` (Optional)
**Grain:** One row per estimate

Enriches raw estimates with job-level opportunity information. Useful for drill-down analysis.

**Key Fields:**
- `estimate_id`, `job_id`, `estimate_status`, `sold_on_utc`
- All job-level opportunity fields from `opportunity_jobs`
- `is_winning_estimate` (TRUE if this is the earliest sold estimate for the job)

---

## 2. Leads (Unique Customers Created)

### Business Logic
- **Lead** = unique customer with at least one eligible job created in a date range
- **Eligible Jobs**: Only jobs with `jobTypeId` in approved allowlist (19 estimate-type job types)
- **Lead Date**: Job `createdOn` converted to America/Phoenix timezone
- **Uniqueness**: Distinct `customerId` per day (company-wide or per BU)

### Job Type Allowlist
```
705557  (ESTIMATE-COMM-INT)
705812  (ESTIMATE-RES-INT)
727444  (ESTIMATE-COMM-EXT)
727572  (ESTIMATE-RES-EXT)
7761171 (ESTIMATE-FLOOR COATINGS-EPOXY)
25643501, 25640548, 40084045, 40091077, 40528050,
52632595, 53425776, 53419951, 53417012, 66527167,
80656917, 142931404, 144397449, 365792375
```

### Views

#### `st_stage.leads_jobs`
**Grain:** One row per job matching allowlist

**Key Fields:**
- `job_id`, `customer_id`, `business_unit_id`, `job_type_id`
- `lead_date` (DATE in Phoenix timezone)
- `job_created_on_utc`, `job_completed_on_utc`

**Sample Query:**
```sql
SELECT
  job_type_id,
  COUNT(*) as total_jobs,
  COUNT(DISTINCT customer_id) as unique_customers
FROM `kpi-auto-471020.st_stage.leads_jobs`
WHERE lead_date >= '2025-01-01'
GROUP BY job_type_id
ORDER BY total_jobs DESC;
```

#### `st_mart_v2.leads_daily` (Company-Wide)
**Grain:** One row per date (all BUs combined)

**Key Fields:**
- `kpi_date` (DATE)
- `unique_leads` (COUNT DISTINCT customers)
- `total_lead_jobs`, `business_units_with_leads`

**Sample Query:**
```sql
SELECT
  kpi_date,
  unique_leads,
  total_lead_jobs
FROM `kpi-auto-471020.st_mart_v2.leads_daily`
WHERE kpi_date >= '2025-01-01'
ORDER BY kpi_date DESC
LIMIT 30;
```

#### `st_mart_v2.leads_daily_bu` (By Business Unit)
**Grain:** One row per date per business unit

**Key Fields:**
- `kpi_date` (DATE)
- `business_unit_id`, `business_unit_name`
- `unique_leads` (COUNT DISTINCT customers per BU)
- `total_lead_jobs`, `total_jobs_including_duplicates`

**Sample Query:**
```sql
SELECT
  kpi_date,
  business_unit_name,
  unique_leads,
  total_lead_jobs
FROM `kpi-auto-471020.st_mart_v2.leads_daily_bu`
WHERE kpi_date BETWEEN '2025-10-01' AND '2025-10-23'
ORDER BY kpi_date DESC, unique_leads DESC
LIMIT 20;
```

**Note:** Sum of `unique_leads` across BUs may exceed company-wide total if customers have jobs in multiple BUs on the same day.

---

## 3. Completed Estimates (Total Jobs Completed)

### Business Logic
- **Completed Estimate** = one completed job from the allowlist
- **Eligible Jobs**: Same 19 jobTypeIds as Leads
- **Completed Date**: Job `completedOn` converted to America/Phoenix timezone
- **Filter**: Only jobs where `completedOn IS NOT NULL`

### Views

#### `st_stage.completed_estimates_jobs`
**Grain:** One row per completed job matching allowlist

**Key Fields:**
- `job_id`, `customer_id`, `business_unit_id`, `job_type_id`
- `completed_date` (DATE in Phoenix timezone)
- `job_created_on_utc`, `job_completed_on_utc`

**Sample Query:**
```sql
SELECT
  completed_date,
  business_unit_id,
  COUNT(*) as jobs_completed
FROM `kpi-auto-471020.st_stage.completed_estimates_jobs`
WHERE completed_date >= '2025-01-01'
GROUP BY completed_date, business_unit_id
ORDER BY completed_date DESC
LIMIT 10;
```

#### `st_mart_v2.completed_estimates_daily`
**Grain:** One row per date per business unit

**Key Fields:**
- `kpi_date` (DATE)
- `business_unit_id`, `business_unit_name`
- `completed_estimates` (COUNT DISTINCT jobs)
- `unique_customers`, `job_types_completed`

**Sample Query:**
```sql
SELECT
  kpi_date,
  business_unit_name,
  completed_estimates,
  unique_customers
FROM `kpi-auto-471020.st_mart_v2.completed_estimates_daily`
WHERE kpi_date >= '2025-10-01'
ORDER BY kpi_date DESC, completed_estimates DESC
LIMIT 20;
```

---

## Cross-View Analysis

### Leads Funnel Analysis
```sql
SELECT
  l.kpi_date,
  l.business_unit_name,
  l.unique_leads,
  COALESCE(c.completed_estimates, 0) as completed_estimates,
  COALESCE(o.sales_opportunities, 0) as sales_opportunities,
  COALESCE(o.closed_opportunities, 0) as closed_opportunities,
  COALESCE(o.close_rate_percent, 0) as close_rate_percent,

  -- Conversion rates
  ROUND(SAFE_DIVIDE(c.completed_estimates, l.unique_leads) * 100, 2) as completion_rate,
  ROUND(SAFE_DIVIDE(o.closed_opportunities, l.unique_leads) * 100, 2) as lead_to_close_rate

FROM `kpi-auto-471020.st_mart_v2.leads_daily_bu` l
LEFT JOIN `kpi-auto-471020.st_mart_v2.completed_estimates_daily` c
  ON l.kpi_date = c.kpi_date AND l.business_unit_id = c.business_unit_id
LEFT JOIN `kpi-auto-471020.st_mart_v2.opportunity_daily` o
  ON l.kpi_date = o.kpi_date AND l.business_unit_id = o.business_unit_id

WHERE l.kpi_date >= '2025-10-01'
ORDER BY l.kpi_date DESC, l.unique_leads DESC
LIMIT 50;
```

### Monthly Summary
```sql
SELECT
  DATE_TRUNC(kpi_date, MONTH) as month,
  business_unit_name,
  SUM(unique_leads) as monthly_leads,
  SUM(completed_estimates) as monthly_completions,
  AVG(close_rate_percent) as avg_close_rate
FROM (
  SELECT
    l.kpi_date,
    l.business_unit_name,
    l.unique_leads,
    COALESCE(c.completed_estimates, 0) as completed_estimates,
    COALESCE(o.close_rate_percent, 0) as close_rate_percent
  FROM `kpi-auto-471020.st_mart_v2.leads_daily_bu` l
  LEFT JOIN `kpi-auto-471020.st_mart_v2.completed_estimates_daily` c
    ON l.kpi_date = c.kpi_date AND l.business_unit_id = c.business_unit_id
  LEFT JOIN `kpi-auto-471020.st_mart_v2.opportunity_daily` o
    ON l.kpi_date = o.kpi_date AND l.business_unit_id = o.business_unit_id
  WHERE l.kpi_date >= '2025-01-01'
)
GROUP BY month, business_unit_name
ORDER BY month DESC, monthly_leads DESC;
```

---

## Data Quality Checks

### Verify Allowlist Coverage
```sql
SELECT
  jobTypeId,
  COUNT(*) as job_count,
  COUNT(DISTINCT customerId) as unique_customers
FROM `kpi-auto-471020.st_raw_v2.raw_jobs`
WHERE jobTypeId IN (
  705557, 705812, 727444, 727572, 7761171,
  25643501, 25640548, 40084045, 40091077, 40528050,
  52632595, 53425776, 53419951, 53417012, 66527167,
  80656917, 142931404, 144397449, 365792375
)
GROUP BY jobTypeId
ORDER BY job_count DESC;
```

### Check for Data Gaps
```sql
WITH date_spine AS (
  SELECT date
  FROM UNNEST(GENERATE_DATE_ARRAY('2025-01-01', CURRENT_DATE())) AS date
)
SELECT
  d.date,
  COALESCE(l.unique_leads, 0) as leads,
  COALESCE(c.completed_estimates, 0) as completed,
  CASE
    WHEN l.unique_leads IS NULL AND c.completed_estimates IS NULL THEN 'No Data'
    WHEN l.unique_leads = 0 AND c.completed_estimates = 0 THEN 'Zero Activity'
    ELSE 'OK'
  END as status
FROM date_spine d
LEFT JOIN (
  SELECT kpi_date, SUM(unique_leads) as unique_leads
  FROM `kpi-auto-471020.st_mart_v2.leads_daily_bu`
  GROUP BY kpi_date
) l ON d.date = l.kpi_date
LEFT JOIN (
  SELECT kpi_date, SUM(completed_estimates) as completed_estimates
  FROM `kpi-auto-471020.st_mart_v2.completed_estimates_daily`
  GROUP BY kpi_date
) c ON d.date = c.kpi_date
WHERE d.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY d.date DESC;
```

---

## Validation Results

**Last Validated:** 2025-10-23

| Metric | Value | Status |
|--------|-------|--------|
| Allowlist JobTypeIds Found | 18 of 19 | ✅ |
| Total Allowlist Jobs | 74,124 | ✅ |
| Opportunity Jobs | 68,622 | ✅ |
| Closed Opportunities | 29,607 | ✅ |
| Average Close Rate | 43.1% | ✅ |
| Unique Customers (Leads) | 62,789 | ✅ |
| Completed Jobs | 71,857 | ✅ |
| Date Range | 2020-04-01 to 2025-10-23 | ✅ |
| 2025 YTD Leads | 11,165 | ✅ |
| Days of Data (2025) | 296 | ✅ |

---

## Common Use Cases

### Looker Studio / Looker Dashboards
Use the `st_mart_v2.*` views directly as data sources. They're optimized for daily aggregation and include business unit names.

**Recommended Filters:**
- Date Range: `kpi_date`
- Business Unit: `business_unit_name` or `business_unit_id`
- Exclude test/inactive BUs: Filter out names starting with "Z-DO NOT USE"

### Ad-Hoc Analysis
Use `st_stage.*` views for row-level detail when you need to drill down or apply custom logic.

### Data Export
```sql
-- Export last 30 days of KPIs for all BUs
SELECT *
FROM `kpi-auto-471020.st_mart_v2.opportunity_daily`
WHERE kpi_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY kpi_date DESC, business_unit_name;
```

---

## Troubleshooting

### NULL Business Unit Names
Some jobs may not have matching records in `dim_jobs` dimension table. This results in NULL business_unit_name in mart views. The `business_unit_id` will still be populated.

**Fix:** Ensure `dim_jobs` table is up to date with all business units from raw_jobs.

### Timezone Discrepancies
All views convert UTC timestamps to America/Phoenix before extracting dates. If comparing to ServiceTitan reports, ensure they're also using Phoenix timezone.

### Missing Job Types
If a jobTypeId from the allowlist shows 0 jobs, it may be:
1. A new job type not yet used
2. An inactive/deprecated job type
3. A typo in the allowlist

---

## Maintenance

### Refresh Views
Views are created with `CREATE OR REPLACE VIEW`, so re-running the deployment script will update them without downtime:
```bash
./deploy_kpi_views.sh
```

### Update Allowlist
To add/remove jobTypeIds from the allowlist:
1. Edit `st_stage_leads_jobs.sql` and `st_stage_completed_estimates_jobs.sql`
2. Update the `WHERE jobTypeId IN (...)` clause
3. Redeploy: `./deploy_kpi_views.sh`

### Performance Optimization
If queries become slow:
1. Consider materializing stage views as tables instead of views
2. Add partitioning on date fields
3. Add clustering on business_unit_id

---

## Contact

For questions or issues with these views, contact the Data Engineering team or file an issue in the repository.
