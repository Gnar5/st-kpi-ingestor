# Consolidated KPI Mart Guide

## Overview
The `kpi_daily_consolidated` table combines ALL KPI metrics into a single table for easy dashboarding!

## Table: `st_kpi_mart.kpi_daily_consolidated`

### Schema
| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `kpi_date` | DATE | All | Date of the metrics |
| `bu_key` | STRING | All | Business Unit (Sales or Production) |
| `leads` | INT64 | Leads | Count of leads |
| `collected_amount` | NUMERIC | Collections | Total collected (with refunds) |
| `sales_opportunities` | INT64 | WBR | Sales estimates/opportunities |
| `closed_opportunities` | INT64 | WBR | Booked/closed opportunities |
| `close_rate_decimal` | NUMERIC | WBR | Close rate (0.3727 = 37.27%) |
| `wbr_total_sales` | NUMERIC | WBR | Total sales from WBR |
| `avg_closed_sale` | NUMERIC | WBR | Average closed sale amount |
| `completed_jobs` | INT64 | Foreman | Count of completed jobs |
| `job_subtotal` | NUMERIC | Foreman | Total job revenue |
| `job_costs` | NUMERIC | Foreman | Total job costs |
| `gross_margin_pct` | NUMERIC | Foreman | GM% calculated from totals |

### Key Points

**Business Unit Split:**
- **Sales BUs** (e.g., "Andy's Painting-Sales"): Have Leads + WBR data, NULL for Collections + Foreman
- **Production BUs** (e.g., "Andy's Painting-Production"): Have Collections + Foreman data, NULL for Leads + WBR

This is because:
- Leads and WBR are tracked by the Sales BU
- Collections and Foreman jobs are tracked by the Production BU

**Data Coverage:**
- **Total Rows:** 1,915
- **Date Range:** 2023-07-24 to 2025-10-14
- **Business Units:** 12 (6 Sales + 6 Production)

## Update Endpoint

```bash
# Update consolidated mart (run AFTER updating individual marts)
curl -X POST https://st-kpi-ingestor-gnz5sx34ba-uc.a.run.app/mart/update/consolidated

# Or update everything at once (includes consolidated)
curl -X POST https://st-kpi-ingestor-gnz5sx34ba-uc.a.run.app/mart/update/all
```

## Example Queries

### Weekly Summary by Business Unit (Separate Sales/Production)
```sql
SELECT
  bu_key,
  -- Leads (Sales BU only)
  SUM(leads) as total_leads,

  -- Collections (Production BU only)
  ROUND(SUM(collected_amount), 2) as total_collected,

  -- WBR (Sales BU only)
  SUM(sales_opportunities) as total_opps,
  SUM(closed_opportunities) as total_closed,
  ROUND(SUM(closed_opportunities) * 100.0 / NULLIF(SUM(sales_opportunities), 0), 2) as close_rate_pct,
  ROUND(SUM(wbr_total_sales), 2) as wbr_sales,

  -- Foreman (Production BU only)
  SUM(completed_jobs) as jobs,
  ROUND(SUM(job_subtotal), 2) as job_revenue,
  ROUND((SUM(job_subtotal) - SUM(job_costs)) * 100.0 / NULLIF(SUM(job_subtotal), 0), 2) as gm_pct

FROM `kpi-auto-471020.st_kpi_mart.kpi_daily_consolidated`
WHERE kpi_date BETWEEN '2025-10-06' AND '2025-10-12'
GROUP BY bu_key
ORDER BY bu_key;
```

### Combined by Region (Sales + Production BUs together)
```sql
WITH combined AS (
  SELECT
    kpi_date,
    CASE
      WHEN bu_key LIKE '%Andy%' THEN 'Andys'
      WHEN bu_key LIKE '%Commercial%' THEN 'Commercial'
      WHEN bu_key LIKE '%Guaranteed%' THEN 'Guaranteed'
      WHEN bu_key LIKE '%Nevada%' THEN 'Nevada'
      WHEN bu_key LIKE '%Phoenix%' THEN 'Phoenix'
      WHEN bu_key LIKE '%Tucson%' THEN 'Tucson'
      ELSE 'Other'
    END as region,
    leads,
    collected_amount,
    sales_opportunities,
    closed_opportunities,
    wbr_total_sales,
    completed_jobs,
    job_subtotal,
    job_costs
  FROM `kpi-auto-471020.st_kpi_mart.kpi_daily_consolidated`
)
SELECT
  region,
  SUM(leads) as total_leads,
  ROUND(SUM(collected_amount), 2) as total_collected,
  SUM(sales_opportunities) as total_opps,
  SUM(closed_opportunities) as total_closed,
  ROUND(SUM(closed_opportunities) * 100.0 / NULLIF(SUM(sales_opportunities), 0), 2) as close_rate_pct,
  ROUND(SUM(wbr_total_sales), 2) as wbr_sales,
  SUM(completed_jobs) as jobs,
  ROUND(SUM(job_subtotal), 2) as job_revenue,
  ROUND((SUM(job_subtotal) - SUM(job_costs)) * 100.0 / NULLIF(SUM(job_subtotal), 0), 2) as gm_pct
FROM combined
WHERE kpi_date BETWEEN '2025-10-06' AND '2025-10-12'
GROUP BY region
ORDER BY region;
```

### Daily Trend (All Metrics)
```sql
SELECT
  kpi_date,
  SUM(leads) as daily_leads,
  ROUND(SUM(collected_amount), 2) as daily_collected,
  SUM(sales_opportunities) as daily_opps,
  SUM(closed_opportunities) as daily_closed,
  ROUND(SUM(wbr_total_sales), 2) as daily_sales,
  SUM(completed_jobs) as daily_jobs,
  ROUND(SUM(job_subtotal), 2) as daily_revenue
FROM `kpi-auto-471020.st_kpi_mart.kpi_daily_consolidated`
WHERE kpi_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY kpi_date
ORDER BY kpi_date DESC;
```

## Looker Studio Setup

**For Looker Studio dashboards:**

1. **Connect to BigQuery**
   - Dataset: `kpi-auto-471020.st_kpi_mart`
   - Table: `kpi_daily_consolidated`

2. **Create Calculated Fields:**
   ```
   Close Rate % = (closed_opportunities / sales_opportunities) * 100
   GM % = ((job_subtotal - job_costs) / job_subtotal) * 100
   ```

3. **Filters to Add:**
   - Date range selector on `kpi_date`
   - Business Unit filter on `bu_key`
   - Region filter (use CASE statement above)

4. **Recommended Charts:**
   - **Scorecard**: Total Leads, Total Sales, Total Collections
   - **Time Series**: Daily trends for each metric
   - **Bar Chart**: KPIs by Business Unit
   - **Table**: Detailed daily breakdown by BU

## Data Flow

```
Raw Data (st_raw.*)
    ↓
Individual Fact Tables
    ├── leads_daily_fact
    ├── collections_daily_fact
    ├── wbr_daily_fact
    └── foreman_daily_fact
    ↓
Consolidated Fact Table
    └── kpi_daily_consolidated (ALL METRICS!)
    ↓
Looker Studio Dashboard
```

## Update Schedule

**Recommended:**
1. **Monday 3:00 AM** - Run `/mart/update/all`
   - This updates all individual marts AND the consolidated table
   - Runs after weekend ingests complete
   - Fresh data ready for Monday morning

**Manual Updates:**
```bash
# Update everything (recommended)
curl -X POST https://st-kpi-ingestor-gnz5sx34ba-uc.a.run.app/mart/update/all

# Or just consolidated (if individual marts already updated)
curl -X POST https://st-kpi-ingestor-gnz5sx34ba-uc.a.run.app/mart/update/consolidated
```

## Verification

**Week of Oct 6-12, 2025 (Sample Results):**

| Region | Leads | Collections | Opps | Closed | Close% | WBR Sales | Jobs | Revenue | GM% |
|--------|-------|-------------|------|--------|--------|-----------|------|---------|-----|
| Andy's | 25 | $33,524 | 24 | 8 | 33.33% | $24,682 | 37 | $49,067 | 44.04% |
| Phoenix | 134 | $191,039 | 110 | 41 | 37.27% | $254,218 | 105 | $225,471 | 50.22% |

All metrics verified against ServiceTitan! ✅
