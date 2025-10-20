# Regional Rollup Guide - Combining Sales + Production BUs

**Date:** October 20, 2025

## Overview

Your BigQuery structure now supports **regional rollups** that combine Sales and Production BUs into single regional views. This allows you to see the complete picture for each location (Phoenix, Tucson, Nevada, etc.) in one place.

---

## Key Tables & Views

### 1. **Dimension Table: `st_kpi_mart.dim_regional_bu_map`**
Maps individual BUs to their regional groupings.

**Schema:**
- `region_key` - Short code (PHX, TUC, NEV, ANDYS, GUAR_TX, COMM_AZ)
- `region_label` - Display name (Phoenix, Tucson, Nevada, Albuquerque, Texas, Phoenix Commercial)
- `bu_key` - Individual BU name (Phoenix-Sales, Phoenix-Production, etc.)
- `bu_type` - Either 'Sales' or 'Production'

**Example:**
| region_key | region_label | bu_key | bu_type |
|---|---|---|---|
| PHX | Phoenix | Phoenix-Sales | Sales |
| PHX | Phoenix | Phoenix-Production | Production |
| PHX | Phoenix | Z-DO NOT USE - West- Production | Production |

### 2. **Regional Consolidated View: `st_kpi_mart.kpi_daily_regional_consolidated`**
**This is your main Looker table!** Combines Sales + Production metrics by region.

**Columns:**
- `kpi_date` - Date
- `region_key` - Short code (PHX, TUC, etc.)
- `region_label` - Display name (Phoenix, Tucson, etc.)

**Sales Metrics** (from WBR):
- `total_estimates` - Total sales opportunities
- `total_booked` - Total closed opportunities
- `region_close_rate` - Overall close rate for the region
- `total_completed_est` - Total completed estimates
- `total_wbr_sales` - Total revenue from sales
- `region_avg_closed_sale` - Average sale amount

**Production Metrics** (from Foreman):
- `total_completed_jobs` - Total jobs finished
- `total_job_revenue` - Total revenue from completed work
- `total_job_costs` - Total costs (labor + materials)
- `region_gross_margin_pct` - Overall gross margin %

**Other Metrics:**
- `total_leads` - Lead count
- `total_collections` - Collections/payments

---

## Data Flow

```
ServiceTitan API
       │
       ├── Daily per-BU WBR calls → raw_daily_wbr_v2 (daily detail)
       ├── Weekly consolidated WBR → raw_daily_wbr_consolidated (weekly totals)
       ├── Foreman data → raw_foreman
       ├── Leads → raw_leads
       └── Collections → raw_collections
       │
       ↓
Individual BU marts (dedupe + aggregate)
       ├── wbr_daily_fact
       ├── foreman_daily_fact
       ├── leads_daily_fact
       └── collections_daily_fact
       │
       ↓
kpi_daily_consolidated (all metrics by individual BU)
       │
       ↓ (JOIN with dim_regional_bu_map)
       ↓
kpi_daily_regional_consolidated (Sales + Production combined by region)
       │
       ↓
    Looker Dashboard
```

---

## Scheduler Jobs

### Daily WBR (Individual BUs):
**Purpose:** Provide daily granular WBR data
- `wbr-andys` - 9:00pm Sunday
- `wbr-commercial-az` - 9:10pm Sunday
- `wbr-guaranteed` - 9:20pm Sunday
- `wbr-nevada` - 9:30pm Sunday
- `wbr-phoenix` - 9:40pm Sunday
- `wbr-tucson` - 9:50pm Sunday
- `wbr-dedupe` - 10:15pm Sunday

These populate `raw_daily_wbr_v2` with daily detail.

### Weekly WBR (Consolidated):
**Purpose:** Weekly totals for verification
- `wbr-weekly-consolidated` - 10:30pm Sunday
- Calls all BUs in one API request
- Stores 7-day totals in `raw_daily_wbr_consolidated`

---

## Usage in Looker

### Option A: Regional View (Recommended)
Connect Looker to: `st_kpi_mart.kpi_daily_regional_consolidated`

**Dimensions:**
- `kpi_date`
- `region_label` (Phoenix, Tucson, Nevada, etc.)

**Measures:**
- `total_leads`
- `total_estimates`
- `total_booked`
- `total_completed_est`
- `total_wbr_sales`
- `total_completed_jobs`
- `total_job_revenue`
- `total_collections`
- `region_close_rate`
- `region_gross_margin_pct`

### Option B: Individual BU Detail
Connect Looker to: `st_kpi_mart.kpi_daily_consolidated`

Use when you need to see Sales vs Production separately.

---

## Regional Mappings

| Region | Display Name | Sales BU | Production BU(s) |
|---|---|---|---|
| PHX | Phoenix | Phoenix-Sales | Phoenix-Production, Z-DO NOT USE - West- Production |
| TUC | Tucson | Tucson-Sales | Tucson-Production |
| NEV | Nevada | Nevada-Sales | Nevada-Production |
| ANDYS | Albuquerque | Andy's Painting-Sales | Andy's Painting-Production |
| GUAR_TX | Texas | Guaranteed Painting-Sales | Guaranteed Painting-Production |
| COMM_AZ | Phoenix Commercial | Commercial-AZ-Sales | Commercial-AZ-Production |

---

## Example Queries

### Daily Regional Performance:
```sql
SELECT
  kpi_date,
  region_label,
  total_leads,
  total_estimates,
  total_booked,
  total_wbr_sales,
  total_completed_jobs,
  total_job_revenue,
  total_collections
FROM `kpi-auto-471020.st_kpi_mart.kpi_daily_regional_consolidated`
WHERE kpi_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY kpi_date DESC, total_wbr_sales DESC
```

### Weekly Regional Rollup:
```sql
SELECT
  DATE_TRUNC(kpi_date, WEEK(SUNDAY)) as week_start,
  region_label,
  SUM(total_leads) as weekly_leads,
  SUM(total_estimates) as weekly_estimates,
  SUM(total_booked) as weekly_booked,
  SUM(total_wbr_sales) as weekly_sales,
  SUM(total_completed_jobs) as weekly_jobs,
  SUM(total_collections) as weekly_collections
FROM `kpi-auto-471020.st_kpi_mart.kpi_daily_regional_consolidated`
WHERE kpi_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 WEEK)
GROUP BY week_start, region_label
ORDER BY week_start DESC, weekly_sales DESC
```

---

## Maintenance

### To Add a New Region:
1. Update `config/config.json` → `bu_mapping` section
2. Run this SQL to update the dimension table:
```sql
INSERT INTO `kpi-auto-471020.st_kpi_mart.dim_regional_bu_map`
VALUES ('NEW_KEY', 'New Region Name', 'New-BU-Sales', 'Sales');
INSERT INTO `kpi-auto-471020.st_kpi_mart.dim_regional_bu_map`
VALUES ('NEW_KEY', 'New Region Name', 'New-BU-Production', 'Production');
```

### To Update Regional Mapping:
Simply rerun the CREATE OR REPLACE TABLE statement in the setup script.

---

## Benefits

✅ **Single Source of Truth** - One table per region combining Sales + Production
✅ **Simplified Dashboards** - No need to union Sales and Production in Looker
✅ **Accurate Rollups** - Handles multiple Production BUs per region (like Phoenix)
✅ **Flexible** - Can still drill down to individual BUs using `kpi_daily_consolidated`
✅ **Performance** - Pre-aggregated at the database level

---

## Next Steps

1. **Test the view** - Run sample queries to verify data
2. **Connect Looker** - Point your dashboard to `kpi_daily_regional_consolidated`
3. **Create visualizations** - Build charts by `region_label`
4. **Monitor daily jobs** - Ensure all 6 WBR jobs complete successfully
5. **Weekly verification** - Check consolidated totals match sum of daily

---

**Questions?** Check the main [BIGQUERY_STRUCTURE.md](BIGQUERY_STRUCTURE.md) for overall architecture.
