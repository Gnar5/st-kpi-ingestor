# ServiceTitan KPI Mart Data Flow

## Overview
All raw data now flows into the mart layer for reporting and dashboards!

## Mart Tables

### 1. **leads_daily_fact**
- **Source**: `st_raw.raw_leads`
- **Grain**: Daily by Business Unit
- **Columns**:
  - `kpi_date` - Date of job creation
  - `bu_key` - Business Unit
  - `leads` - Count of leads

### 2. **collections_daily_fact**
- **Source**: `st_raw.raw_collections`
- **Grain**: Daily by Business Unit
- **Columns**:
  - `kpi_date` - Payment date
  - `bu_key` - Business Unit (Production)
  - `collected_amount` - Total collected (includes negative refunds)

### 3. **wbr_daily_fact** (Weekly Business Review)
- **Source**: `st_raw.raw_daily_wbr_v2`
- **Grain**: Daily by Business Unit
- **Columns**:
  - `kpi_date` - Event date
  - `bu_key` - Business Unit (Sales)
  - `estimates` - Sales opportunities
  - `booked` - Closed opportunities
  - `close_rate_decimal` - Close rate (e.g., 0.3727 = 37.27%)
  - `total_sales` - Total sales dollars
  - `avg_closed_sale` - Average sale amount

### 4. **foreman_daily_fact**
- **Source**: `st_raw.raw_foreman`
- **Grain**: Daily by Business Unit
- **Columns**:
  - `kpi_date` - Job start date
  - `bu_key` - Business Unit (Production)
  - `total_jobs` - Count of jobs
  - `total_subtotal` - Total job revenue
  - `total_costs` - Total job costs
  - `gm_pct` - Gross margin percentage

## Mart Update Endpoints

### Individual Mart Updates
```bash
# Update leads mart
curl -X POST https://st-kpi-ingestor-gnz5sx34ba-uc.a.run.app/mart/update/leads

# Update collections mart
curl -X POST https://st-kpi-ingestor-gnz5sx34ba-uc.a.run.app/mart/update/collections

# Update WBR mart
curl -X POST https://st-kpi-ingestor-gnz5sx34ba-uc.a.run.app/mart/update/wbr

# Update foreman mart
curl -X POST https://st-kpi-ingestor-gnz5sx34ba-uc.a.run.app/mart/update/foreman
```

### Update All Marts
```bash
# Update all marts at once
curl -X POST https://st-kpi-ingestor-gnz5sx34ba-uc.a.run.app/mart/update/all
```

## Verification Queries

### WBR Weekly Summary (Oct 6-12, 2025)
```sql
SELECT
  bu_key,
  SUM(estimates) as total_sales_opps,
  SUM(booked) as total_closed_opps,
  ROUND(SUM(booked) * 100.0 / NULLIF(SUM(estimates), 0), 2) as close_rate_pct,
  ROUND(SUM(total_sales), 2) as total_sales
FROM `kpi-auto-471020.st_kpi_mart.wbr_daily_fact`
WHERE kpi_date BETWEEN '2025-10-06' AND '2025-10-12'
GROUP BY bu_key
ORDER BY bu_key;
```

**Results (matches ServiceTitan perfectly!):**
| Business Unit | Sales Opps | Closed | Close Rate % | Total Sales |
|--------------|------------|--------|--------------|-------------|
| Andy's | 24 | 8 | 33.33% | $24,681.65 |
| Commercial | 20 | 9 | 45.00% | $83,848.00 |
| Guaranteed | 8 | 4 | 50.00% | $14,392.00 |
| Nevada | 23 | 9 | 39.13% | $42,325.00 |
| Phoenix | 110 | 41 | 37.27% | $254,217.78 |
| Tucson | 46 | 13 | 28.26% | $56,341.66 |

### Collections Weekly Summary
```sql
SELECT
  bu_key,
  ROUND(SUM(collected_amount), 2) as total_collected
FROM `kpi-auto-471020.st_kpi_mart.collections_daily_fact`
WHERE kpi_date BETWEEN '2025-10-06' AND '2025-10-12'
GROUP BY bu_key
ORDER BY bu_key;
```

### Leads Weekly Summary
```sql
SELECT
  bu_key,
  SUM(leads) as total_leads
FROM `kpi-auto-471020.st_kpi_mart.leads_daily_fact`
WHERE kpi_date BETWEEN '2025-10-06' AND '2025-10-12'
GROUP BY bu_key
ORDER BY bu_key;
```

### Foreman Weekly Summary
```sql
SELECT
  bu_key,
  SUM(total_jobs) as jobs,
  ROUND(SUM(total_subtotal), 2) as subtotal,
  ROUND(SUM(total_costs), 2) as costs,
  ROUND((SUM(total_subtotal) - SUM(total_costs)) * 100.0 / NULLIF(SUM(total_subtotal), 0), 2) as gm_pct
FROM `kpi-auto-471020.st_kpi_mart.foreman_daily_fact`
WHERE kpi_date BETWEEN '2025-10-06' AND '2025-10-12'
GROUP BY bu_key
ORDER BY bu_key;
```

## Data Flow Pipeline

```
ServiceTitan API
    ↓
Raw Tables (st_raw.*)
    ↓ [Dedupe]
Cleaned Raw Data
    ↓ [Mart Transform]
Mart Tables (st_kpi_mart.*_daily_fact)
    ↓
Looker Studio Dashboards
```

## Key Fixes Implemented

1. **Close Rate Calculation** - Changed from AVG to proper formula: `SUM(closed) / SUM(opportunities)`
2. **Collections Dedupe** - Removed aggressive deduplication to preserve legitimate duplicate payments
3. **Collections Truncate-and-Reload** - Each ingest now truncates the date range first
4. **GM% Calculation** - Calculate from totals, not average: `(SUM(subtotal) - SUM(costs)) / SUM(subtotal) * 100`
5. **Phoenix WBR** - Fixed by cleaning technician list in ServiceTitan

## Scheduled Updates

The mart tables should be updated after raw data is ingested. Recommended schedule:

- **Monday 3:00 AM** - Run `/mart/update/all` after all weekend ingests complete
- This ensures Looker Studio has fresh data for Monday morning reports

## Current Status

✅ All raw ingestors verified and matching ServiceTitan
✅ All mart transformations created and tested
✅ All mart tables populated with historical data (back to 2023)
✅ Verification queries confirm data accuracy
✅ Ready for Looker Studio dashboard creation!
