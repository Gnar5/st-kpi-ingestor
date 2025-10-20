# Complete Data Flow & Architecture

**Last Updated:** October 20, 2025

---

## Quick Reference: Where Should Looker Connect?

**PRIMARY TABLE FOR DASHBOARDS:**
```
st_kpi_mart.kpi_daily_regional_consolidated
```

This view combines:
- Sales BUs + Production BUs by region (Phoenix, Tucson, etc.)
- All metrics: Leads, WBR, Foreman, Collections
- Updated daily for WBR, weekly for others

---

## Layer 1: Raw Data Ingestion (ServiceTitan → BigQuery)

### Source: ServiceTitan API Reports

| Data Type | Endpoint | Schedule | Destination Table | Key Columns |
|---|---|---|---|---|
| **Leads** | `/ingest/leads` | Weekly (Sun 11:50pm) | `st_raw.raw_leads` | bu_key, job_created_on, customer_name, job_type, job_id |
| **WBR (Daily)** | `/ingest/daily_wbr` | **Daily (9:00-9:10pm)** | `st_raw.raw_daily_wbr_v2` | event_date, bu_name, sales_opportunities, closed_opportunities, completed_est, total_sales |
| **WBR (Weekly)** | `/ingest/daily_wbr_consolidated` | Weekly (Sun 10:30pm) | `st_raw.raw_daily_wbr_consolidated` | event_date, bu_name, sales_opportunities, closed_opportunities, completed_est, total_sales |
| **Foreman** | `/ingest/foreman` | Weekly (Sun 11:55pm) | `st_raw.raw_foreman` | bu_key, job_id, job_start, job_type, job_subtotal, job_total_costs, job_gm_pct |
| **Collections** | `/ingest/collections` | Weekly (Sun 5:00pm) | `st_raw.raw_collections` | bu_key, payment_date, amount, job_id |
| **Future Bookings** | `/ingest/future_bookings` | Weekly (Sun 11:58pm) | `st_raw.raw_future_bookings` | bu_key, job_id, scheduled_date, job_subtotal |

### Important Notes:
- **WBR pulls 7 days** each time (rolling window)
- **Dedupe jobs** remove overlapping data from the 7-day windows
- Raw tables include `raw` column with full JSON from ServiceTitan

---

## Layer 2: Dedupe & Daily Aggregation (Raw → Fact Tables)

### Dedupe Process
After ingestion, dedupe jobs run to remove duplicates from overlapping 7-day windows:

| Endpoint | Schedule | What It Does |
|---|---|---|
| `/dedupe/daily_wbr` | Daily (10:15pm) | Keeps best record per (event_date, bu_name, estimator) |
| `/dedupe/leads` | Weekly (Mon 12:05am) | Keeps best record per (job_id, bu_key) |
| `/dedupe/foreman` | Weekly (Mon 12:10am) | Keeps best record per (job_id, bu_key) |
| `/dedupe/collections` | Weekly (Sun 5:15pm) | No actual deduplication (keeps all payments) |
| `/dedupe/future_bookings` | Weekly (Mon 12:13am) | Keeps best record per (job_id, bu_key) |

### Mart Update Process
After dedupe, mart update jobs aggregate to daily facts:

| Endpoint | Source | Destination | Aggregation |
|---|---|---|---|
| `/mart/update/leads` | `raw_leads` | `st_kpi_mart.leads_daily_fact` | COUNT(*) per (date, bu_key) |
| `/mart/update/wbr` | `raw_daily_wbr_v2` | `st_kpi_mart.wbr_daily_fact` | SUM metrics per (date, bu_key) |
| `/mart/update/foreman` | `raw_foreman` | `st_kpi_mart.foreman_daily_fact` | SUM metrics per (date, bu_key) |
| `/mart/update/collections` | `raw_collections` | `st_kpi_mart.collections_daily_fact` | SUM amounts per (date, bu_key) |

**Key Schema for `wbr_daily_fact`:**
```sql
kpi_date DATE
bu_key STRING           -- e.g., "Phoenix-Sales"
estimates INT           -- sales_opportunities
booked INT              -- closed_opportunities
completed_est INT       -- NEW! completed estimates
close_rate_decimal FLOAT
total_sales NUMERIC
avg_closed_sale NUMERIC
```

---

## Layer 3: Consolidated Join (Individual BUs)

### Table: `st_kpi_mart.kpi_daily_consolidated`

**Created by:** `/mart/update/consolidated`

**SQL Logic:**
```sql
CREATE OR REPLACE TABLE kpi_daily_consolidated AS
WITH date_bu_spine AS (
  -- Get all unique (date, bu) combinations from all fact tables
  SELECT DISTINCT kpi_date, bu_key FROM leads_daily_fact
  UNION DISTINCT
  SELECT DISTINCT kpi_date, bu_key FROM collections_daily_fact
  UNION DISTINCT
  SELECT DISTINCT kpi_date, bu_key FROM wbr_daily_fact
  UNION DISTINCT
  SELECT DISTINCT kpi_date, bu_key FROM foreman_daily_fact
)
SELECT
  s.kpi_date,
  s.bu_key,                        -- Individual BU (e.g., "Phoenix-Sales" or "Phoenix-Production")
  l.leads,
  c.collected_amount,
  w.estimates as sales_opportunities,
  w.booked as closed_opportunities,
  w.close_rate_decimal,
  w.completed_est,                 -- ← Completed estimates (from Sales)
  w.total_sales as wbr_total_sales,
  w.avg_closed_sale,
  f.total_jobs as completed_jobs,  -- ← Completed jobs (from Production)
  f.total_subtotal as job_subtotal,
  f.total_costs as job_costs,
  f.gm_pct as gross_margin_pct
FROM date_bu_spine s
LEFT JOIN leads_daily_fact l ON s.kpi_date = l.kpi_date AND s.bu_key = l.bu_key
LEFT JOIN collections_daily_fact c ON s.kpi_date = c.kpi_date AND s.bu_key = c.bu_key
LEFT JOIN wbr_daily_fact w ON s.kpi_date = w.kpi_date AND s.bu_key = w.bu_key
LEFT JOIN foreman_daily_fact f ON s.kpi_date = f.kpi_date AND s.bu_key = f.bu_key
```

**Result:** One row per (date, individual BU)

**Example:**
| kpi_date | bu_key | leads | sales_opps | completed_est | wbr_sales | completed_jobs | job_revenue |
|---|---|---|---|---|---|---|---|
| 2025-10-18 | Phoenix-Sales | 16 | 120 | 95 | $250K | NULL | NULL |
| 2025-10-18 | Phoenix-Production | NULL | NULL | NULL | NULL | 45 | $200K |

---

## Layer 4: Regional Rollup (Sales + Production Combined)

### Dimension Table: `st_kpi_mart.dim_regional_bu_map`

Maps individual BUs to regions:

| region_key | region_label | bu_key | bu_type |
|---|---|---|---|
| PHX | Phoenix | Phoenix-Sales | Sales |
| PHX | Phoenix | Phoenix-Production | Production |
| PHX | Phoenix | Z-DO NOT USE - West- Production | Production |
| TUC | Tucson | Tucson-Sales | Sales |
| TUC | Tucson | Tucson-Production | Production |
| ... | ... | ... | ... |

### View: `st_kpi_mart.kpi_daily_regional_consolidated` ⭐

**THIS IS YOUR MAIN LOOKER TABLE!**

**SQL Logic:**
```sql
CREATE OR REPLACE VIEW kpi_daily_regional_consolidated AS
WITH regional_data AS (
  SELECT
    k.kpi_date,
    m.region_key,
    m.region_label,
    k.leads,
    k.collected_amount,
    k.sales_opportunities,
    k.closed_opportunities,
    k.close_rate_decimal,
    k.completed_est,           -- From Sales BU
    k.wbr_total_sales,
    k.avg_closed_sale,
    k.completed_jobs,          -- From Production BU
    k.job_subtotal,
    k.job_costs,
    k.gross_margin_pct
  FROM kpi_daily_consolidated k
  JOIN dim_regional_bu_map m ON k.bu_key = m.bu_key
)
SELECT
  kpi_date,
  region_key,
  region_label,
  -- SUM across Sales + Production BUs for the same region
  SUM(leads) as total_leads,
  SUM(collected_amount) as total_collections,
  SUM(sales_opportunities) as total_estimates,
  SUM(closed_opportunities) as total_booked,
  SAFE_DIVIDE(SUM(closed_opportunities), SUM(sales_opportunities)) as region_close_rate,
  SUM(completed_est) as total_completed_est,
  SUM(wbr_total_sales) as total_wbr_sales,
  SAFE_DIVIDE(SUM(wbr_total_sales), SUM(closed_opportunities)) as region_avg_closed_sale,
  SUM(completed_jobs) as total_completed_jobs,
  SUM(job_subtotal) as total_job_revenue,
  SUM(job_costs) as total_job_costs,
  SAFE_DIVIDE((SUM(job_subtotal) - SUM(job_costs)) * 100.0, SUM(job_subtotal)) as region_gross_margin_pct
FROM regional_data
GROUP BY kpi_date, region_key, region_label
```

**Result:** One row per (date, region)

**Example:**
| kpi_date | region_label | total_leads | total_estimates | total_completed_est | total_wbr_sales | total_completed_jobs | total_job_revenue |
|---|---|---|---|---|---|---|---|
| 2025-10-18 | Phoenix | 16 | 120 | 95 | $250K | 45 | $200K |
| 2025-10-18 | Tucson | 5 | 45 | 35 | $95K | 22 | $80K |

---

## Complete Data Flow Diagram

```
ServiceTitan API
       │
       ├─ /ingest/leads ────────────→ raw_leads ────────────┐
       ├─ /ingest/daily_wbr ────────→ raw_daily_wbr_v2 ─────┤
       ├─ /ingest/foreman ──────────→ raw_foreman ──────────┤
       └─ /ingest/collections ──────→ raw_collections ──────┤
                                                             │
                                     ┌───────────────────────┘
                                     ↓
                            /dedupe/* endpoints
                                     │
                                     ↓
       ┌────────────────────────────────────────────────────┐
       │            Individual BU Fact Tables               │
       ├────────────────────────────────────────────────────┤
       │  leads_daily_fact       (date, bu_key, leads)      │
       │  wbr_daily_fact         (date, bu_key, estimates,  │
       │                          booked, completed_est)    │
       │  foreman_daily_fact     (date, bu_key,             │
       │                          completed_jobs)           │
       │  collections_daily_fact (date, bu_key, amount)     │
       └────────────────────────────────────────────────────┘
                                     │
                                     ↓ /mart/update/consolidated
       ┌────────────────────────────────────────────────────┐
       │         kpi_daily_consolidated                     │
       │  (One row per date per individual BU)              │
       │  - Phoenix-Sales: leads, estimates, completed_est  │
       │  - Phoenix-Production: completed_jobs, revenue     │
       └────────────────────────────────────────────────────┘
                                     │
                                     ↓ JOIN with dim_regional_bu_map
       ┌────────────────────────────────────────────────────┐
       │      kpi_daily_regional_consolidated ⭐            │
       │  (One row per date per REGION)                     │
       │  - Phoenix: SUM(Sales + Production metrics)        │
       │  - Tucson: SUM(Sales + Production metrics)         │
       └────────────────────────────────────────────────────┘
                                     │
                                     ↓
                              Looker Dashboard
```

---

## Key Concepts for Troubleshooting

### 1. NULL vs 0 vs Missing
- **NULL** in Sales metrics (Phoenix-Production) = Expected (Production BUs don't have sales data)
- **NULL** in Production metrics (Phoenix-Sales) = Expected (Sales BUs don't have production data)
- **NULL** in regional view after SUM = Problem! Should have data from at least one BU

### 2. Data Freshness
- **WBR**: Updated **daily** (last run determines latest data)
- **Leads/Collections/Foreman**: Updated **weekly Sunday** (won't have Mon-Sat current week data)

### 3. Date Ranges
- **WBR ingests 7 days** each run (e.g., today pulls Oct 14-20)
- **Dedupe** removes duplicates from overlapping windows
- **Mart update** aggregates ALL data in raw table (not just last 7 days)

### 4. Common Issues

**Issue:** Regional view shows NULL for WBR metrics
**Cause:** Either `wbr_daily_fact` is empty, or JOIN on bu_key is failing
**Check:**
```sql
-- Check if wbr_daily_fact has data
SELECT COUNT(*), MIN(kpi_date), MAX(kpi_date)
FROM st_kpi_mart.wbr_daily_fact;

-- Check if consolidated has data
SELECT COUNT(*), MIN(kpi_date), MAX(kpi_date)
FROM st_kpi_mart.kpi_daily_consolidated
WHERE sales_opportunities IS NOT NULL;
```

**Issue:** Numbers don't match ServiceTitan
**Cause:** Could be date range issue, dedupe removing wrong records, or BU name mismatch
**Check:**
```sql
-- Check raw data for specific date
SELECT * FROM st_raw.raw_daily_wbr_v2
WHERE event_date = '2025-10-18' AND bu_name = 'Phoenix-Sales';

-- Check if BU names match mapping
SELECT * FROM st_kpi_mart.dim_regional_bu_map
WHERE bu_key LIKE '%Phoenix%';
```

---

## Manual Re-ingestion Commands

### Re-ingest WBR for specific dates:
```bash
# Daily per-BU approach (recommended)
curl "https://st-kpi-ingestor-999875365235.us-central1.run.app/ingest/daily_wbr?bu=Phoenix-Sales&from=2025-10-13&to=2025-10-19"

# Or for all BUs, run each:
for bu in "Phoenix-Sales" "Tucson-Sales" "Nevada-Sales" "Andy's Painting-Sales" "Guaranteed Painting-Sales" "Commercial-AZ-Sales"; do
  curl "https://st-kpi-ingestor-999875365235.us-central1.run.app/ingest/daily_wbr?bu=$bu&from=2025-10-13&to=2025-10-19"
  sleep 2
done
```

### After re-ingestion, update marts:
```bash
# Dedupe first
curl -X POST "https://st-kpi-ingestor-999875365235.us-central1.run.app/dedupe/daily_wbr"

# Update WBR mart
curl -X POST "https://st-kpi-ingestor-999875365235.us-central1.run.app/mart/update/wbr"

# Update consolidated
curl -X POST "https://st-kpi-ingestor-999875365235.us-central1.run.app/mart/update/consolidated"
```

---

## Verification Queries

### Check each layer:

**Layer 1 - Raw Data:**
```sql
SELECT event_date, bu_name, COUNT(*) as records, SUM(total_sales) as total
FROM st_raw.raw_daily_wbr_v2
WHERE event_date BETWEEN '2025-10-13' AND '2025-10-19'
GROUP BY event_date, bu_name
ORDER BY event_date DESC, total DESC;
```

**Layer 2 - WBR Fact:**
```sql
SELECT kpi_date, bu_key, estimates, booked, completed_est, total_sales
FROM st_kpi_mart.wbr_daily_fact
WHERE kpi_date BETWEEN '2025-10-13' AND '2025-10-19'
ORDER BY kpi_date DESC, total_sales DESC;
```

**Layer 3 - Individual BU Consolidated:**
```sql
SELECT kpi_date, bu_key, sales_opportunities, completed_est, wbr_total_sales, completed_jobs, job_subtotal
FROM st_kpi_mart.kpi_daily_consolidated
WHERE kpi_date BETWEEN '2025-10-13' AND '2025-10-19'
  AND (bu_key LIKE '%Phoenix%' OR bu_key LIKE '%Tucson%')
ORDER BY kpi_date DESC, bu_key;
```

**Layer 4 - Regional Consolidated:**
```sql
SELECT kpi_date, region_label, total_estimates, total_completed_est, total_wbr_sales, total_completed_jobs, total_job_revenue
FROM st_kpi_mart.kpi_daily_regional_consolidated
WHERE kpi_date BETWEEN '2025-10-13' AND '2025-10-19'
ORDER BY kpi_date DESC, total_wbr_sales DESC;
```

---

## Ready for Your Long Message!

I'm ready to hear about:
1. What you're seeing in Looker
2. What you expect to see
3. Which dates/regions are wrong
4. What the numbers should be (if you know from ServiceTitan)

We can trace through each layer to find where the discrepancy is!
