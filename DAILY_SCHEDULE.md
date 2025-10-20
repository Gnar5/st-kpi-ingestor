# Daily Ingestion Schedule

**Updated:** October 20, 2025

All times in **America/Phoenix** timezone.

---

## Daily Jobs (Run Every Night)

### WBR (Weekly Business Review) - Sales Metrics
**Purpose:** Ingest daily WBR metrics for each Sales BU

| Job | Time | BU | Pulls |
|---|---|---|---|
| `wbr-andys` | 9:00pm | Andy's Painting-Sales | Last 7 days |
| `wbr-commercial-az` | 9:02pm | Commercial-AZ-Sales | Last 7 days |
| `wbr-guaranteed` | 9:04pm | Guaranteed Painting-Sales | Last 7 days |
| `wbr-nevada` | 9:06pm | Nevada-Sales | Last 7 days |
| `wbr-phoenix` | 9:08pm | Phoenix-Sales | Last 7 days |
| `wbr-tucson` | 9:10pm | Tucson-Sales | Last 7 days |

**Destination:** `st_raw.raw_daily_wbr_v2`

### WBR Cleanup
| Job | Time | Purpose |
|---|---|---|
| `wbr-dedupe` | 10:15pm | Dedupe overlapping WBR data |

---

## Weekly Jobs (Run Sunday Nights Only)

### Weekly WBR Verification
| Job | Time | Day | Purpose |
|---|---|---|---|
| `wbr-weekly-consolidated` | 10:30pm | Sunday | Pull all BUs in one call for weekly totals verification |

**Destination:** `st_raw.raw_daily_wbr_consolidated`

### Leads
| Job | Time | Day | Purpose |
|---|---|---|---|
| `leads-weekly-ingest` | 11:50pm | Sunday | Weekly lead ingestion |
| `leads-dedupe` | 12:05am | Monday | Dedupe leads |

### Collections
| Job | Time | Day | Purpose |
|---|---|---|---|
| `collections-weekly-ingest` | 5:00pm | Sunday | Weekly collections ingestion |
| `collections-dedupe-weekly` | 5:15pm | Sunday | Dedupe collections |

### Foreman (Production)
| Job | Time | Day | Purpose |
|---|---|---|---|
| `foreman-ingest-weekly` | 11:55pm | Sunday | Weekly foreman/job cost ingestion |
| `foreman-dedupe-weekly` | 12:10am | Monday | Dedupe foreman data |

### Future Bookings
| Job | Time | Day | Purpose |
|---|---|---|---|
| `future-bookings-ingest-weekly` | 11:58pm | Sunday | Weekly future bookings ingestion |
| `future-bookings-dedupe-weekly` | 12:13am | Monday | Dedupe future bookings |

---

## Year-Over-Year Jobs (Run Monday Early Morning)

These jobs pull data from the same week last year for comparison.

| Job | Time | Day | Purpose |
|---|---|---|---|
| `leads-yoy-ingest` | 1:00am | Monday | Pull leads from same week last year |
| `leads-yoy-dedupe` | 2:10am | Monday | Dedupe YoY leads |
| `collections-yoy-ingest` | 1:10am | Monday | Pull collections from same week last year |
| `collections-yoy-dedupe` | 2:20am | Monday | Dedupe YoY collections |
| `foreman-yoy-ingest` | 1:05am | Monday | Pull foreman from same week last year |
| `foreman-yoy-dedupe` | 2:15am | Monday | Dedupe YoY foreman |
| `wbr-yoy-andy-s-painting-sales` | 1:15am | Monday | WBR YoY for Andy's |
| `wbr-yoy-commercial-az-sales` | 1:25am | Monday | WBR YoY for Commercial AZ |
| `wbr-yoy-guaranteed-painting-sales` | 1:35am | Monday | WBR YoY for Guaranteed |
| `wbr-yoy-nevada-sales` | 1:45am | Monday | WBR YoY for Nevada |
| `wbr-yoy-phoenix-sales` | 1:55am | Monday | WBR YoY for Phoenix |
| `wbr-yoy-tucson-sales` | 2:05am | Monday | WBR YoY for Tucson |
| `wbr-yoy-dedupe` | 2:25am | Monday | Dedupe all WBR YoY data |

---

## Hourly Jobs

| Job | Schedule | Purpose |
|---|---|---|
| `ingest-ar` | Every hour at :20 | Accounts Receivable ingestion |

---

## Timeline (Daily Schedule)

```
9:00pm  - wbr-andys starts
9:02pm  - wbr-commercial-az starts
9:04pm  - wbr-guaranteed starts
9:06pm  - wbr-nevada starts
9:08pm  - wbr-phoenix starts
9:10pm  - wbr-tucson starts
10:15pm - wbr-dedupe (after all WBR jobs complete)

SUNDAY ONLY:
10:30pm - wbr-weekly-consolidated (verification)
11:50pm - leads-weekly-ingest
11:55pm - foreman-ingest-weekly
11:58pm - future-bookings-ingest-weekly
12:05am - leads-dedupe (Monday)
12:10am - foreman-dedupe-weekly (Monday)
12:13am - future-bookings-dedupe-weekly (Monday)

MONDAY ONLY (YoY):
1:00am  - leads-yoy-ingest
1:05am  - foreman-yoy-ingest
1:10am  - collections-yoy-ingest
1:15am  - wbr-yoy jobs start (6 jobs staggered)
2:10am  - YoY dedupe jobs start
```

---

## Data Freshness

**WBR Metrics:** Updated **every night** at ~9:10pm (after all 6 BU jobs complete)
**Leads:** Updated **weekly** (Sunday 11:50pm)
**Collections:** Updated **weekly** (Sunday 5:00pm)
**Foreman:** Updated **weekly** (Sunday 11:55pm)
**Future Bookings:** Updated **weekly** (Sunday 11:58pm)
**AR:** Updated **hourly**

---

## Expected Looker Dashboard Refresh

Your Looker dashboard reading from `st_kpi_mart.kpi_daily_regional_consolidated` will have:

- **WBR data:** Refreshed daily by 10:30pm with yesterday's metrics
- **Leads/Collections/Foreman:** Refreshed weekly on Monday morning

---

## Monitoring

Check Cloud Scheduler logs:
```bash
gcloud logging read "resource.type=cloud_scheduler_job" --limit 50 --format json
```

Check specific job:
```bash
gcloud scheduler jobs describe wbr-phoenix --location=us-central1
```

List all jobs:
```bash
gcloud scheduler jobs list --location=us-central1
```

---

## Notes

- WBR pulls **last 7 days** each night to ensure no data is missed
- Dedupe jobs remove overlapping data from the 7-day rolling window
- YoY jobs use `offset_days=365` to pull same week from last year
- All jobs have 10-minute timeout and retry on failure
