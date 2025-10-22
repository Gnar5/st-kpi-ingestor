# Next Steps: Building KPI Automation

## Current Status

### ✅ Completed
- Backfilled historical data (2020-2025):
  - Jobs: 161,332 records
  - Estimates: 149,310 records
  - Invoices: 172,777 records
  - Payments: 55,459 records
- Byte-based batching implemented for large payloads
- Incremental sync working for daily updates
- KPI requirements documented from ServiceTitan reports

### ❌ Blocking Issues

**CRITICAL:** Jobs table has `businessUnitId` and `jobTypeId` (integers), but KPI logic requires **names**:
- Need: "Andy's Painting Sales", "Commercial-AZ-Sales", etc.
- Have: businessUnitId=123, businessUnitId=456, etc.

**Solution:** Ingest reference data from ServiceTitan API

## Required Reference Data

### 1. Business Units
**API Endpoint:** `settings/v2/tenant/{tenant}/business-units`

**Fields Needed:**
- `id` (INTEGER)
- `name` (STRING) - e.g., "Andy's Painting Sales"
- `active` (BOOLEAN)

**Why Critical:** Every KPI calculation filters by business unit name. Without this mapping, we can't:
- Separate sales vs production units
- Filter by specific branches
- Combine Phoenix + "Z-DO NOT USE - West" units

### 2. Job Types
**API Endpoint:** `jpm/v2/tenant/{tenant}/job-types`

**Fields Needed:**
- `id` (INTEGER)
- `name` (STRING) - e.g., "ESTIMATE-RES-EXT", "Warranty", etc.
- `active` (BOOLEAN)

**Why Critical:** KPI calculations have complex job type filters:
- Estimate types: "ESTIMATE-RES-EXT", "Estimate", "Cabinets", etc. (20+ types)
- Excluded types: "PM Inspection", "Safety Inspection", "Window/Solar Washing"
- Warranty types: "Warranty", "Touchup"

### 3. Campaign (Optional but Helpful)
**API Endpoint:** `marketing/v2/tenant/{tenant}/campaigns`

**Why:** Jobs have `campaignId`, useful for marketing attribution

## Implementation Plan

### Phase 1: Reference Data Ingestors (Priority 1)
1. Create `business_units.js` ingestor
2. Create `job_types.js` ingestor
3. Add to daily sync schedule
4. Backfill current reference data

### Phase 2: Dimension Layer (Priority 2)
Create enriched dimension tables that join reference data:

```sql
CREATE OR REPLACE TABLE `st_dim.dim_jobs` AS
SELECT
  j.*,
  bu.name as businessUnitName,
  jt.name as jobTypeName,
  c.name as campaignName
FROM `st_raw_v2.raw_jobs` j
LEFT JOIN `st_raw_v2.raw_business_units` bu ON j.businessUnitId = bu.id
LEFT JOIN `st_raw_v2.raw_job_types` jt ON j.jobTypeId = jt.id
LEFT JOIN `st_raw_v2.raw_campaigns` c ON j.campaignId = c.id
```

### Phase 3: KPI Mart (Priority 3)
Create daily KPI mart with all 10 metrics:

```sql
CREATE OR REPLACE TABLE `st_mart.daily_kpis` AS
WITH ...
SELECT
  event_date,
  business_unit,
  lead_count,
  total_booked,
  dollars_produced,
  gpm_percent,
  dollars_collected,
  num_estimates,
  close_rate_percent,
  future_bookings,
  warranty_percent,
  outstanding_ar
FROM kpi_calculations
```

### Phase 4: Business Logic (Priority 4)
1. Create business unit mapping table for combinations:
   - Phoenix-Sales + Z-DO NOT USE - West - Sales → Phoenix-Sales
   - Phoenix-Production + Z-DO NOT USE - West- Production → Phoenix-Production

2. Create job type filter lists:
   - Estimate types (20+ values)
   - Excluded types (3 values)
   - Warranty types (2 values)

### Phase 5: Validation (Priority 5)
1. Run KPI queries for a known date range
2. Compare against actual ServiceTitan report output
3. Iterate until numbers match exactly

## Missing Data Investigation

Based on your KPI requirements, we need to check if these fields exist:

### Jobs Table (Need to verify):
- `jobs_subtotal` - Used for $ Produced, Future Bookings
- `jobs_total_cost` - Used for GPM, Warranty %
- `gross_margin_pct` - Used for GPM calculation
- `startDate` - Used for $ Produced date filtering

**Action:** Check if jobs API returns cost/margin data, or if we need to calculate from invoices/expenses

### Invoices Table (Need to verify):
- Does `balance > 0` correctly identify outstanding A/R?
- Is there a `netAmount` field for A/R calculation?

### Payments Table (Need to verify):
- How to associate payments with business units? (via invoice → job → businessUnit?)

## Immediate Next Steps

1. **Check ServiceTitan API docs** for business-units and job-types endpoints
2. **Create reference data ingestors** (business_units.js, job_types.js)
3. **Sample the jobs data** to see if cost/margin fields exist
4. **Build dim_jobs table** with business unit and job type names joined
5. **Test one KPI calculation** (e.g., Leads) to validate the approach

## Questions to Resolve

1. Do jobs records include cost/margin data, or do we calculate from other tables?
2. How are payments linked to business units? Through invoices?
3. Should we combine Phoenix + "Z-DO NOT USE" in the raw data or in the mart layer?
4. What date field determines "Job Start Date" for the Foreman Job Cost report?
