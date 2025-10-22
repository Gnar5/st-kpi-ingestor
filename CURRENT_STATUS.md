# ServiceTitan KPI Ingestor - Current Status

**Date:** 2025-10-22

## ✅ Completed

### Data Backfills (Historical 2020-2025)
- **Jobs:** 161,332 records ✅
- **Estimates:** 149,310 records ✅
- **Invoices:** 172,777 records ✅
- **Payments:** 55,459 records ✅
- **Payroll:** 536 records (incremental only - backfill failing) ⚠️

### Infrastructure
- Byte-based batching for large payloads (estimates, invoices) ✅
- Incremental sync working for daily updates ✅
- `createdOn` filtering for historical backfills ✅
- Business units reference data (`st_ref_v2.dim_business_units`) ✅
- Business unit rollups (`st_ref_v2.dim_bu_rollup`) ✅
- **Job types reference data (`st_ref_v2.dim_job_types`) - 46 types ✅**
- **Enriched jobs dimension table (`st_dim.dim_jobs`) ✅**

### Documentation
- KPI requirements mapped from ServiceTitan reports ✅
- All 10 KPIs documented with SQL logic ✅

## ✅ RESOLVED: Job Types Reference Data
**Status:** COMPLETED via JPM API endpoint

**Solution:** Found working endpoint at `jpm/v2/tenant/{tenant}/job-types`
- Successfully ingested 46 job types including:
  - 16 estimate types (ESTIMATE-RES-EXT, ESTIMATE-RES-INT, etc.)
  - 3 warranty types (Warranty, Touchup, Window/Solar-Touchup)
  - Production types (Carpentry, Painting, etc.)

**Results:**
- Table: `st_ref_v2.dim_job_types` (46 records)
- Enriched table: `st_dim.dim_jobs` (161,332 jobs with business unit and job type names)
- Can now filter jobs by name instead of ID for KPI calculations

## ❌ Remaining Blocking Issues

### Job Costing Data (CRITICAL FOR 4 KPIs)
**Problem:** Jobs table doesn't have cost/margin fields needed for:
- $ Produced (needs `jobs_subtotal` or invoice totals)
- G.P.M (needs `jobs_total_cost`, `gross_margin_pct`)
- Future Bookings (needs revenue from incomplete jobs)
- Warranty % (needs `jobs_total_cost` to calculate warranty cost percentage)

**API Investigation Results:**
- `accounting/v2/tenant/{tenant}/export/job-costing` → 404 error
- `accounting/v2/tenant/{tenant}/export/jobs` → 404 error
- `pricebook/v2/tenant/{tenant}/materials` → Works (667 materials with costs)
- But materials don't link to specific jobs

**Critical Questions:**
1. **How does your FOREMAN report get "Jobs Subtotal" and "Jobs Total Cost"?**
   - Is this calculated from invoices linked to jobs?
   - Is there a job costing module in ServiceTitan we need to access?
   - Can you export a FOREMAN report and share it so I can reverse-engineer the calculations?

2. **Should we calculate job revenue from invoices?**
   - We have 172,777 invoices with `jobId` foreign keys
   - Could aggregate invoice totals per job for "$ Produced"
   - But still need cost data for margins

## 🔄 Next Steps

### Immediate (Waiting on You)
1. **Clarify job costing data source** (BLOCKING 4 KPIs)
   - How do you get "Jobs Subtotal" and "Jobs Total Cost" in the FOREMAN report?
   - Can you share a sample FOREMAN report export?
   - Is there a Job Costing module/API we should be using?

### Ready to Build (Job Types Complete!)
1. ✅ **dim_jobs enriched table created** (161,332 jobs)
   - All jobs now have businessUnitName, jobTypeName, business unit rollups
   - Ready for KPI calculations

2. **Build initial KPI queries** (Can start now for KPIs that don't need costing)
   - Leads (needs job types ✅)
   - Total Booked (needs job types ✅)
   - # Estimates (needs job types ✅)
   - Success Rate (needs job types ✅)
   - $ Collected (needs payment-to-BU mapping)
   - Outstanding A/R (needs invoice balance validation)

## 📊 KPI Status

| KPI | Data Available | Status |
|-----|---------------|--------|
| Leads | ✅ Yes | **READY** - dim_jobs has all filters |
| Total Booked | ✅ Yes | **READY** - dim_jobs has all filters |
| $ Produced | ⚠️ Partial | **BLOCKED** - Need job costing/revenue data |
| G.P.M | ❌ No | **BLOCKED** - Need job costs and margins |
| $ Collected | ✅ Yes | **READY** - Can aggregate payments by BU |
| # Estimates | ✅ Yes | **READY** - dim_jobs has estimate types |
| Success Rate | ✅ Yes | **READY** - Can calculate from estimates |
| Future Bookings | ❌ No | **BLOCKED** - Need job costing data |
| Warranty % | ❌ No | **BLOCKED** - Need job costs |
| Outstanding A/R | ✅ Yes | **READY** - Invoices have balance field |

**Summary:**
- **6 KPIs READY** to build (don't require job costing)
- **4 KPIs BLOCKED** waiting on job costing data source

## 📁 Files Created This Session

### Ingestors
- `/src/ingestors/job_types.js` - Job types ingestor (waiting on API/data)

### Scripts
- `/backfill_entity.js` - Universal entity backfill script
- `/check_entity_progress.sh` - Progress checker for any entity
- `/test_job_types.js` - Job types test script

### Documentation
- `/docs/kpi_mapping.md` - Complete KPI → SQL mappings
- `/docs/next_steps_kpis.md` - Implementation plan

## 💡 Recommendations

1. **Quick Win:** Export job types CSV from ServiceTitan → upload to BigQuery
2. **Build dim_jobs** immediately after job types are available
3. **Test one KPI** (e.g., # Estimates) to validate the approach
4. **Investigate job costing** to unblock $ Produced, GPM, Future Bookings
5. **Skip payroll backfill** for now (incremental sync works, backfill has data issues)
