# Repository Cleanup Recommendations

**Analysis Date:** 2025-10-22
**Purpose:** Identify safe-to-delete files that are no longer needed for production

---

## 🗑️ SAFE TO DELETE

### Test Files (Development/Debug Only - 11 files)
These were used during development to test API connections and data transformations. No longer needed for production.

```bash
rm v2_ingestor/test_api_directly.js
rm v2_ingestor/test_auth.js
rm v2_ingestor/test_bq_insert.js
rm v2_ingestor/test_estimates_backfill.js
rm v2_ingestor/test_estimates_payroll.js
rm v2_ingestor/test_job_costing.js
rm v2_ingestor/test_job_costing_components.js
rm v2_ingestor/test_job_types.js
rm v2_ingestor/test_job_types_jpm.js
rm v2_ingestor/test_payroll_backfill_fixed.js
rm v2_ingestor/test_with_filters.js
```

**Why safe:** These are one-off test scripts. All functionality has been integrated into production code.

---

### Duplicate/Obsolete Backfill Scripts (3 files)
We now use `backfill_entity.js` as the universal backfill script. These older variants are redundant.

```bash
rm v2_ingestor/backfill_chunked.js       # Replaced by backfill_entity.js
rm v2_ingestor/backfill_smart.js         # Replaced by backfill_entity.js
rm v2_ingestor/backfill_windowed.js      # Replaced by backfill_entity.js
rm v2_ingestor/backfill_payroll_test.js  # Test file, functionality in production
```

**Why safe:** `backfill_entity.js` is the authoritative universal backfill script with all features.

---

### Log Files (1 file)
```bash
rm v2_ingestor/estimates_backfill.log
```

**Why safe:** Old backfill logs. Production logs should go to Cloud Logging, not local files.

---

### Duplicate KPI SQL Files (2 files)
We have multiple versions of KPI mart SQL. Keep only the latest production version.

```bash
rm v2_ingestor/st_mart_v2_kpis.sql      # Older version, replaced by create_kpi_mart.sql
rm v2_ingestor/bigquery_schemas.sql     # Ad-hoc schema definitions, not used
```

**Why safe:** `create_kpi_mart.sql` is the authoritative production version with all fixes (timezone, jobId, etc.)

---

### Duplicate/Interim Validation Files (2 files)
```bash
rm v2_ingestor/validate_kpis.sql        # Had variable naming bug, replaced by validate_kpis_fixed.sql
rm v2_ingestor/VALIDATION_SUMMARY.md    # Interim report, superseded by FINAL_VALIDATION_REPORT.md
```

**Why safe:** We have the fixed/final versions of these files.

---

### Monitoring Scripts (3 files - OPTIONAL)
These are convenience scripts for monitoring backfill progress. Can delete if you prefer Cloud Console monitoring.

```bash
rm v2_ingestor/monitor_backfill.sh
rm v2_ingestor/monitor_estimates.sh
rm v2_ingestor/check_backfill_progress.sh
```

**Why safe:** These were helper scripts during development. Production monitoring should use Cloud Logging/Monitoring.

---

## ✅ KEEP - Production Critical Files

### Core Application Files
- ✅ `index.js` - Main Cloud Run entrypoint
- ✅ `backfill_entity.js` - Universal backfill script (production ready)
- ✅ `package.json` / `package-lock.json` - Dependencies
- ✅ `.env` / `.env.example` - Configuration
- ✅ `Dockerfile` / `.dockerignore` - Cloud Run deployment

### Production SQL
- ✅ `create_kpi_mart.sql` - **AUTHORITATIVE** KPI mart with all fixes
- ✅ `validate_kpis_fixed.sql` - Fixed validation queries
- ✅ `validate_leads_fix.sql` - Leads validation with new logic
- ✅ `diagnostics_total_booked.sql` - Total Booked diagnostics

### Source Code (src/ directory)
- ✅ All files in `src/api/`, `src/bq/`, `src/ingestors/`
- ✅ Especially `src/ingestors/invoices.js` (has the critical jobId fix)

### Documentation (Keep for reference)
- ✅ `FINAL_VALIDATION_REPORT.md` - Production validation results
- ✅ `README.md` - Main documentation
- ✅ `DEPLOYMENT_GUIDE.md` - How to deploy to Cloud Run
- ✅ `BACKFILL_GUIDE.md` - How to run backfills

### Utility Scripts
- ✅ `setup.sh` - Environment setup
- ✅ `fetch_secrets.sh` - Secret Manager integration
- ✅ `run_full_backfill.sh` - Production backfill orchestration
- ✅ `check_backfill_status.sh` - Check entity sync status
- ✅ `check_entity_progress.sh` - Monitor progress
- ✅ `test_endpoints_simple.sh` - Quick API health check

---

## 📊 Cleanup Impact Summary

**Files to delete:** 24 files
**Disk space saved:** ~100 KB (minimal, mostly cleanup for clarity)
**Risk level:** **ZERO** (all are development/test files or duplicates)

---

## 🚀 Recommended Cleanup Commands

### Quick cleanup (all safe-to-delete files):
```bash
cd /Users/calebpena/Desktop/repos/st-kpi-ingestor/v2_ingestor

# Test files
rm test_*.js backfill_payroll_test.js

# Obsolete backfill variants
rm backfill_chunked.js backfill_smart.js backfill_windowed.js

# Logs
rm *.log

# Duplicate SQL
rm st_mart_v2_kpis.sql bigquery_schemas.sql validate_kpis.sql

# Interim docs
rm VALIDATION_SUMMARY.md

# Optional: monitoring scripts (if you use Cloud Console instead)
# rm monitor_*.sh check_backfill_progress.sh
```

### Create archive before deleting (if you want backups):
```bash
mkdir -p archive
mv test_*.js backfill_chunked.js backfill_smart.js backfill_windowed.js \
   backfill_payroll_test.js *.log st_mart_v2_kpis.sql bigquery_schemas.sql \
   validate_kpis.sql VALIDATION_SUMMARY.md archive/
```

---

## ⚠️ DO NOT DELETE

### Critical Configuration
- ❌ `.env` - Contains API keys and credentials
- ❌ `config/config.json` - BigQuery dataset configuration
- ❌ `service-account-key.json` - BigQuery authentication

### Node Modules
- ❌ `node_modules/` - Dependencies (managed by npm)
- ❌ `package-lock.json` - Dependency lock file

---

## 📝 Notes

1. **Before deleting:** Consider creating a `archive/` folder and moving files there first, in case you need to reference them later.

2. **Git safety:** Files listed here are either:
   - Not yet committed to git (can safely delete)
   - Test/development files (safe to remove from repo)

3. **After cleanup:** Run `git status` to see what's been removed, then commit the cleanup:
   ```bash
   git add -u
   git commit -m "Clean up test files and obsolete scripts"
   git push
   ```

4. **Monitoring:** The monitoring shell scripts (`monitor_*.sh`) are optional. Keep them if you find them useful for quick local checks, otherwise rely on Cloud Console.

---

**Recommendation:** Start with deleting test files first (lowest risk), then proceed to duplicates. Keep monitoring scripts until you're fully comfortable with Cloud Console.
