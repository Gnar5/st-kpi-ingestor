# ServiceTitan V2 Ingestor - Scheduler Optimization Plan

## Current Scheduler Status

### ✓ Existing Schedulers (7 total)
| Entity | Schedule | Time (AZ) | Purpose |
|--------|----------|-----------|---------|
| jobs | `0 2 * * *` | 2:00 AM | Core entity - job records |
| appointments | `10 2 * * *` | 2:10 AM | Job scheduling/dates |
| invoices | `15 2 * * *` | 2:15 AM | Revenue data |
| estimates | `30 2 * * *` | 2:30 AM | Estimate data |
| payments | `45 2 * * *` | 2:45 AM | Payment data |
| payroll | `0 3 * * *` | 3:00 AM | Labor costs |
| customers | `15 3 * * *` | 3:15 AM | Customer data |

### ⚠️ Missing Critical Schedulers for Job Costing
| Entity | Status | Impact on GPM |
|--------|--------|---------------|
| **purchase_orders** | ❌ Missing | HIGH - Materials costs (-$7K gap) |
| **payroll_adjustments** | ❌ Missing | MEDIUM - Labor adjustments |
| **returns** | ❌ Missing | LOW - Material credits (-$524 in TSV) |
| **inventory_bills** | ❌ Missing | MEDIUM - Material costs |

### Reference Data (Less Critical)
| Entity | Status | Update Frequency Needed |
|--------|--------|-------------------------|
| business_units | ❌ Missing | Weekly (rarely changes) |
| technicians | ❌ Missing | Weekly (employee changes) |
| activity_codes | ❌ Missing | Monthly (rarely changes) |
| locations | ❌ Missing | Weekly (new locations rare) |
| campaigns | ❌ Missing | Weekly (marketing data) |

---

## Recommended Scheduler Strategy

### Timing Philosophy
**Key Principle:** Run ingestors LATE in the night (4-5 AM) to capture stable daily snapshots after:
- All technicians have clocked out (typically by 8-9 PM)
- Office staff has completed invoice/PO entry (typically by 5-6 PM)
- Automated ServiceTitan processes have run (midnight-2 AM)
- Any late-night adjustments are complete

**Current Problem:** Running at 2-3 AM may miss:
- Late invoice entries
- End-of-day job status updates
- Appointment reschedules made overnight
- PO/bill finalizations

### Proposed Schedule - Phased Approach

#### Phase 1: Core Entities (4:00-4:30 AM)
These form the foundation - must complete first
```bash
4:00 AM - jobs              # Base entity
4:05 AM - appointments      # Dates/scheduling
4:10 AM - customers         # Customer info
4:15 AM - locations         # Location data
```

#### Phase 2: Financial Entities (4:30-5:00 AM)
Revenue and cost transactions - depend on jobs
```bash
4:30 AM - invoices          # Revenue
4:35 AM - estimates         # Estimate data
4:40 AM - payments          # Payment tracking
4:45 AM - purchase_orders   # ⭐ ADD - Materials costs
4:50 AM - inventory_bills   # ⭐ ADD - Materials costs
4:55 AM - returns           # ⭐ ADD - Material credits
```

#### Phase 3: Labor Costs (5:00-5:15 AM)
Labor data - depends on jobs completing
```bash
5:00 AM - payroll           # Base labor
5:05 AM - payroll_adjustments # ⭐ ADD - Labor adjustments
```

#### Phase 4: Reference Data (Weekly - Sundays at 6:00 AM)
Dimension tables that change infrequently
```bash
6:00 AM (Sun) - business_units
6:05 AM (Sun) - technicians
6:10 AM (Sun) - activity_codes
6:15 AM (Sun) - campaigns
```

---

## Implementation Commands

### 1. Update Existing Schedulers to New Times

```bash
# Update jobs to 4:00 AM
gcloud scheduler jobs update http v2-sync-jobs-daily \
  --location=us-central1 \
  --schedule="0 4 * * *" \
  --description="Daily incremental sync of ServiceTitan jobs (4:00 AM AZ time)"

# Update appointments to 4:05 AM
gcloud scheduler jobs update http v2-sync-appointments-daily \
  --location=us-central1 \
  --schedule="5 4 * * *" \
  --description="Daily incremental sync of ServiceTitan appointments (4:05 AM AZ time)"

# Update customers to 4:10 AM
gcloud scheduler jobs update http v2-sync-customers-daily \
  --location=us-central1 \
  --schedule="10 4 * * *" \
  --description="Daily incremental sync of ServiceTitan customers (4:10 AM AZ time)"

# Update invoices to 4:30 AM
gcloud scheduler jobs update http v2-sync-invoices-daily \
  --location=us-central1 \
  --schedule="30 4 * * *" \
  --description="Daily incremental sync of ServiceTitan invoices (4:30 AM AZ time)"

# Update estimates to 4:35 AM
gcloud scheduler jobs update http v2-sync-estimates-daily \
  --location=us-central1 \
  --schedule="35 4 * * *" \
  --description="Daily incremental sync of ServiceTitan estimates (4:35 AM AZ time)"

# Update payments to 4:40 AM
gcloud scheduler jobs update http v2-sync-payments-daily \
  --location=us-central1 \
  --schedule="40 4 * * *" \
  --description="Daily incremental sync of ServiceTitan payments (4:40 AM AZ time)"

# Update payroll to 5:00 AM
gcloud scheduler jobs update http v2-sync-payroll-daily \
  --location=us-central1 \
  --schedule="0 5 * * *" \
  --description="Daily incremental sync of ServiceTitan payroll (5:00 AM AZ time)"
```

### 2. Create Missing Critical Schedulers

```bash
# ⭐ Purchase Orders (4:45 AM) - CRITICAL for materials
gcloud scheduler jobs create http v2-sync-purchase-orders-daily \
  --location=us-central1 \
  --schedule="45 4 * * *" \
  --time-zone="America/Phoenix" \
  --uri="https://st-v2-ingestor-999875365235.us-central1.run.app/ingest/purchase_orders?mode=incremental" \
  --http-method=GET \
  --description="Daily incremental sync of ServiceTitan purchase orders (4:45 AM AZ time)"

# ⭐ Inventory Bills (4:50 AM) - CRITICAL for materials
gcloud scheduler jobs create http v2-sync-inventory-bills-daily \
  --location=us-central1 \
  --schedule="50 4 * * *" \
  --time-zone="America/Phoenix" \
  --uri="https://st-v2-ingestor-999875365235.us-central1.run.app/ingest/inventory_bills?mode=incremental" \
  --http-method=GET \
  --description="Daily incremental sync of ServiceTitan inventory bills (4:50 AM AZ time)"

# ⭐ Returns (4:55 AM) - For material credits
gcloud scheduler jobs create http v2-sync-returns-daily \
  --location=us-central1 \
  --schedule="55 4 * * *" \
  --time-zone="America/Phoenix" \
  --uri="https://st-v2-ingestor-999875365235.us-central1.run.app/ingest/returns?mode=incremental" \
  --http-method=GET \
  --description="Daily incremental sync of ServiceTitan returns (4:55 AM AZ time)"

# ⭐ Payroll Adjustments (5:05 AM) - For labor adjustments
gcloud scheduler jobs create http v2-sync-payroll-adjustments-daily \
  --location=us-central1 \
  --schedule="5 5 * * *" \
  --time-zone="America/Phoenix" \
  --uri="https://st-v2-ingestor-999875365235.us-central1.run.app/ingest/payroll_adjustments?mode=incremental" \
  --http-method=GET \
  --description="Daily incremental sync of ServiceTitan payroll adjustments (5:05 AM AZ time)"

# Locations (4:15 AM) - For job location data
gcloud scheduler jobs create http v2-sync-locations-daily \
  --location=us-central1 \
  --schedule="15 4 * * *" \
  --time-zone="America/Phoenix" \
  --uri="https://st-v2-ingestor-999875365235.us-central1.run.app/ingest/locations?mode=incremental" \
  --http-method=GET \
  --description="Daily incremental sync of ServiceTitan locations (4:15 AM AZ time)"
```

### 3. Create Weekly Reference Data Schedulers

```bash
# Business Units (Sundays 6:00 AM)
gcloud scheduler jobs create http v2-sync-business-units-weekly \
  --location=us-central1 \
  --schedule="0 6 * * 0" \
  --time-zone="America/Phoenix" \
  --uri="https://st-v2-ingestor-999875365235.us-central1.run.app/ingest/ref/business_units?mode=full" \
  --http-method=GET \
  --description="Weekly full sync of ServiceTitan business units (Sundays 6:00 AM AZ time)"

# Technicians (Sundays 6:05 AM)
gcloud scheduler jobs create http v2-sync-technicians-weekly \
  --location=us-central1 \
  --schedule="5 6 * * 0" \
  --time-zone="America/Phoenix" \
  --uri="https://st-v2-ingestor-999875365235.us-central1.run.app/ingest/ref/technicians?mode=full" \
  --http-method=GET \
  --description="Weekly full sync of ServiceTitan technicians (Sundays 6:05 AM AZ time)"

# Activity Codes (Sundays 6:10 AM)
gcloud scheduler jobs create http v2-sync-activity-codes-weekly \
  --location=us-central1 \
  --schedule="10 6 * * 0" \
  --time-zone="America/Phoenix" \
  --uri="https://st-v2-ingestor-999875365235.us-central1.run.app/ingest/ref/activity_codes?mode=full" \
  --http-method=GET \
  --description="Weekly full sync of ServiceTitan activity codes (Sundays 6:10 AM AZ time)"
```

---

## Rollout Plan

### Option A: Immediate (Aggressive)
**Timeline:** Tonight (Oct 30)
**Risk:** Higher - changing 7 schedulers + adding 5 new ones
**Benefit:** Immediate improvement to data quality

1. Update all 7 existing schedulers to new times (4:00-5:00 AM range)
2. Create 5 new critical schedulers (POs, bills, returns, payroll adjustments, locations)
3. Monitor tomorrow morning (Oct 31) for any failures

### Option B: Phased (Conservative) - RECOMMENDED
**Timeline:** Over 3 days
**Risk:** Lower - gradual rollout with validation
**Benefit:** Safer, can roll back if issues arise

**Night 1 (Oct 30):** Add missing critical schedulers only
- Create purchase_orders scheduler (4:45 AM)
- Create inventory_bills scheduler (4:50 AM)
- Create returns scheduler (4:55 AM)
- Create payroll_adjustments scheduler (5:05 AM)
- Keep existing schedulers at current times (2-3 AM)

**Night 2 (Oct 31):** Shift existing schedulers if Night 1 succeeds
- Update all 7 existing schedulers to new times (4:00-5:00 AM)
- Monitor for conflicts or failures

**Night 3 (Nov 1):** Add reference data schedulers
- Create weekly reference schedulers (business_units, technicians, activity_codes)

---

## Expected Impact on GPM Reconciliation

### After Adding Missing Schedulers

| Gap | Current | Expected After Fix | Reason |
|-----|---------|-------------------|--------|
| Materials | -$7,024 (-6.52%) | < -$1,000 (-1%) | POs and inventory bills will capture missing material costs |
| Labor | -$3,182 (-1.85%) | < -$500 (-0.3%) | Payroll adjustments will capture missing labor adjustments |
| Revenue | +$9,734 (+2.05%) | +$5,000 (+1%) | Later sync time captures more invoices |

### After Time Shift to 4-5 AM

**Additional Benefits:**
- More stable job status (fewer in-progress jobs)
- Complete appointment schedules (reschedules finalized)
- Finalized invoices (no late edits)
- Complete PO/bill entries (end-of-day processing done)

**Expected Result:**
- Job count stays at 244 ✓
- Revenue gap: < 1%
- Labor gap: < 0.5%
- Materials gap: < 1%
- Overall GPM within 1-2% of ServiceTitan reports

---

## Monitoring & Validation

### Daily Checks (After Implementation)
```bash
# Check all schedulers ran successfully
gcloud scheduler jobs list --location=us-central1 \
  --format="table(name.basename(),schedule,state,lastAttemptTime,httpTarget.uri)" \
  | grep v2-sync

# Check data freshness
bq query --use_legacy_sql=false "
SELECT
  'raw_jobs' as table_name,
  MAX(updated_on) as last_updated,
  COUNT(*) as total_records
FROM \`kpi-auto-471020.st_raw_v2.raw_jobs\`
UNION ALL
SELECT 'raw_appointments', MAX(updated_on), COUNT(*) FROM \`kpi-auto-471020.st_raw_v2.raw_appointments\`
UNION ALL
SELECT 'raw_purchase_orders', MAX(updated_on), COUNT(*) FROM \`kpi-auto-471020.st_raw_v2.raw_purchase_orders\`
UNION ALL
SELECT 'raw_inventory_bills', MAX(updated_on), COUNT(*) FROM \`kpi-auto-471020.st_raw_v2.raw_inventory_bills\`
"
```

### Weekly Reconciliation
- Run same week comparison (10/20-10/26) after schedulers stabilize
- Compare totals to validate improvement
- Document remaining gaps

---

## Summary Recommendation

**Execute Option B (Phased Rollout) starting tonight:**

1. ✅ **Tonight (Oct 30):** Create 4 missing critical schedulers
   - This will immediately improve materials and labor accuracy
   - Low risk - just adding new schedulers, not changing existing ones

2. ✅ **Tomorrow night (Oct 31):** If successful, shift all schedulers to 4-5 AM window
   - This will capture more stable/complete data
   - Monitor for any timing conflicts

3. ✅ **Weekend (Nov 2-3):** Add weekly reference schedulers
   - These are less critical but complete the architecture

**Expected Outcome:** GPM reconciliation within 1-2% by next week, with all critical data pipelines in place.
