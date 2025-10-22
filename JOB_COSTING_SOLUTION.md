# Job Costing Implementation Plan

## Problem Statement
The FOREMAN report shows "Jobs Total Costs" which is calculated from multiple data sources. We need to ingest and aggregate these sources to calculate job costs for KPI metrics.

## FOREMAN Report Formula (Confirmed)

```
Jobs Subtotal = Total revenue from invoices linked to job
Labor Pay = SUM(payroll.amount WHERE jobId = job.id AND activity IN ('Working', 'Direct Adjustment'))
Materials + Equip. + PO/Bill Costs = SUM(purchase-orders.total WHERE jobId = job.id)
Returns = SUM(returns.total WHERE jobId = job.id)

Jobs Total Costs = Labor Pay + (PO Costs - Returns)
Jobs Gross Margin % = (Jobs Subtotal - Jobs Total Costs) / Jobs Subtotal * 100
```

**Key Insight:** The existing `raw_payroll` table already includes BOTH labor pay and payroll adjustments via the `activity` field:
- "Working" = Regular labor hours
- "Direct Adjustment" = Payroll adjustments
- "Driving" = Drive time

No need for separate gross-pay-items or payroll-adjustments endpoints!

## API Endpoint Test Results

### ✅ Purchase Orders (Material/Equipment Costs)
- **Endpoint:** `inventory/v2/tenant/{tenant}/purchase-orders`
- **Status:** Working ✅
- **Test Results:** 495 POs in week of Oct 1-8, 2024
- **Key Fields:**
  - `id` - PO identifier
  - `jobId` - Links to job
  - `invoiceId` - Links to invoice
  - `total` - Total PO cost (including tax)
  - `tax` - Tax amount
  - `status` - "Exported", "Pending", etc.
  - `items[]` - Line items with material details

**Sample Record:**
```json
{
  "id": 149700158,
  "jobId": 147300995,
  "invoiceId": 147300999,
  "status": "Exported",
  "total": 9.14,
  "tax": 0.66,
  "items": 1
}
```

### ✅ Returns (Material Returns to Credit)
- **Endpoint:** `inventory/v2/tenant/{tenant}/returns`
- **Status:** Working ✅
- **Test Results:** 5 returns in week of Oct 1-8, 2024
- **Key Fields:**
  - `id` - Return identifier
  - `jobId` - Links to job
  - `total` - Total return value (likely)
  - `status` - "CreditReceived", etc.
  - `items[]` - Returned items

**Sample Record:**
```json
{
  "id": 149726073,
  "jobId": 149291025,
  "status": "CreditReceived",
  "items": 2
}
```

### ✅ Payroll (Labor Costs - ALREADY HAVE!)
- **Table:** `st_raw_v2.raw_payroll` (already ingested!)
- **Status:** Working ✅ - No new API calls needed
- **Record Count:** 536 records (but needs historical backfill)
- **Key Fields:**
  - `payrollId` - Payroll item identifier
  - `jobId` - Links to job
  - `employeeId` - Technician who worked
  - `amount` - Labor cost for this item
  - `activity` - Type: "Working", "Direct Adjustment", "Driving"
  - `paidDurationHours` - Hours worked (for "Working" activity)
  - `date` - When paid

**Activity Breakdown (current data):**
- Working: 302 records, $34K (regular labor)
- Direct Adjustment: 221 records, $201K (payroll adjustments)
- Driving: 13 records, $110 (drive time)

**Note:** We DON'T need to call gross-pay-items or payroll-adjustments endpoints - the existing payroll ingestor already has everything!

## Implementation Plan

### Phase 1: Create Missing Ingestors

Need to create only 2 new ingestors (purchase orders and returns):

1. **Purchase Orders Ingestor**
   - Path: `v2_ingestor/src/ingestors/purchase_orders.js`
   - Table: `st_raw_v2.raw_purchase_orders`
   - Primary key: `id`
   - Partition: `createdOn`
   - Clusters: `jobId`, `status`
   - Date filters: `createdOnOrAfter`, `createdBefore`
   - Endpoint: `inventory/v2/tenant/{tenant}/purchase-orders`

2. **Returns Ingestor**
   - Path: `v2_ingestor/src/ingestors/returns.js`
   - Table: `st_raw_v2.raw_returns`
   - Primary key: `id`
   - Partition: `createdOn`
   - Clusters: `jobId`, `status`
   - Date filters: `createdOnOrAfter`, `createdBefore`
   - Endpoint: `inventory/v2/tenant/{tenant}/returns`

3. ~~Gross Pay Items Ingestor~~ ✅ **Already have!** Using existing `raw_payroll` table
4. ~~Payroll Adjustments Ingestor~~ ✅ **Already have!** Included in `raw_payroll` via "Direct Adjustment" activity

### Phase 2: Backfill Historical Data

Run backfills for 2020-2025 for each new entity:
```bash
node backfill_entity.js purchase_orders 2020
node backfill_entity.js returns 2020
# Payroll backfill already attempted (needs fixing - current blocker)
```

### Phase 3: Create Job Costing Mart Table

Create `st_kpi.fact_job_costs` with aggregated cost data per job:

```sql
CREATE OR REPLACE TABLE `kpi-auto-471020.st_kpi.fact_job_costs` AS
SELECT
  j.id as jobId,
  j.jobNumber,
  j.completedOn,

  -- Revenue (from invoices)
  COALESCE(SUM(inv.total), 0) as jobs_subtotal,

  -- Material Costs (PO - Returns)
  COALESCE(SUM(po.total), 0) as material_costs,
  COALESCE(SUM(ret.total), 0) as material_returns,
  COALESCE(SUM(po.total), 0) - COALESCE(SUM(ret.total), 0) as net_material_costs,

  -- Labor Costs (from payroll - includes Working, Direct Adjustment, Driving)
  COALESCE(SUM(pr.amount), 0) as total_labor_costs,

  -- Total Job Costs
  (COALESCE(SUM(po.total), 0) - COALESCE(SUM(ret.total), 0)) +
  COALESCE(SUM(pr.amount), 0) as jobs_total_costs,

  -- Gross Margin
  COALESCE(SUM(inv.total), 0) -
  ((COALESCE(SUM(po.total), 0) - COALESCE(SUM(ret.total), 0)) +
   COALESCE(SUM(pr.amount), 0)) as gross_margin_dollars,

  CASE
    WHEN COALESCE(SUM(inv.total), 0) > 0 THEN
      ((COALESCE(SUM(inv.total), 0) -
        ((COALESCE(SUM(po.total), 0) - COALESCE(SUM(ret.total), 0)) +
         COALESCE(SUM(pr.amount), 0))) /
       COALESCE(SUM(inv.total), 0)) * 100
    ELSE 0
  END as jobs_gross_margin_pct

FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j

-- Invoices for revenue
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` inv
  ON j.id = inv.jobId

-- Purchase Orders for material costs
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_purchase_orders` po
  ON j.id = po.jobId

-- Returns for material credits
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_returns` ret
  ON j.id = ret.jobId

-- Payroll for all labor costs (Working + Direct Adjustment + Driving)
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_payroll` pr
  ON j.id = pr.jobId

GROUP BY j.id, j.jobNumber, j.completedOn
```

### Phase 4: Update KPI Queries

Now that we have job costs, we can calculate the 4 blocked KPIs:

1. **$ Produced** - Use `jobs_subtotal` from fact_job_costs
2. **G.P.M** - Use `jobs_gross_margin_pct` from fact_job_costs
3. **Future Bookings** - Use `jobs_subtotal` where job not completed
4. **Warranty %** - Calculate warranty costs / total costs

## Next Steps

1. ✅ Confirm gross-pay-items and payroll-adjustments endpoints work
2. Create 3-4 new ingestors for job costing components
3. Run historical backfills for each component
4. Create fact_job_costs aggregation table
5. Build the 4 blocked KPI queries
6. Validate calculations against actual FOREMAN reports

## Questions for User

1. Do you have access to a recent FOREMAN report we can use to validate our calculations?
2. Should we prioritize getting these job costing ingestors working before building the 6 non-costing KPIs?
3. Are there any other cost components we're missing from the formula?
