# GPM Comparison - After Material Cost Fix
**Date:** 2025-10-23
**Status:** Material costs now populated, but GPM still showing variance

---

## Results After Fix

### Comparison Table

| Region | ST GPM % | BQ GPM % (Before) | BQ GPM % (After) | Change | New Variance |
|--------|----------|-------------------|------------------|--------|--------------|
| Nevada | 24.04% | 43.77% | **13.66%** | -30.11% | -10.38% |
| Phoenix | 50.83% | 57.69% | **40.51%** | -17.18% | -10.32% |
| Tucson | 48.00% | 56.11% | **39.40%** | -16.71% | -8.60% |
| Andy's | 47.83% | 48.53% | **29.88%** | -18.65% | -17.95% |
| Commercial | 46.98% | 59.61% | **39.25%** | -20.36% | -7.73% |
| Guaranteed | 45.84% | 36.59% | **30.86%** | -5.73% | -14.98% |

### Observations

**BEFORE Fix:** All regions showing GPM TOO HIGH (no material costs)
**AFTER Fix:** All regions showing GPM TOO LOW (material costs now included)

This suggests we've overcorrected - possibly including material costs that ST doesn't count.

---

## Nevada Detailed Analysis

### ServiceTitan FOREMAN Report (Expected)
- Revenue: $23,975.00
- Total Costs: $18,210.83
- GPM: 24.04%

### BigQuery (Actual After Fix)
- Revenue: $23,975.00 ✅ Match
- Labor: $13,480.93
- Materials: $7,218.43
- **Total Costs: $20,699.36** ❌ TOO HIGH by $2,489
- GPM: 13.66% ❌ TOO LOW by 10.38%

### Cost Breakdown Comparison

ST expects total costs of $18,210.83 but BQ shows $20,699.36.

**Possible causes:**
1. Including materials from 'Exported' POs that haven't been received yet
2. Including materials from jobs that shouldn't be counted
3. Labor burden rate incorrect (30% assumed vs actual)
4. Missing returns/credits

---

## Hypothesis: PO Status Filter Too Broad

Current filter: `status IN ('Exported', 'Received')`

**'Exported'** may include:
- POs sent to vendors but not yet received
- POs that won't be used on the job
- Estimated costs rather than actual costs

**Recommendation:** Try filtering only for `status = 'Received'`

---

## Next Steps

### Test 1: Use Only 'Received' POs
```sql
-- Update line 54 in create_job_costing_table.sql
AND status = 'Received'  -- Only received materials
```

### Test 2: Check for Missing Returns
```sql
SELECT
  COUNT(*) as return_count,
  SUM(total) as total_credits
FROM `st_raw_v2.raw_returns`
WHERE DATE(createdOn) >= '2025-08-18'
  AND jobId IN (
    SELECT job_id FROM `st_mart_v2.job_costing`
    WHERE businessUnitNormalized = 'Nevada-Production'
      AND DATE(job_start_date) BETWEEN '2025-08-18' AND '2025-08-24'
  )
```

### Test 3: Verify Labor Burden Rate
The SQL uses a 30% burden rate. ST may use a different rate or actual burden costs.

```sql
-- Check if there's a burden rate field in payroll
SELECT *
FROM `st_raw_v2.raw_payroll`
LIMIT 5
```

---

## Material Cost Deep Dive - Nevada

Looking at individual jobs:

| Job | Revenue | Labor | Materials (BQ) | Materials (ST Est) | Total Cost (BQ) | Total Cost (ST) |
|-----|---------|-------|----------------|--------------------|-----------------|-----------------|
| 365700738 | $7,600 | $3,626 | $1,746 | ~$1,400? | $5,372 | ~$5,000? |
| 367707926 | $5,280 | $2,438 | $1,542 | ~$1,200? | $3,979 | ~$3,600? |
| 385872228 | $4,560 | $1,690 | $1,481 | ~$1,000? | $3,171 | ~$2,700? |

**Pattern:** BQ material costs appear 15-20% higher than ST values

This supports the hypothesis that we're including 'Exported' POs that haven't been delivered/used yet.

---

## Recommendation

**Priority 1:** Change purchase order filter to `status = 'Received'` only
**Priority 2:** Verify labor burden rate matches ST methodology
**Priority 3:** Check if returns table has data

**Expected Outcome:** GPM should increase from 13.66% toward 24.04% target
