# CRITICAL FINDINGS: WBR Report Logic Discrepancy

## Date: October 30, 2025
## Validation Week: 10/20-10/26

---

## Summary

We successfully implemented the WBR (Weekly Business Review) logic using **job completion date** as specified in Service Titan's "Daily WBR C/R" report. However, there is a fundamental discrepancy between the implementation and the user's expected baseline numbers.

---

## The Discrepancy

### Current Implementation Results (using job completion date):
- **Completed Jobs**: 187 jobs
- **Closed Jobs**: 45 jobs (have sold estimates)
- **Close Rate**: 24.06%
- **Total Booked**: $179,541.81

### User's Expected Baseline:
- **Completed Jobs**: 190 jobs
- **Closed Jobs**: ~76 (implied by 40.34% close rate)
- **Close Rate**: 40.34%
- **Total Booked**: $434,600

### Variance:
- Jobs Count: -1.6% (187 vs 190) ✅ Within tolerance
- Closed Jobs: -40.8% (45 vs 76) ❌ Major discrepancy
- Close Rate: -16.28 pts (24.06% vs 40.34%) ❌ Major discrepancy
- Total Booked: -58.7% ($179K vs $435K) ❌ Major discrepancy

---

## Root Cause Analysis

### Finding #1: Date Logic Is Correct But Baseline May Be Wrong

Our implementation follows ServiceTitan's official "Daily WBR C/R" report logic:
1. Filter to 19 specific WBR job types ✅
2. Filter to completed jobs only ✅
3. Filter to Sales business units ✅
4. **Use JOB COMPLETION DATE as primary date filter** ✅

The 22 "missing" sold estimates investigation revealed:
- 67 estimates have soldOn dates in 10/20-10/26
- But only 45 of those belong to jobs completed in 10/20-10/26
- The other 22 estimates belong to jobs completed OUTSIDE the date range

**Example from data:**
- Job completed on Oct 15 (before validation week)
- Estimate sold on Oct 22 (during validation week)
- ❌ NOT counted in WBR report (job completion was Oct 15)
- ✅ Would be counted if using "estimate sold date" filter

### Finding #2: Two Different Metrics

There are TWO different ways to calculate "Total Booked":

**Method A: By Job Completion Date** (what we implemented)
- "Show me revenue from jobs that completed this week"
- Jobs completed 10/20-10/26: 187 jobs
- Of those, 45 had sold estimates (at ANY date)
- Total booked: $179,541.81

**Method B: By Estimate Sold Date** (what the baseline appears to be)
- "Show me revenue from estimates sold this week"
- Estimates sold 10/20-10/26: 67 estimates
- Those estimates belong to jobs completed in various weeks
- Total booked: $434,600 (expected)

### Finding #3: The User's Baseline May Be From A Different Report

The user provided baseline numbers that suggest they pulled from a report using **estimate sold date**, not job completion date.

Possible sources:
1. ServiceTitan's "Sold Estimates" report (uses sold date)
2. ServiceTitan's "Revenue" or "Sales" report (uses sold date)
3. A custom report with sold date filter

But the official "Daily WBR C/R" report name specifically says "Close Rate", which requires:
- Opportunities (completed jobs)
- Closed opportunities (completed jobs with sold estimates)
- Close Rate = Closed / Total Opportunities

This can ONLY be calculated using job completion date as the primary filter.

---

## Data Validation

### Jobs in WBR View (10/20-10/26 completion):
```sql
SELECT COUNT(*) FROM st_stage.wbr_jobs
WHERE completion_date BETWEEN '2025-10-20' AND '2025-10-26'
-- Result: 187 jobs
```

### Sold Estimates by Sold Date (10/20-10/26 sold):
```sql
SELECT COUNT(*) FROM raw_estimates
WHERE DATE(soldOn) BETWEEN '2025-10-20' AND '2025-10-26'
-- Result: 67 estimates
```

### WBR Jobs with Sold Estimates:
```sql
SELECT COUNT(*) FROM st_stage.wbr_jobs
WHERE completion_date BETWEEN '2025-10-20' AND '2025-10-26'
  AND sold_estimates > 0
-- Result: 45 jobs (24.06% close rate)
```

### Missing 22 Estimates Breakdown:
These 22 estimates were sold in 10/20-10/26 but belong to jobs completed OUTSIDE that range:
- 5 jobs completed Oct 15-17 (just before validation week)
- 15 jobs completed Sept-Oct (earlier)
- 1 job completed Nov 2024 (very old job, estimate finally sold a year later!)
- 1 job completed May 2025

This proves the estimates are "floating" - they can be sold weeks or months after job completion.

---

## Next Steps Required

**URGENT: Clarify with user which report they pulled the baseline from**

Questions to ask:
1. What is the exact name of the ServiceTitan report you used for the baseline?
2. When you filtered for 10/20-10/26, which date field did you filter on?
   - Job Completion Date?
   - Estimate Sold Date?
   - Job Created Date?
3. Can you provide a screenshot showing:
   - Report name
   - Date filter applied
   - The actual numbers (190 completed estimates, $434,600 total sales, 40.34% close rate)

**Options going forward:**

### Option A: User's Baseline is from "Estimate Sold Date" report
- Our WBR implementation is correct for "Daily WBR C/R" report
- We need to create a SEPARATE mart view for "Revenue by Sold Date"
- Both metrics are valid but measure different things
- Document clearly which report/view to use for which KPI

### Option B: Service Titan's "Daily WBR C/R" really uses Sold Date
- Our understanding of the report is wrong
- Need to update wbr_jobs view to use sold date instead of completion date
- This would mean "Close Rate" calculation changes fundamentally

### Option C: The Baseline Numbers Are Wrong
- User pulled from the wrong week or wrong report
- We validate against the correct "Daily WBR C/R" report
- Accept current numbers as accurate

---

## Recommendation

I recommend **Option A**:
1. Keep the current WBR implementation (uses job completion date)
2. Create a separate view for "Revenue by Sold Date"
3. Clarify with user which metric they actually need
4. Document both approaches clearly

This allows flexibility and ensures we support both common business questions:
- "How did our sales team perform this week?" (job completion based)
- "How much revenue did we book this week?" (sold date based)

---

*Analysis completed: 2025-10-30 20:45 MST*
*Implementation: Phase 2 Complete - wbr_jobs view deployed*
*Status: Awaiting user clarification on baseline source*
