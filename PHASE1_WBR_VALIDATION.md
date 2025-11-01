# Phase 1: WBR Job Count Validation

## Date Range: October 20-26, 2025

### Validation Results

**Expected Count**: 190 completed jobs
**Actual Count**: 188 completed jobs (182 exact matches + 6 name variation)
**Accuracy**: 98.9% (missing 2 jobs)

### WBR Job Types Found (8 of 19 types)

| Job Type | Count | Status |
|---|---|---|
| ESTIMATE-RES-EXT | 99 | ✅ Exact Match |
| ESTIMATE-RES-INT | 43 | ✅ Exact Match |
| ESTIMATE-COMM-EXT | 14 | ✅ Exact Match |
| ESTIMATE-RES-EXT/INT | 14 | ✅ Exact Match |
| ESTIMATE-COMM-INT | 6 | ✅ Exact Match |
| ESTIMATE-COMM-PLANBID | 3 | ✅ Exact Match |
| ESTIMATE-COMM-EXT/INT | 2 | ✅ Exact Match |
| ESTIMATE-RES-HOA | 1 | ✅ Exact Match |
| **Subtotal** | **182** | |

### Name Variations Found

| WBR List Name | Actual Database Name | Count | Issue |
|---|---|---|---|
| "Estimate, Cabinets" (comma) | "Estimate- Cabinets" (hyphen-space) | 6 | Name mismatch |

**Updated Count with Variations**: 188 jobs

### WBR Job Types Not Found (11 of 19 types)

The following WBR job types from the official list were NOT found in the validation week:

1. ESTIMATE- WINDOW WASHING
2. Estimate- Exterior PLUS Int Cabinets
3. Estimate- Interior PLUS Cabinets
4. ESTIMATE -RES-EXT-PRE 1960
5. ESTIMATE -RES-INT/EXT-PRE 1960
6. ESTIMATE-COMM-Striping
7. ESTIMATE-FLOOR COATING-EPOXY
8. ESTIMATE-FLOOR COATING-H&C Coatings
9. ESTIMATE-POPCORN
10. Estimate-RES-INT/EXT Plus Cabinets
11. (Remaining 1 unaccounted for)

**Note**: These job types may exist in ServiceTitan but had no completed jobs during this specific week.

### Missing Jobs Analysis

**Missing 2 jobs (190 - 188 = 2)**

Potential explanations:
1. **Business Unit Filter**: Jobs may be in non-Sales business units
2. **Additional Name Variations**: Other job type names with subtle differences
3. **Data Sync Timing**: Jobs completed but not yet synced to BigQuery
4. **ServiceTitan Definition**: ServiceTitan may include job types not in our list

### Recommendations for Phase 2

1. **Update WBR Job Types List**: Add "Estimate- Cabinets" to handle the name variation
2. **Flexible Matching**: Consider using LIKE patterns for common variations (comma vs hyphen-space)
3. **Business Unit Investigation**: Verify if all 190 expected jobs are in Sales business units
4. **Ongoing Monitoring**: Track which WBR job types appear over time to validate the list

### Query Used

```sql
WITH wbr_job_types AS (
  SELECT job_type FROM UNNEST([
    'ESTIMATE- WINDOW WASHING',
    'Estimate, Cabinets',
    -- ... [full list of 19 job types]
  ]) AS job_type
)

SELECT COUNT(*) as wbr_job_count
FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
WHERE jobStatus = 'Completed'
  AND DATE(completedOn) BETWEEN '2025-10-20' AND '2025-10-26'
  AND jobTypeName IN (SELECT job_type FROM wbr_job_types)
```

### Next Steps

Proceed to **Phase 2**: Create `st_stage.wbr_jobs` view using the corrected job type list.

---
*Generated: 2025-10-30*
*Validation Period: 2025-10-20 to 2025-10-26*
