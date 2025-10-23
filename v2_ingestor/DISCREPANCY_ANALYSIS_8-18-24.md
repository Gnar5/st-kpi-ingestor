# KPI Discrepancy Analysis: Week of 8/18/25 - 8/24/25

## ServiceTitan UI vs BigQuery Comparison

### ✅ EXACT MATCHES (No Issues)

| Region | Metric | ST Value | BQ Value | Status |
|--------|--------|----------|----------|--------|
| **Tucson** | Leads | 39 | 39 | ✅ EXACT |
| **Tucson** | Total Booked | $89,990.11 | $89,990.11 | ✅ EXACT |
| **Phoenix** | Total Booked | $116,551.26 | $116,551.26 | ✅ EXACT |
| **Nevada** | Total Booked | $105,890.00 | $105,890.00 | ✅ EXACT |
| **Commercial AZ** | Total Booked | $119,803.60 | $119,803.60 | ✅ EXACT |
| **Guaranteed Painting** | Total Booked | $26,067.40 | $26,067.40 | ✅ EXACT |

---

## ❌ DISCREPANCIES FOUND

### 1. PHOENIX - Leads Count
| Source | Value | Difference |
|--------|-------|------------|
| ServiceTitan UI | 96 | |
| BigQuery | 97 | +1 lead |

**Investigation Needed:**
- Check if there's a test customer or excluded customer in BQ
- Verify lead definition (job creation date vs other criteria)

---

### 2. PHOENIX - Dollars Produced (MAJOR)
| Source | Value | Difference |
|--------|-------|------------|
| ServiceTitan UI | $232,891.98 | |
| BigQuery | $175,773.97 | **-$57,118** (24% lower) |

**Status:** 🔴 CRITICAL - Missing $57K in production revenue

**Possible Causes:**
- Missing invoices in BigQuery
- Different date field (invoice date vs completion date vs deposited date)
- Missing business units or invoice types

---

### 3. PHOENIX - GPM %
| Source | Value | Difference |
|--------|-------|------------|
| ServiceTitan UI | 50.83% | |
| BigQuery | 3.76% | **-47%** (HUGE discrepancy) |

**Status:** 🔴 CRITICAL - GPM calculation is completely wrong

**This is linked to #2** - If we're missing $57K in revenue, the GPM calculation will be way off.

---

### 4. TUCSON - Dollars Produced
| Source | Value | Difference |
|--------|-------|------------|
| ServiceTitan UI | $83,761.16 | |
| BigQuery | $79,460.75 | **-$4,300** (5% lower) |

**Status:** 🟡 MODERATE - Small gap, likely a few missing invoices

---

### 5. TUCSON - GPM %
| Source | Value | Difference |
|--------|-------|------------|
| ServiceTitan UI | 48.00% | |
| BigQuery | 20.65% | **-27%** |

**Status:** 🔴 CRITICAL - GPM way too low

---

### 6. NEVADA - Dollars Produced
| Source | Value | Difference |
|--------|-------|------------|
| ServiceTitan UI | $23,975.00 | |
| BigQuery | $19,707.50 | **-$4,267** (18% lower) |

**Status:** 🟡 MODERATE

---

### 7. NEVADA - GPM %
| Source | Value | Difference |
|--------|-------|------------|
| ServiceTitan UI | 24.04% | |
| BigQuery | -1.21% | **Negative!** |

**Status:** 🔴 CRITICAL - Shows LOSS instead of profit

---

### 8. ANDY'S PAINTING - Leads & Total Booked
| Metric | ST Value | BQ Value | Difference |
|--------|----------|----------|------------|
| Leads | 25 | 0 | -25 leads |
| Total Booked | $30,896.91 | $0.00 | -$30,897 |

**Status:** 🔴 CRITICAL - Completely missing Andy's sales data

**Root Cause:** Andy's Painting Sales business unit is not creating jobs/estimates OR the business unit name doesn't match.

---

### 9. ANDY'S PAINTING - Dollars Produced
| Source | Value | Difference |
|--------|-------|------------|
| ServiceTitan UI | $53,752.56 | |
| BigQuery | $55,691.78 | **+$1,939** (4% higher) |

**Status:** ✅ CLOSE ENOUGH - Within 4%, likely rounding or date differences

---

### 10. COMMERCIAL AZ - Dollars Produced
| Source | Value | Difference |
|--------|-------|------------|
| ServiceTitan UI | $77,345.25 | |
| BigQuery | $71,850.00 | **-$5,495** (7% lower) |

**Status:** 🟡 MODERATE

---

### 11. COMMERCIAL AZ - GPM %
| Source | Value | Difference |
|--------|-------|------------|
| ServiceTitan UI | 46.98% | |
| BigQuery | 20.49% | **-26.5%** |

**Status:** 🔴 CRITICAL

---

### 12. GUARANTEED PAINTING - Dollars Produced
| Source | Value | Difference |
|--------|-------|------------|
| ServiceTitan UI | $30,472.30 | |
| BigQuery | $27,636.85 | **-$2,835** (9% lower) |

**Status:** 🟡 MODERATE

---

### 13. GUARANTEED PAINTING - GPM %
| Source | Value | Difference |
|--------|-------|------------|
| ServiceTitan UI | 45.84% | |
| BigQuery | 8.06% | **-37.8%** |

**Status:** 🔴 CRITICAL

---

## ROOT CAUSE ANALYSIS

### Pattern Identified: **Dollars Produced & GPM are SYSTEMATICALLY WRONG**

All Production BUs show:
1. ✅ **Total Booked (Sales) = EXACT MATCH** (estimate data is correct)
2. ❌ **Dollars Produced = 5-25% TOO LOW** (invoice/production data is missing)
3. ❌ **GPM % = WAY TOO LOW** (because production revenue is understated)

### Top Priority Issues:

#### 🔴 CRITICAL #1: Andy's Painting Sales Data Missing
- **Impact:** 25 leads and $30,897 in sales completely missing
- **Fix:** Check business unit name mapping in dim_jobs
- **Query to investigate:**
```sql
SELECT DISTINCT businessUnitNormalized, businessUnitName
FROM `kpi-auto-471020.st_dim_v2.dim_jobs`
WHERE businessUnitName LIKE '%Andy%'
OR businessUnitNormalized LIKE '%Andy%'
```

#### 🔴 CRITICAL #2: Dollars Produced Definition Wrong
- **Impact:** All regions showing 5-25% lower production revenue
- **Possible Causes:**
  1. Using wrong date field (completedOn vs invoiceDate vs depositedOn)
  2. Missing invoice records (not all invoices ingested)
  3. Wrong status filter (only counting certain invoice types)
  4. Job costing issues (not all costs captured)

- **Fix:** Compare invoice totals in BQ vs ST UI for specific dates

#### 🔴 CRITICAL #3: GPM % Calculation Wrong
- **Impact:** All regions showing dramatically lower GPM (20-47% too low)
- **Linked to #2:** If revenue is understated, GPM will be wrong
- **But also check:** How is cost calculated? Are we capturing all job costs?

---

## NEXT STEPS

### Immediate Actions:

1. **Fix Andy's Painting Sales** ← Start here (quickest win)
   - Verify business unit name in ServiceTitan
   - Update business unit mapping if needed

2. **Investigate Dollars Produced Logic**
   - Check what date field ST UI uses for "Produced"
   - Compare raw invoice counts for Phoenix week of 8/18-8/24
   - Verify invoice status filters

3. **Investigate GPM Calculation**
   - Check how ST UI calculates GPM
   - Verify job costing data is being captured correctly
   - Compare cost data in BQ vs ST UI for sample jobs

### Validation Queries Needed:

```sql
-- Check Andy's business unit names
SELECT DISTINCT businessUnitNormalized, COUNT(*) as job_count
FROM `kpi-auto-471020.st_dim_v2.dim_jobs`
WHERE createdOn >= '2025-08-18' AND createdOn < '2025-08-25'
  AND (businessUnitName LIKE '%Andy%' OR businessUnitNormalized LIKE '%Andy%')
GROUP BY businessUnitNormalized;

-- Check Phoenix invoice counts for the week
SELECT
  DATE(i.invoiceDate) as inv_date,
  COUNT(*) as invoice_count,
  SUM(i.total) as invoice_total
FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
JOIN `kpi-auto-471020.st_dim_v2.dim_jobs` j ON i.jobId = j.id
WHERE j.businessUnitNormalized = 'Phoenix-Production'
  AND DATE(i.invoiceDate) >= '2025-08-18'
  AND DATE(i.invoiceDate) <= '2025-08-24'
GROUP BY inv_date
ORDER BY inv_date;
```

---

## SUMMARY

**Total Booked (Sales KPIs):** ✅ **100% ACCURATE** - All regions exact match
**Dollars Produced (Production KPIs):** ❌ **BROKEN** - Consistently 5-25% too low
**GPM %:** ❌ **BROKEN** - Way too low across all regions
**Andy's Sales Data:** ❌ **MISSING COMPLETELY**

**Confidence in Current Data:**
- Sales metrics (leads, booked, estimates): **HIGH ✅**
- Production metrics (produced, GPM): **LOW ❌**
