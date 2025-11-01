# Reporting API Investigation Results

## Date: October 30, 2025
## Investigation Goal: Find the correct ServiceTitan report that produces baseline WBR numbers

---

## Summary

After investigating the ServiceTitan Reporting API, we discovered that **9 out of 11 report IDs from config.json are NOT accessible** via the Reporting API.

---

## Key Findings

### 1. Report Accessibility Results

**Tested 11 reports from config/config.json:**

| Report Name | Report ID | Status | Category | Notes |
|-------------|-----------|--------|----------|-------|
| **Leads Report** | 389357017 | ❌ NOT FOUND | N/A | Returns 404 in all categories |
| **Daily WBR C/R** | 130700652 | ❌ NOT FOUND | N/A | Returns 404 in all categories |
| **Daily WBR Consolidated** | 397555674 | ❌ NOT FOUND | N/A | Returns 404 in all categories |
| **Phoenix-Sales Daily WBR** | 387935289 | ❌ NOT FOUND | N/A | Returns 404 in all categories |
| **Andy's Painting-Sales Daily WBR** | 387936790 | ❌ NOT FOUND | N/A | Returns 404 in all categories |
| **Commercial-AZ-Sales Daily WBR** | 387930556 | ❌ NOT FOUND | N/A | Returns 404 in all categories |
| **Guaranteed Painting-Sales Daily WBR** | 387945629 | ❌ NOT FOUND | N/A | Returns 404 in all categories |
| **Nevada-Sales Daily WBR** | 387945741 | ❌ NOT FOUND | N/A | Returns 404 in all categories |
| **Tucson-Sales Daily WBR** | 387951872 | ❌ NOT FOUND | N/A | Returns 404 in all categories |
| **Foreman Job Cost This Week** | 389438975 | ✅ FOUND | operations | Metadata accessible, requires DateType param |
| **Collections** | 26117979 | ✅ FOUND | accounting | Working (already in use) |

**Result:** Only 2 of 11 reports are accessible via the Reporting API.

---

## 2. Why Are WBR Reports Missing?

The WBR report IDs in config.json appear to be:

1. **Deleted or Archived**: Reports may have been removed from ServiceTitan
2. **Different Tenant**: Report IDs may belong to a different ServiceTitan tenant
3. **UI-Only Custom Views**: May not be accessible via API (only visible in ServiceTitan web UI)
4. **Different API Version**: May require different URL structure or API version

---

## 3. URL Patterns Tested

For each report, we tested 5 different URL patterns:

1. `https://api.servicetitan.io/reporting/v2/tenant/{TENANT_ID}/report-category/sales/reports/{REPORT_ID}`
2. `https://api.servicetitan.io/reporting/v2/tenant/{TENANT_ID}/report-category/operations/reports/{REPORT_ID}`
3. `https://api.servicetitan.io/reporting/v2/tenant/{TENANT_ID}/report-category/accounting/reports/{REPORT_ID}`
4. `https://api.servicetitan.io/reporting/v2/tenant/{TENANT_ID}/report-category/service/reports/{REPORT_ID}`
5. `https://api.servicetitan.io/reporting/v2/tenant/{TENANT_ID}/reports/{REPORT_ID}` (no category)

All 5 patterns returned 404 for the WBR reports.

---

## 4. Reporting API Technical Details

### Correct API Usage Pattern:

**Metadata Endpoint:**
```
GET /reporting/v2/tenant/{TENANT_ID}/report-category/{CATEGORY}/reports/{REPORT_ID}
```

**Data Endpoint:**
```
POST /reporting/v2/tenant/{TENANT_ID}/report-category/{CATEGORY}/reports/{REPORT_ID}/data

Body:
{
  "request": {
    "page": 1,
    "pageSize": 5000
  },
  "parameters": [
    { "name": "From", "value": "2025-10-20" },
    { "name": "To", "value": "2025-10-26" },
    { "name": "DateType", "value": "1" }  // If required
  ]
}
```

**Headers:**
- `Authorization: Bearer {TOKEN}`
- `ST-App-Key: {APP_KEY}`
- `Content-Type: application/json`

---

## 5. Connection to Data Discrepancy

This finding is **directly related to the 68% data gap** we discovered:

- **Expected (User's Baseline):** 190 completed estimates, $434,600, 40.34% close rate
- **Current Implementation:** 60 sold estimates (by sold date), $202,745

**Theory:** The user's baseline numbers may have come from one of the inaccessible WBR reports that:
1. Use different date logic (estimate sold date vs job completion date)
2. Use different filters or aggregation logic
3. Are no longer accessible via the API

---

## 6. Next Steps

### URGENT: Ask the user:

1. **Where did you pull the baseline numbers from?**
   - Report name in ServiceTitan?
   - Screenshot of the report showing filters?
   - Is it a saved report or ad-hoc analysis?

2. **Can you provide access to the report in ServiceTitan?**
   - Share the report URL
   - Export a sample to verify the numbers
   - Check if the report still exists

3. **Date filter used in the baseline:**
   - Job Completion Date?
   - Estimate Sold Date?
   - Job Created Date?
   - Other date field?

### Alternative Approaches:

**Option A: Use Entity API data (current implementation)**
- Build WBR metrics using jobs + estimates from Entity API v2
- Match ServiceTitan's WBR logic as closely as possible
- Document differences from the inaccessible WBR report

**Option B: Find alternate report**
- Search for other reports in ServiceTitan that show similar metrics
- Test if they're accessible via Reporting API
- Validate against user's baseline

**Option C: Build custom logic**
- Use raw entity data to replicate the expected calculations
- Create multiple date-based views (by completion, by sold date, by created date)
- Let user choose which metric matches their needs

---

## 7. What We Know Works

### Collections Report (ID: 26117979)
- **Category:** accounting
- **Parameters:** From, To, DateType (2 = payment date)
- **Status:** ✅ Working and in production use
- **Data Format:** Array of arrays (not objects)

### Foreman Job Cost Report (ID: 389438975)
- **Category:** operations
- **Parameters:** From, To, DateType (required), plus 2 others
- **Status:** ✅ Metadata accessible, not yet tested with data
- **Use Case:** Production job costing for painting crews

---

## 8. Config.json Report IDs Status

The report_ids section in config.json appears to be **outdated**:

```json
"report_ids": {
  "leads": "389357017",                    // ❌ NOT FOUND
  "daily_wbr_cr": "130700652",             // ❌ NOT FOUND
  "daily_wbr_consolidated": "397555674",   // ❌ NOT FOUND
  "foreman_job_cost_this_week": "389438975", // ✅ FOUND
  "collections": "26117979",                 // ✅ FOUND
  "ar_report": "235"                         // ⚠️  NOT TESTED
}
```

The `daily_wbr_report_ids_by_bu` section is also **outdated** - all 6 per-BU reports return 404.

---

## 9. Impact on Current Implementation

### What This Means:

1. **Cannot use v1 ingestors for WBR data** - the reports they reference don't exist
2. **Must build WBR logic using Entity API v2** - jobs + estimates + invoices
3. **Need user clarification on baseline source** - to understand which logic to replicate

### Current Status:

- ✅ Entity API v2 implementation complete (jobs, estimates, invoices, etc.)
- ✅ WBR foundation view created (st_stage.wbr_jobs)
- ❌ Cannot validate against Reporting API WBR data (reports inaccessible)
- ❓ Unknown if Entity API logic matches user's baseline numbers

---

## Recommendation

**Immediately contact the user with these questions:**

1. What is the exact report name you used for the baseline (190 completed estimates, $434,600)?
2. Is that report still accessible in your ServiceTitan account?
3. Can you share a screenshot showing the report name, date filter, and numbers?
4. What date filter did you use: Job Completion Date, Estimate Sold Date, or other?

**Until we get this information, we cannot resolve the 68% data discrepancy.**

---

*Investigation completed: 2025-10-30 21:15 MST*
*Test script: v2_ingestor/test_daily_wbr_cr.js*
*Config file: config/config.json*
