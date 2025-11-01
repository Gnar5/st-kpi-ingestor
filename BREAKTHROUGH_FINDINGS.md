# BREAKTHROUGH: Found Reports with Close Rate Data

## Date: October 30, 2025
## Status: CRITICAL FINDINGS - Possible Solution to Data Discrepancy

---

## Executive Summary

After comprehensive investigation of the ServiceTitan Reporting API, we discovered:

1. ✅ **138 accessible reports** (50 operations, 38 accounting, 50 marketing)
2. ✅ **"*BA Customers & Revenue*" report (ID: 132073290)** - Contains Close Rate field
3. ✅ **"Leads" report IS accessible** in the marketing category (ID: 389357017)
4. ❌ **All 9 WBR reports from config.json do NOT exist** (404 errors)

---

## Critical Discovery: BA Customers & Revenue Report

**Report ID:** 132073290
**Category:** marketing
**Description:** "This report shows Brand Ambassadors total Jobs created new and existing customers, total Sales vs. Revenue, & close rate"

### Fields:
1. Campaign Name
2. Jobs Booked from New Customers
3. Jobs Booked from Existing Customers
4. Total Jobs Booked
5. **Close Rate** ← KEY FIELD
6. **Total Sales** ← KEY FIELD
7. Completed Revenue
8. WIP Jobs
9. Campaign Category

### Sample Data (10/20-10/26):
```
Keith:  7 jobs booked, 50% close rate, $10,065.25 total sales
Sean:   3 jobs booked, 50% close rate, $600 total sales
Brad:   1 job booked,  null close rate, $0 total sales
Total:  7 records (Brand Ambassador campaigns)
```

### Notes:
- **This report groups by Campaign (Brand Ambassador names)**
- Close rate is calculated per campaign/BA
- Data format: Array of arrays (not objects)
- Successfully tested and working

---

## Other Key Reports Found

### 1. Leads Report (ID: 389357017)
- **Category:** marketing (NOT sales!)
- **Status:** Metadata accessible, data fetch requires DateType parameter
- **Description:** "List of the leads we got by date range and business unit"
- **Note:** This is the report referenced in config.json

### 2. BMG - # of Sold Jobs, Amount, Lead Source (ID: 40761142)
- **Category:** marketing
- **Description:** "Filter by Sold On date. Use Sales business Units"
- **Fields:** Sold On date, Estimates Total, Business Unit
- **Note:** Requires DateType parameter (likely "Sold On" date)

### 3. BMG - # of Estimates & Lead Source (ID: 26396477)
- **Category:** marketing
- **Description:** "Date Type: Parent Completion Date, sales business units"
- **Note:** Uses "Parent Completion Date" as date filter

### 4. Weekly Batch (ID: 133963674)
- **Category:** operations
- **Description:** "Filter by start date, select all business units, use prior week dates (Monday-Sunday)"

---

## Why WBR Reports from config.json Don't Exist

**Reports NOT found (all return 404):**
1. daily_wbr_cr (130700652)
2. daily_wbr_consolidated (397555674)
3. Phoenix-Sales Daily WBR (387935289)
4. Andy's Painting-Sales Daily WBR (387936790)
5. Commercial-AZ-Sales Daily WBR (387930556)
6. Guaranteed Painting-Sales Daily WBR (387945629)
7. Nevada-Sales Daily WBR (387945741)
8. Tucson-Sales Daily WBR (387951872)
9. Z_DO NOT USE - West - Sales Daily WBR (387950018)

**Likely reasons:**
1. Reports were deleted or archived in ServiceTitan
2. Report IDs belong to a different tenant/environment
3. Reports are custom UI views not accessible via API

---

## Connection to Baseline Numbers

### User's Baseline (10/20-10/26):
- **Leads:** 227
- **Completed Estimates:** 190
- **Close Rate:** 40.34%
- **Total Booked:** $434,600

### Our Current Implementation:
- **Leads:** 220 (97% accuracy via Entity API)
- **Completed Estimates:** 60 sold estimates by sold date OR 187 completed jobs (depending on interpretation)
- **Close Rate:** 24.06% (by job completion) OR varies by campaign
- **Total Booked:** $202,745 (by sold date) OR $179,542 (by completion date)

### Analysis:

The BA Customers & Revenue report shows close rates **per campaign/Brand Ambassador**, not overall. The user's 40.34% close rate likely comes from a different calculation or report.

**Possible scenarios:**

1. **User is using a deleted/archived report** that we can't access
2. **User is calculating manually** from multiple reports
3. **User is using a different date filter** (e.g., estimate sold date vs job completion date)
4. **The baseline came from a specific Business Unit** filter we haven't matched

---

## Recommendations

### Immediate Action Required:

**Ask the user these specific questions:**

1. **Which ServiceTitan report name did you use for the baseline?**
   - Can you provide the exact report name shown in ServiceTitan?
   - Can you share a screenshot of the report with filters visible?

2. **Is the data grouped by Campaign/Business Unit or is it an overall total?**
   - BA Customers & Revenue shows per-campaign close rates
   - We found close rates of 50% for some campaigns, not 40.34%

3. **What date filter did you use?**
   - Job Created Date?
   - Job Completion Date?
   - Estimate Sold Date?
   - Other?

4. **Did you filter to specific Business Units?**
   - Sales units only?
   - Specific markets (Phoenix, Tucson, Nevada)?
   - All units?

### Alternative Approaches:

**Option A: Use BA Customers & Revenue Report**
- Aggregate campaign-level data to get overall close rate
- Match date filter to user's selection
- Compare totals to baseline

**Option B: Use BMG Reports**
- "BMG - # of Sold Jobs" uses Sold On date filter
- May match user's $434,600 total booked number
- Need to test with DateType parameter

**Option C: Continue with Entity API**
- Keep current implementation using jobs + estimates
- Create multiple date-based views
- Let user choose which matches their baseline

---

## Technical Details: How to Access BA Report

```javascript
const reportId = '132073290';
const category = 'marketing';

// Metadata URL
const metadataUrl = `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${category}/reports/${reportId}`;

// Data URL (POST request)
const dataUrl = `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${category}/reports/${reportId}/data`;

// Request body
const body = {
  request: { page: 1, pageSize: 5000 },
  parameters: [
    { name: 'From', value: '2025-10-20' },
    { name: 'To', value: '2025-10-26' }
  ]
};

// Response format: Array of arrays
// [0] Campaign Name
// [1] Jobs Booked from New Customers
// [2] Jobs Booked from Existing Customers
// [3] Total Jobs Booked
// [4] Close Rate (decimal, e.g., 0.5 = 50%)
// [5] Total Sales
// [6] Completed Revenue
// [7] WIP Jobs
// [8] Campaign Category
```

---

## Report Discovery Summary

### Total Reports Found: 138

**Operations (50 reports):**
- Job costing reports
- Weekly trackers
- Production numbers
- Appointment tracking

**Accounting (38 reports):**
- AR reports
- Collections ✅ (already using)
- Invoice details
- Payment reports

**Marketing (50 reports):**
- Leads reports ✅ (found!)
- Campaign performance
- Customer lists
- Close rate data ✅ (BA report)

**Sales/Service/Customers:**
- List endpoint returns 404 (cannot discover reports)

---

## Next Steps

1. **User provides clarification** on baseline source
2. **Test BMG reports** with DateType parameter to see sold date data
3. **Compare BA report aggregated data** to user's baseline
4. **Decide on final implementation** based on user's actual report source

---

## Files Generated

1. `/v2_ingestor/test_daily_wbr_cr.js` - Tests WBR reports (all return 404)
2. `/v2_ingestor/list_available_reports.js` - Discovers all 138 accessible reports
3. `/v2_ingestor/test_leads_and_ba_reports.js` - Tests marketing reports (BA report works!)
4. `/v2_ingestor/all_reports_found.json` - Complete list of 138 reports with metadata
5. `/v2_ingestor/wbr_reports_found.json` - 41 reports containing WBR-related keywords
6. `/v2_ingestor/_ba_customers_revenue__data.json` - Sample data from BA report (10/20-10/26)

---

*Investigation completed: 2025-10-30 21:30 MST*
*Status: Awaiting user clarification on baseline source*
*Key Finding: BA Customers & Revenue report contains Close Rate field*
