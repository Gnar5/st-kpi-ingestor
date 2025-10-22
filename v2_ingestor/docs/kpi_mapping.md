# KPI Mapping: ServiceTitan Reports → BigQuery

This document maps the current manual ServiceTitan report process to automated BigQuery queries.

## Business Unit Structure

### Sales Business Units
- Andy's Painting Sales
- Commercial-AZ-Sales
- Guaranteed Painting-Sales
- Nevada-Sales
- Phoenix-Sales (combined with "Z-DO NOT USE - West - Sales")
- Tucson-Sales

### Production Business Units
- Andy's Painting-Production
- Commercial-AZ-Production
- Guaranteed Painting-Production
- Nevada-Production
- Phoenix-Production (combined with "Z-DO NOT USE - West- Production")
- Tucson-Production

## KPI Definitions

### 1. Leads
**Report:** *Leads*
**Filters:**
- Business Unit: [Each sales BU separately]
- Job Creation Date: [Date range]
- Customer Name: Does not contain "Test"
- Job Type: Does contain "Estimate", Does not contain "COMM."

**Calculation:** Count Distinct of "Customer Name" Column

**BigQuery Mapping:**
```sql
SELECT
  businessUnitName,
  COUNT(DISTINCT customerId) as lead_count
FROM `st_raw_v2.raw_jobs`
WHERE
  DATE(createdOn) BETWEEN @start_date AND @end_date
  AND businessUnitName IN ('Andy''s Painting Sales', 'Commercial-AZ-Sales', ...)
  AND customerName NOT LIKE '%Test%'
  AND jobType LIKE '%Estimate%'
  AND jobType NOT LIKE '%COMM.%'
GROUP BY businessUnitName
```

### 2. Total Booked
**Report:** *Daily WBR C/R*
**Filters:**
- Business Unit: [Each sales BU separately]
- Job Type: [Long list of estimate types - see below]

**Job Types:**
- ESTIMATE- WINDOW WASHING
- Estimate
- Cabinets
- Estimate- Exterior PLUS Int Cabinets
- Estimate- Interior PLUS Cabinets
- ESTIMATE -RES-EXT-PRE 1960
- ESTIMATE -RES-INT/EXT-PRE 1960
- ESTIMATE-COMM-EXT
- ESTIMATE-COMM-EXT/INT
- ESTIMATE-COMM-INT
- ESTIMATE-COMM-PLANBID
- ESTIMATE-COMM-Striping
- ESTIMATE-FLOOR COATING-EPOXY
- ESTIMATE-FLOOR COATING-H&C Coatings
- ESTIMATE-POPCORN
- ESTIMATE-RES-EXT
- ESTIMATE-RES-EXT/INT
- ESTIMATE-RES-HOA
- ESTIMATE-RES-INT
- Estimate-RES-INT/EXT Plus Cabinets

**Calculation:** Sum of "Total Sales" Column

**BigQuery Mapping:**
```sql
-- Need to join jobs with estimates or invoices to get "Total Sales"
-- This likely comes from estimates.subtotal or invoices.total
SELECT
  j.businessUnitName,
  SUM(e.subtotal) as total_booked
FROM `st_raw_v2.raw_jobs` j
JOIN `st_raw_v2.raw_estimates` e ON j.id = e.jobId
WHERE
  DATE(j.createdOn) BETWEEN @start_date AND @end_date
  AND j.businessUnitName IN ('Andy''s Painting Sales', 'Commercial-AZ-Sales', ...)
  AND j.jobType IN ('ESTIMATE- WINDOW WASHING', 'Estimate', 'Cabinets', ...)
GROUP BY j.businessUnitName
```

### 3. $ Produced
**Report:** *FOREMAN Job Cost - THIS WEEK ONLY*
**Filters:**
- Job Start Date: [Date range]
- Business Unit: [Each production BU separately]
- Job Type: Is NOT one of: PM Inspection, Safety Inspection, Window/Solar Washing

**Calculation:** Sum of "Jobs Subtotal" Column (Total for each branch)

**BigQuery Mapping:**
```sql
SELECT
  businessUnitName,
  SUM(jobs_subtotal) as dollars_produced
FROM `st_raw_v2.raw_jobs`
WHERE
  DATE(startDate) BETWEEN @start_date AND @end_date
  AND businessUnitName IN ('Andy''s Painting-Production', 'Commercial-AZ-Production', ...)
  AND jobType NOT IN ('PM Inspection', 'Safety Inspection', 'Window/Solar Washing')
GROUP BY businessUnitName
```

### 4. G.P.M (Gross Profit Margin)
**Report:** *FOREMAN Job Cost - THIS WEEK ONLY*
**Filters:** Same as $ Produced

**Calculation:**
- EITHER: Average of "Job Gross Margin %" Column
- OR: (Sum of "Jobs Subtotal" / Sum of "Jobs Total Costs") * 100

**BigQuery Mapping:**
```sql
SELECT
  businessUnitName,
  -- Method 1: Average of individual margins
  AVG(gross_margin_pct) as avg_gpm,
  -- Method 2: Total calculation
  (SUM(jobs_subtotal) / NULLIF(SUM(jobs_total_cost), 0)) * 100 as total_gpm
FROM `st_raw_v2.raw_jobs`
WHERE
  DATE(startDate) BETWEEN @start_date AND @end_date
  AND businessUnitName IN ('Andy''s Painting-Production', 'Commercial-AZ-Production', ...)
  AND jobType NOT IN ('PM Inspection', 'Safety Inspection', 'Window/Solar Washing')
GROUP BY businessUnitName
```

### 5. $ Collected
**Report:** *Collections*
**Filters:**
- Business Unit: [Each production BU separately]

**Calculation:** Sum of "Amount" Column

**BigQuery Mapping:**
```sql
SELECT
  -- Need to determine which field has business unit
  -- Payments table likely needs to be joined with jobs or invoices
  SUM(amount) as dollars_collected
FROM `st_raw_v2.raw_payments`
WHERE
  DATE(createdOn) BETWEEN @start_date AND @end_date
  -- AND businessUnitName via join
GROUP BY businessUnitName
```

### 6. # Estimates
**Report:** *Daily WBR C/R*
**Filters:** Same as Total Booked

**Calculation:** Sum of "Completed Job" Column

**BigQuery Mapping:**
```sql
SELECT
  businessUnitName,
  COUNT(*) as completed_estimates
FROM `st_raw_v2.raw_jobs`
WHERE
  DATE(completedOn) BETWEEN @start_date AND @end_date
  AND businessUnitName IN ('Andy''s Painting Sales', 'Commercial-AZ-Sales', ...)
  AND jobType IN ('ESTIMATE- WINDOW WASHING', 'Estimate', 'Cabinets', ...)
  AND jobStatus = 'Completed' -- Or whatever indicates completion
GROUP BY businessUnitName
```

### 7. Success Rate (Close Rate)
**Report:** *Daily WBR C/R*
**Filters:** Same as Total Booked

**Calculation:** Average of "Close Rate" Column

**BigQuery Mapping:**
```sql
-- This likely needs estimates table to see sold vs not sold
SELECT
  businessUnitName,
  AVG(CASE WHEN e.status = 'Sold' THEN 1 ELSE 0 END) * 100 as close_rate
FROM `st_raw_v2.raw_jobs` j
JOIN `st_raw_v2.raw_estimates` e ON j.id = e.jobId
WHERE
  DATE(j.createdOn) BETWEEN @start_date AND @end_date
  AND j.businessUnitName IN ('Andy''s Painting Sales', 'Commercial-AZ-Sales', ...)
  AND j.jobType IN ('ESTIMATE- WINDOW WASHING', 'Estimate', 'Cabinets', ...)
GROUP BY j.businessUnitName
```

### 8. Future Bookings
**Report:** *FOREMAN Job Cost - THIS WEEK ONLY*
**Filters:**
- Job Start Date: Current Monday through +1 year
- Business Unit: [Each production BU separately]
- Job Type: Is NOT one of: PM Inspection, Safety Inspection, Window/Solar Washing

**Calculation:** Sum of "Jobs Subtotal" Column

**BigQuery Mapping:**
```sql
SELECT
  businessUnitName,
  SUM(jobs_subtotal) as future_bookings
FROM `st_raw_v2.raw_jobs`
WHERE
  DATE(startDate) BETWEEN CURRENT_DATE('America/Phoenix') AND DATE_ADD(CURRENT_DATE('America/Phoenix'), INTERVAL 1 YEAR)
  AND businessUnitName IN ('Andy''s Painting-Production', 'Commercial-AZ-Production', ...)
  AND jobType NOT IN ('PM Inspection', 'Safety Inspection', 'Window/Solar Washing')
GROUP BY businessUnitName
```

### 9. Warranty %
**Report:** *FOREMAN Job Cost - THIS WEEK ONLY*
**Filters:**
- Job Start Date: [Date range]
- Business Unit: [Each production BU separately]
- Job Type: IS one of: Warranty, Touchup

**Calculation:** (Sum of "Jobs Total Cost" / "$ Produced") * 100

**BigQuery Mapping:**
```sql
WITH warranty_costs AS (
  SELECT
    businessUnitName,
    SUM(jobs_total_cost) as warranty_cost
  FROM `st_raw_v2.raw_jobs`
  WHERE
    DATE(startDate) BETWEEN @start_date AND @end_date
    AND businessUnitName IN ('Andy''s Painting-Production', ...)
    AND jobType IN ('Warranty', 'Touchup')
  GROUP BY businessUnitName
),
produced AS (
  SELECT
    businessUnitName,
    SUM(jobs_subtotal) as dollars_produced
  FROM `st_raw_v2.raw_jobs`
  WHERE
    DATE(startDate) BETWEEN @start_date AND @end_date
    AND businessUnitName IN ('Andy''s Painting-Production', ...)
    AND jobType NOT IN ('PM Inspection', 'Safety Inspection', 'Window/Solar Washing')
  GROUP BY businessUnitName
)
SELECT
  w.businessUnitName,
  (w.warranty_cost / NULLIF(p.dollars_produced, 0)) * 100 as warranty_pct
FROM warranty_costs w
JOIN produced p ON w.businessUnitName = p.businessUnitName
```

### 10. Outstanding A/R
**Report:** *AR Report*
**Filters:**
- Location Name: Is not "Name"
- Net Amount: >= $10
- Business Unit: [Each production BU separately]

**Calculation:** Sum of "Net Amount" Column

**BigQuery Mapping:**
```sql
-- Need to check if invoices table has outstanding balances
-- Or if there's a separate AR field
SELECT
  businessUnitName,
  SUM(net_amount) as outstanding_ar
FROM `st_raw_v2.raw_invoices`
WHERE
  locationName != 'Name'
  AND net_amount >= 10
  AND businessUnitName IN ('Andy''s Painting-Production', ...)
  AND balance > 0 -- Still has outstanding balance
GROUP BY businessUnitName
```

## Data Gaps to Investigate

1. **Jobs Subtotal** - Need to verify this field exists in raw_jobs
2. **Jobs Total Cost** - Need to verify this field exists in raw_jobs
3. **Jobs Gross Margin %** - May need to calculate or verify field name
4. **Close Rate** - Need to understand how estimates track sold/not sold status
5. **Net Amount** - Need to verify field in invoices table for A/R
6. **Business Unit Mapping** - Need to handle Phoenix + Z-DO NOT USE combinations

## Next Steps

1. Inspect actual schema of raw_jobs, raw_estimates, raw_invoices tables
2. Create reference table for business unit mappings (including combined units)
3. Create reference table for job type filters (estimate types, excluded types)
4. Build KPI mart views with all calculations
5. Validate against actual ServiceTitan report outputs
