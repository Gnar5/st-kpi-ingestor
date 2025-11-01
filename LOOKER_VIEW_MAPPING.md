# Looker View Mapping - Production KPI Views

## ✅ Use These Views in Looker (V2 Views - CURRENT)

Connect Looker to these **st_mart_v2** views for each KPI:

### 1. Lead Count
**View:** `kpi-auto-471020.st_mart_v2.leads_daily_bu`
- **Grain:** One row per date per business unit
- **Date Field:** `kpi_date` (DATE)
- **BU Field:** `business_unit` (STRING) - ends with "-Sales"
- **Metric Field:** `leads_count` (INTEGER)
- **Additional Fields:**
  - `total_estimate_jobs` - Number of jobs with estimates
  - `view_created_at` - Timestamp of last refresh

**Looker Dimensions:**
- `kpi_date` (date)
- `business_unit` (string)

**Looker Measures:**
- `leads_count` (sum)

---

### 2. Number of Estimates
**View:** `kpi-auto-471020.st_mart_v2.completed_estimates_daily`
- **Grain:** One row per date per business unit
- **Date Field:** `kpi_date` (DATE)
- **BU Field:** `business_unit` (STRING) - ends with "-Sales"
- **Metric Field:** `completed_estimates_count` (INTEGER)
- **Additional Fields:**
  - `unique_customers` - Count of distinct customers
  - `view_created_at` - Timestamp of last refresh

**Looker Dimensions:**
- `kpi_date` (date)
- `business_unit` (string)

**Looker Measures:**
- `completed_estimates_count` (sum)
- `unique_customers` (sum)

---

### 3. Close Rate %
**View:** `kpi-auto-471020.st_mart_v2.opportunity_daily`
- **Grain:** One row per date per business unit
- **Date Field:** `kpi_date` (DATE)
- **BU Field:** `business_unit` (STRING) - ends with "-Sales"
- **Metric Fields:**
  - `sales_opportunities` (INTEGER) - Total opportunities
  - `closed_opportunities` (INTEGER) - Sold opportunities
  - `close_rate_percent` (FLOAT) - Pre-calculated close rate
- **Additional Fields:**
  - `unique_customers` - Distinct customer count
  - `total_estimates` - Total estimate count
  - `total_sold_estimates` - Sold estimate count

**Looker Dimensions:**
- `kpi_date` (date)
- `business_unit` (string)

**Looker Measures:**
- `sales_opportunities` (sum)
- `closed_opportunities` (sum)
- `close_rate_percent` (average) - Or calculate: closed/sales*100

**Note:** You can either use the pre-calculated `close_rate_percent` field or calculate it yourself in Looker as `SUM(closed_opportunities) / SUM(sales_opportunities) * 100`

---

### 4. Total Booked
**View:** `kpi-auto-471020.st_mart_v2.total_booked_daily`
- **Grain:** One row per date per business unit
- **Date Field:** `kpi_date` (DATE)
- **BU Field:** `business_unit` (STRING) - ends with "-Sales"
- **Metric Field:** `total_booked` (FLOAT64)
- **Additional Fields:**
  - `sold_jobs_count` - Number of jobs sold
  - `unique_customers` - Distinct customers
  - `avg_job_value` - Average value per job
  - `view_created_at` - Timestamp of last refresh

**Looker Dimensions:**
- `kpi_date` (date)
- `business_unit` (string)

**Looker Measures:**
- `total_booked` (sum)
- `sold_jobs_count` (sum)
- `avg_job_value` (average)

---

### 5. Dollars Produced
**View:** `kpi-auto-471020.st_mart_v2.dollars_produced_daily`
- **Grain:** One row per date per business unit
- **Date Field:** `kpi_date` (DATE) - Actually named `kpi_date`, but comes from `p.start_date`
- **BU Field:** `business_unit` (STRING) - ends with "-Production"
- **Metric Field:** `dollars_produced` (FLOAT64)
- **Additional Fields:**
  - `total_gross_profit` - Gross profit amount
  - `total_cost` - Total costs (labor + materials)
  - `total_labor_cost` - Labor costs
  - `total_material_cost` - Material costs
  - `total_job_count` - Number of jobs
  - `warranty_job_count` - Number of warranty jobs
  - `completed_job_count` - Completed jobs
  - `hold_job_count` - Jobs on hold
  - `gpm_percent` - GPM for reference
  - `labor_efficiency` - Revenue per labor dollar

**Looker Dimensions:**
- `kpi_date` (date)
- `business_unit` (string)

**Looker Measures:**
- `dollars_produced` (sum)
- `total_job_count` (sum)
- `total_gross_profit` (sum)
- `total_cost` (sum)

---

### 6. GPM % (Gross Profit Margin)
**View:** `kpi-auto-471020.st_mart_v2.gpm_daily_bu`
- **Grain:** One row per date per business unit
- **Date Field:** `kpi_date` (DATE)
- **BU Field:** `business_unit` (STRING) - ends with "-Production"
- **Metric Fields:**
  - `total_revenue` (FLOAT64)
  - `total_labor_cost` (FLOAT64)
  - `total_material_cost` (FLOAT64)
  - `total_cost` (FLOAT64)
  - `gross_profit` (FLOAT64)
  - `gpm_percent` (FLOAT64) - Pre-calculated GPM
- **Additional Fields:**
  - `job_count` - Number of jobs
  - `labor_percent_of_revenue` - Labor as % of revenue
  - `material_percent_of_revenue` - Materials as % of revenue

**Looker Dimensions:**
- `kpi_date` (date)
- `business_unit` (string)

**Looker Measures:**
- `total_revenue` (sum)
- `gross_profit` (sum)
- `total_cost` (sum)
- `job_count` (sum)
- `gpm_percent` (weighted average) - Calculate: SUM(gross_profit) / SUM(total_revenue) * 100

**Important:** Don't use AVG(gpm_percent) - calculate weighted average as shown above!

---

### 7. Warranty %
**View:** `kpi-auto-471020.st_mart_v2.warranty_percent_daily_bu`
- **Grain:** One row per date per business unit
- **Date Field:** `kpi_date` (DATE)
- **BU Field:** `business_unit` (STRING) - ends with "-Production"
- **Metric Fields:**
  - `warranty_revenue` (FLOAT64)
  - `total_revenue` (FLOAT64)
  - `warranty_percent` (FLOAT64) - Pre-calculated warranty %
- **Additional Fields:**
  - `warranty_job_count` - Number of warranty jobs
  - `total_job_count` - Total jobs
  - `avg_warranty_job_revenue` - Average warranty job size

**Looker Dimensions:**
- `kpi_date` (date)
- `business_unit` (string)

**Looker Measures:**
- `warranty_revenue` (sum)
- `total_revenue` (sum)
- `warranty_job_count` (sum)
- `warranty_percent` (weighted average) - Calculate: SUM(warranty_revenue) / SUM(total_revenue) * 100

---

### 8. Outstanding AR
**View:** `kpi-auto-471020.st_mart_v2.outstanding_ar_daily_bu`
- **Grain:** One row per date per business unit
- **Date Field:** `kpi_date` (DATE)
- **BU Field:** `business_unit` (STRING) - ends with "-Production"
- **Metric Field:** `outstanding_amount` (FLOAT64)
- **Additional Fields:**
  - `outstanding_invoice_count` - Number of unpaid invoices
  - `avg_outstanding_per_invoice` - Average amount per invoice
  - `oldest_invoice_date` - Date of oldest unpaid invoice
  - `days_outstanding_avg` - Average days outstanding

**Looker Dimensions:**
- `kpi_date` (date)
- `business_unit` (string)

**Looker Measures:**
- `outstanding_amount` (sum)
- `outstanding_invoice_count` (sum)
- `days_outstanding_avg` (average)

**Note:** This is a snapshot metric - typically you'd use the most recent date for current AR.

---

### 9. Future Bookings
**View:** `kpi-auto-471020.st_mart_v2.future_bookings_daily_bu`
- **Grain:** One row per date per business unit
- **Date Field:** `kpi_date` (DATE) - The snapshot date
- **BU Field:** `business_unit` (STRING) - ends with "-Production"
- **Metric Field:** `future_bookings_amount` (FLOAT64)
- **Additional Fields:**
  - `future_appointments_count` - Number of future appointments
  - `avg_future_job_value` - Average value per future job
  - `earliest_future_date` - Earliest future appointment
  - `latest_future_date` - Latest future appointment

**Looker Dimensions:**
- `kpi_date` (date)
- `business_unit` (string)

**Looker Measures:**
- `future_bookings_amount` (sum)
- `future_appointments_count` (sum)
- `avg_future_job_value` (average)

**Note:** This is a snapshot metric - typically you'd use the most recent date for current future bookings.

---

### 10. Dollars Collected
**View:** `kpi-auto-471020.st_mart_v2.collections_daily_bu`
- **Grain:** One row per payment date per business unit
- **Date Field:** `payment_date` (DATE) ⚠️ Different field name!
- **BU Field:** `business_unit` (STRING) - Production BUs but WITHOUT suffix
- **Metric Field:** `total_collections` (FLOAT64)
- **Additional Fields:**
  - `payment_count` - Number of payments
  - `avg_payment_amount` - Average payment size
  - `min_payment` - Smallest payment
  - `max_payment` - Largest payment
  - `last_updated` - When data was last refreshed

**Looker Dimensions:**
- `payment_date` (date) ⚠️ Not kpi_date!
- `business_unit` (string)

**Looker Measures:**
- `total_collections` (sum)
- `payment_count` (sum)
- `avg_payment_amount` (average)

**Important Notes:**
- Use `payment_date` NOT `kpi_date` for time dimensions
- Business units in collections don't have "-Production" suffix
- Data sourced from ServiceTitan Reporting API (not entity API)

---

## 🔴 DO NOT Use These Views (V1 Views - DEPRECATED)

These are old views from the v1 ingestor and should NOT be used in Looker:

- ❌ `st_mart.daily_kpis` - Old combined KPI view
- ❌ `st_mart.regional_kpis` - Old regional view
- ❌ `st_mart.leads_daily` - Old leads view (no BU breakdown)
- ❌ Any views in `st_raw` dataset (not st_raw_v2)
- ❌ Any `*_norm` tables in st_stage (old normalized data from CSVs)

---

## Looker Connection Details

### BigQuery Connection Settings:
- **Project ID:** `kpi-auto-471020`
- **Dataset for KPIs:** `st_mart_v2`
- **Auth Method:** Service Account (recommended) or OAuth

### Recommended Looker Explores:

**Sales Funnel Explore:**
```lookml
explore: sales_funnel {
  label: "Sales Funnel Metrics"

  view_name: leads_daily_bu

  join: completed_estimates_daily {
    type: left_outer
    sql_on: ${leads_daily_bu.kpi_date} = ${completed_estimates_daily.kpi_date}
      AND ${leads_daily_bu.business_unit} = ${completed_estimates_daily.business_unit} ;;
    relationship: one_to_one
  }

  join: opportunity_daily {
    type: left_outer
    sql_on: ${leads_daily_bu.kpi_date} = ${opportunity_daily.kpi_date}
      AND ${leads_daily_bu.business_unit} = ${opportunity_daily.business_unit} ;;
    relationship: one_to_one
  }

  join: total_booked_daily {
    type: left_outer
    sql_on: ${leads_daily_bu.kpi_date} = ${total_booked_daily.kpi_date}
      AND ${leads_daily_bu.business_unit} = ${total_booked_daily.business_unit} ;;
    relationship: one_to_one
  }
}
```

**Production Metrics Explore:**
```lookml
explore: production_metrics {
  label: "Production & Financial Metrics"

  view_name: dollars_produced_daily

  join: gpm_daily_bu {
    type: left_outer
    sql_on: ${dollars_produced_daily.kpi_date} = ${gpm_daily_bu.kpi_date}
      AND ${dollars_produced_daily.business_unit} = ${gpm_daily_bu.business_unit} ;;
    relationship: one_to_one
  }

  join: warranty_percent_daily_bu {
    type: left_outer
    sql_on: ${dollars_produced_daily.kpi_date} = ${warranty_percent_daily_bu.kpi_date}
      AND ${dollars_produced_daily.business_unit} = ${warranty_percent_daily_bu.business_unit} ;;
    relationship: one_to_one
  }

  join: collections_daily_bu {
    type: left_outer
    sql_on: ${dollars_produced_daily.kpi_date} = ${collections_daily_bu.payment_date}
      AND REPLACE(${dollars_produced_daily.business_unit}, '-Production', '') = ${collections_daily_bu.business_unit} ;;
    relationship: one_to_many
  }
}
```

---

## Business Unit Name Mapping

### Sales BUs (Leads, Estimates, Close Rate, Total Booked):
- `Andy's Painting-Sales`
- `Commercial-AZ-Sales`
- `Guaranteed Painting-Sales`
- `Nevada-Sales`
- `Phoenix-Sales`
- `Tucson-Sales`

### Production BUs (Dollars Produced, GPM, Warranty, AR, Future):
- `Andy's Painting-Production`
- `Commercial-AZ-Production`
- `Guaranteed Painting-Production`
- `Nevada-Production`
- `Phoenix-Production`
- `Tucson-Production`

### Collections BUs (Special - No Suffix):
- `Andy's Painting`
- `Commercial-AZ`
- `Guaranteed Painting`
- `Nevada`
- `Phoenix`
- `Tucson`

**In Looker:** Create a dimension that strips the suffix for consistent grouping:
```lookml
dimension: bu_name_clean {
  sql: REPLACE(REPLACE(${business_unit}, '-Sales', ''), '-Production', '') ;;
}
```

---

## Data Freshness

All views are refreshed based on the underlying raw data sync schedule:

- **Sales Metrics:** Updated after daily syncs (2:00-3:15 AM Arizona time)
- **Production Metrics:** Updated after daily syncs + new PO/payroll syncs (2:00-5:05 AM)
- **Collections:** Updated weekly (Sundays 6:20 AM) or after manual triggers

Views are **not materialized** - they query underlying tables in real-time, so data is always current as of last ingest.

---

## Quick Reference Table

| KPI | View Name | Date Field | BU Suffix | Key Metric Column |
|-----|-----------|------------|-----------|-------------------|
| 1. Leads | `leads_daily_bu` | `kpi_date` | `-Sales` | `leads_count` |
| 2. Estimates | `completed_estimates_daily` | `kpi_date` | `-Sales` | `completed_estimates_count` |
| 3. Close Rate | `opportunity_daily` | `kpi_date` | `-Sales` | `close_rate_percent` |
| 4. Total Booked | `total_booked_daily` | `kpi_date` | `-Sales` | `total_booked` |
| 5. Dollars Produced | `dollars_produced_daily` | `kpi_date` | `-Production` | `dollars_produced` |
| 6. GPM % | `gpm_daily_bu` | `kpi_date` | `-Production` | `gpm_percent` |
| 7. Warranty % | `warranty_percent_daily_bu` | `kpi_date` | `-Production` | `warranty_percent` |
| 8. Outstanding AR | `outstanding_ar_daily_bu` | `kpi_date` | `-Production` | `outstanding_amount` |
| 9. Future Bookings | `future_bookings_daily_bu` | `kpi_date` | `-Production` | `future_bookings_amount` |
| 10. Collections | `collections_daily_bu` | **`payment_date`** ⚠️ | **none** ⚠️ | `total_collections` |

---

## Testing Your Looker Connection

Use these sample queries to test each view:

```sql
-- Test Leads (should return ~219 for 8/18-8/24)
SELECT SUM(leads_count) as total_leads
FROM `kpi-auto-471020.st_mart_v2.leads_daily_bu`
WHERE kpi_date BETWEEN '2025-08-18' AND '2025-08-24';

-- Test Total Booked (should return ~$489K for 8/18-8/24)
SELECT SUM(total_booked) as total_booked
FROM `kpi-auto-471020.st_mart_v2.total_booked_daily`
WHERE kpi_date BETWEEN '2025-08-18' AND '2025-08-24';

-- Test GPM (should return ~48% weighted avg for 8/18-8/24)
SELECT
  SUM(gross_profit) / SUM(total_revenue) * 100 as gpm_percent
FROM `kpi-auto-471020.st_mart_v2.gpm_daily_bu`
WHERE kpi_date BETWEEN '2025-08-18' AND '2025-08-24';
```

---

## Need Help?

- **View Schemas:** Use `bq show --schema kpi-auto-471020:st_mart_v2.<view_name>`
- **Sample Data:** Use `bq head kpi-auto-471020:st_mart_v2.<view_name>`
- **Validation Report:** See `KPI_VALIDATION_RESULTS_8_18_8_24.md`
- **Technical Details:** See `DEPLOYMENT_STATUS_FINAL.md`
