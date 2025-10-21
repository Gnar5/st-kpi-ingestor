# ServiceTitan v2 Reference Dimension Layer

**Status:** Production-Ready
**Version:** 1.0.0
**Dataset:** `st_ref_v2`
**Purpose:** ID-to-Name lookups for human-readable analytics

---

## Overview

The Reference Dimension Layer provides lookup tables that translate internal ServiceTitan IDs into human-readable names, enabling intuitive joins and dashboard-friendly reporting.

### What Problem Does This Solve?

**Before (Raw Data Only):**
```sql
SELECT
  businessUnitId,  -- Shows: 12345678
  SUM(total) as revenue
FROM `kpi-auto-471020.st_raw_v2.raw_invoices`
GROUP BY businessUnitId;
```

**After (With Reference Dimensions):**
```sql
SELECT
  bu.name AS business_unit,  -- Shows: "East Phoenix-Production"
  SUM(i.total) AS revenue
FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_business_units` bu
  ON i.businessUnitId = bu.id
GROUP BY bu.name;
```

---

## Architecture

### Directory Structure

```
v2_ingestor/
├── src/
│   ├── ingestors_ref/              # Reference dimension ingestors
│   │   ├── base_ref_ingestor.js    # Base class for all ref ingestors
│   │   ├── business_units.js       # Business unit lookups
│   │   ├── technicians.js          # Technician/employee lookups
│   │   ├── activity_codes.js       # Payroll activity code lookups
│   │   └── index.js                # Exports all ref ingestors
│   ├── api/
│   │   └── servicetitan_client.js  # Reference API methods added
│   └── bq/
│       └── bigquery_client.js      # Reused for ref tables
├── schema_registry_ref.json        # Reference dimension schemas
├── config_ref.json                 # Reference configuration
└── README_REF.md                   # This file
```

### Design Principles

1. **Full Refresh, Not Incremental**
   - Reference data is small (<1000 records per table)
   - Full refresh ensures completeness and catches deletions
   - Runs fast (< 10 seconds per table)

2. **No Partitioning**
   - Small tables don't benefit from partitioning
   - Reduces complexity and cost
   - Clustering on `active` and key fields provides sufficient performance

3. **Idempotent Upserts**
   - Uses `MERGE` statements for safe re-runs
   - Primary key deduplication
   - Prevents duplicate data

4. **Isolated Dataset**
   - Lives in `st_ref_v2` dataset, separate from `st_raw_v2`
   - Won't interfere with entity ingestion
   - Clear separation of concerns

---

## Available Reference Dimensions

### 1. Business Units (`dim_business_units`)

**Purpose:** Organizational units for revenue/cost allocation
**API Endpoint:** `settings/v2/tenant/{tenant}/business-units`
**Primary Key:** `id`
**Estimated Records:** ~10

**Schema:**
```sql
CREATE OR REPLACE TABLE `kpi-auto-471020.st_ref_v2.dim_business_units` (
  id INT64 NOT NULL,
  name STRING,
  active BOOL,
  officialName STRING,
  phoneNumber STRING,
  email STRING,
  address JSON,
  timezone STRING,
  _ingested_at TIMESTAMP NOT NULL,
  _ingestion_source STRING NOT NULL
)
CLUSTER BY active, name;
```

**Usage Example:**
```sql
SELECT
  bu.name,
  COUNT(DISTINCT j.id) as job_count,
  SUM(i.total) as total_revenue
FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i
  ON j.id = i.jobId
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_business_units` bu
  ON j.businessUnitId = bu.id
WHERE bu.active = TRUE
GROUP BY bu.name
ORDER BY total_revenue DESC;
```

---

### 2. Technicians (`dim_technicians`)

**Purpose:** Technician/employee master data for job assignments
**API Endpoint:** `settings/v2/tenant/{tenant}/technicians`
**Primary Key:** `id`
**Estimated Records:** ~500

**Schema:**
```sql
CREATE OR REPLACE TABLE `kpi-auto-471020.st_ref_v2.dim_technicians` (
  id INT64 NOT NULL,
  name STRING,
  active BOOL,
  businessUnitId INT64,
  businessUnitName STRING,
  email STRING,
  phoneNumber STRING,
  employeeId INT64,
  role STRING,
  team STRING,
  modifiedOn TIMESTAMP,
  _ingested_at TIMESTAMP NOT NULL,
  _ingestion_source STRING NOT NULL
)
CLUSTER BY active, businessUnitId;
```

**Usage Example:**
```sql
-- Technician productivity report
SELECT
  t.name AS technician,
  t.businessUnitName,
  COUNT(DISTINCT j.id) AS jobs_completed,
  SUM(p.amount) AS total_compensation
FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_technicians` t
  ON j.technicianId = t.id
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_payroll` p
  ON t.id = p.employeeId
WHERE t.active = TRUE
  AND j.completedOn >= CURRENT_DATE() - 30
GROUP BY t.name, t.businessUnitName
ORDER BY jobs_completed DESC;
```

---

### 3. Activity Codes (`dim_activity_codes`)

**Purpose:** Payroll activity type lookups
**API Endpoint:** `settings/v2/tenant/{tenant}/activity-codes`
**Primary Key:** `id`
**Estimated Records:** ~50

**Schema:**
```sql
CREATE OR REPLACE TABLE `kpi-auto-471020.st_ref_v2.dim_activity_codes` (
  id INT64 NOT NULL,
  name STRING,
  active BOOL,
  description STRING,
  code STRING,
  isPaid BOOL,
  modifiedOn TIMESTAMP,
  _ingested_at TIMESTAMP NOT NULL,
  _ingestion_source STRING NOT NULL
)
CLUSTER BY active, name;
```

**Usage Example:**
```sql
-- Payroll breakdown by activity type
SELECT
  ac.name AS activity,
  ac.isPaid,
  SUM(p.paidDurationHours) AS total_hours,
  SUM(p.amount) AS total_cost
FROM `kpi-auto-471020.st_raw_v2.raw_payroll` p
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_activity_codes` ac
  ON p.activityCodeId = ac.id
WHERE p.date >= CURRENT_DATE() - 30
GROUP BY ac.name, ac.isPaid
ORDER BY total_cost DESC;
```

---

## API Endpoints

### Ingest Single Reference Dimension

```bash
curl http://localhost:8081/ingest-ref/business_units
curl http://localhost:8081/ingest-ref/technicians
curl http://localhost:8081/ingest-ref/activity_codes
```

**Response:**
```json
{
  "success": true,
  "refEntity": "business_units",
  "recordsProcessed": 6,
  "runId": "a1b2c3d4-...",
  "duration": 3456
}
```

### Ingest All Reference Dimensions

```bash
curl http://localhost:8081/ingest-ref-all
```

**Response:**
```json
{
  "success": true,
  "results": {
    "business_units": {
      "success": true,
      "recordsProcessed": 6,
      "duration": 3456
    },
    "technicians": {
      "success": true,
      "recordsProcessed": 482,
      "duration": 8901
    },
    "activity_codes": {
      "success": true,
      "recordsProcessed": 42,
      "duration": 2345
    }
  },
  "summary": {
    "total": 3,
    "succeeded": 3,
    "failed": 0
  }
}
```

### List Available Reference Entities

```bash
curl http://localhost:8081/ref-entities
```

**Response:**
```json
{
  "refEntities": [
    "business_units",
    "technicians",
    "activity_codes"
  ],
  "count": 3,
  "description": "Reference dimensions for ID-to-name lookups"
}
```

---

## Cloud Scheduler Setup

### Create Scheduler Jobs

Run these commands to schedule nightly reference dimension refreshes at 3 AM (after entity ingestors complete):

```bash
# Set variables
PROJECT_ID="kpi-auto-471020"
REGION="us-central1"
SERVICE_URL="https://st-v2-ingestor-xxxxx-uc.a.run.app"  # Replace with your Cloud Run URL

# Create single job for all reference dimensions
gcloud scheduler jobs create http st-ref-all-daily \
  --project=$PROJECT_ID \
  --location=$REGION \
  --schedule="0 3 * * *" \
  --time-zone="America/Phoenix" \
  --uri="$SERVICE_URL/ingest-ref-all" \
  --http-method=GET \
  --description="Daily refresh of all reference dimensions"

# Or create individual jobs for finer control
gcloud scheduler jobs create http st-ref-business-units-daily \
  --project=$PROJECT_ID \
  --location=$REGION \
  --schedule="0 3 * * *" \
  --time-zone="America/Phoenix" \
  --uri="$SERVICE_URL/ingest-ref/business_units" \
  --http-method=GET \
  --description="Daily refresh of business units dimension"

gcloud scheduler jobs create http st-ref-technicians-daily \
  --project=$PROJECT_ID \
  --location=$REGION \
  --schedule="5 3 * * *" \
  --time-zone="America/Phoenix" \
  --uri="$SERVICE_URL/ingest-ref/technicians" \
  --http-method=GET \
  --description="Daily refresh of technicians dimension"

gcloud scheduler jobs create http st-ref-activity-codes-daily \
  --project=$PROJECT_ID \
  --location=$REGION \
  --schedule="10 3 * * *" \
  --time-zone="America/Phoenix" \
  --uri="$SERVICE_URL/ingest-ref/activity_codes" \
  --http-method=GET \
  --description="Daily refresh of activity codes dimension"
```

### Pause/Resume Jobs

```bash
# Pause
gcloud scheduler jobs pause st-ref-all-daily

# Resume
gcloud scheduler jobs resume st-ref-all-daily

# Delete
gcloud scheduler jobs delete st-ref-all-daily
```

---

## Common Join Patterns

### 1. Invoice Revenue by Business Unit

```sql
SELECT
  bu.name AS business_unit,
  DATE_TRUNC(i.createdOn, MONTH) AS month,
  COUNT(DISTINCT i.id) AS invoice_count,
  SUM(i.total) AS total_revenue,
  AVG(i.total) AS avg_invoice_value
FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_business_units` bu
  ON i.businessUnitId = bu.id
WHERE bu.active = TRUE
  AND i.createdOn >= '2025-01-01'
GROUP BY bu.name, month
ORDER BY month DESC, total_revenue DESC;
```

### 2. Technician Utilization

```sql
SELECT
  t.name AS technician,
  bu.name AS home_business_unit,
  COUNT(DISTINCT j.id) AS jobs_assigned,
  SUM(p.paidDurationHours) AS total_hours,
  SUM(p.amount) AS total_compensation,
  SAFE_DIVIDE(SUM(p.amount), SUM(p.paidDurationHours)) AS avg_hourly_rate
FROM `kpi-auto-471020.st_ref_v2.dim_technicians` t
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_business_units` bu
  ON t.businessUnitId = bu.id
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_jobs` j
  ON j.technicianId = t.id
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_payroll` p
  ON p.employeeId = t.id
WHERE t.active = TRUE
  AND j.completedOn >= CURRENT_DATE() - 30
GROUP BY t.name, bu.name
ORDER BY total_compensation DESC;
```

### 3. Payroll by Activity Type

```sql
SELECT
  ac.name AS activity_type,
  ac.isPaid,
  bu.name AS business_unit,
  SUM(p.paidDurationHours) AS total_hours,
  SUM(p.amount) AS total_cost,
  COUNT(DISTINCT p.payrollId) AS payroll_entries
FROM `kpi-auto-471020.st_raw_v2.raw_payroll` p
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_activity_codes` ac
  ON p.activityCodeId = ac.id
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_business_units` bu
  ON p.businessUnitName = bu.name  -- Note: payroll has BU name, not ID
WHERE p.date >= CURRENT_DATE() - 30
GROUP BY ac.name, ac.isPaid, bu.name
ORDER BY total_cost DESC;
```

### 4. Full Enriched Jobs View

```sql
CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.vw_jobs_enriched` AS
SELECT
  j.id,
  j.jobNumber,
  bu.name AS business_unit,
  t.name AS technician,
  c.name AS customer,
  l.address AS location_address,
  j.completedOn,
  j.total,
  j.status
FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_business_units` bu
  ON j.businessUnitId = bu.id
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_technicians` t
  ON j.technicianId = t.id
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_customers` c
  ON j.customerId = c.id
LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_locations` l
  ON j.locationId = l.id;
```

---

## Monitoring

### Check Reference Table Row Counts

```sql
SELECT
  table_name,
  row_count,
  ROUND(size_bytes/1024/1024, 2) AS size_mb,
  TIMESTAMP_MILLIS(creation_time) AS created_at,
  TIMESTAMP_MILLIS(last_modified_time) AS last_modified
FROM `kpi-auto-471020.st_ref_v2.__TABLES__`
ORDER BY table_name;
```

**Expected Results:**
| table_name | row_count | size_mb |
|------------|-----------|---------|
| dim_activity_codes | ~50 | < 0.01 |
| dim_business_units | ~10 | < 0.01 |
| dim_technicians | ~500 | < 0.05 |

### Check Recent Reference Runs

```sql
SELECT
  entity_type,
  start_time,
  status,
  records_fetched,
  records_inserted,
  ROUND(duration_ms/1000, 2) AS duration_sec,
  error_message
FROM `kpi-auto-471020.st_logs_v2.ingestion_logs`
WHERE entity_type LIKE 'ref_%'
ORDER BY start_time DESC
LIMIT 20;
```

### Check for Orphaned IDs

Find IDs in fact tables that don't have matching reference data:

```sql
-- Orphaned business unit IDs
SELECT DISTINCT
  j.businessUnitId,
  COUNT(*) AS orphan_count
FROM `kpi-auto-471020.st_raw_v2.raw_jobs` j
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_business_units` bu
  ON j.businessUnitId = bu.id
WHERE j.businessUnitId IS NOT NULL
  AND bu.id IS NULL
GROUP BY j.businessUnitId
ORDER BY orphan_count DESC;
```

---

## Troubleshooting

### Issue: Reference table is empty

**Cause:** API permissions or endpoint not found
**Solution:**
1. Check ServiceTitan API scopes in developer portal
2. Test endpoint directly:
   ```bash
   curl "https://api.servicetitan.io/settings/v2/tenant/$ST_TENANT_ID/business-units?page=1&pageSize=10" \
     -H "ST-App-Key: $ST_APP_KEY" \
     -H "Authorization: Bearer $ACCESS_TOKEN"
   ```
3. Check logs for 403/404 errors

### Issue: Joins returning NULL names

**Cause:** ID field mismatch or inactive records
**Solution:**
1. Check if filtering by `active = TRUE` is excluding needed records
2. Verify ID field types match (INT64 = INT64)
3. Use `LEFT JOIN` instead of `INNER JOIN` to see orphaned IDs

### Issue: Duplicate reference data

**Cause:** MERGE statement not working correctly
**Solution:**
1. Check primary key is unique in source data
2. Verify MERGE key matches primary key
3. Drop and recreate table if needed:
   ```bash
   bq rm -f -t kpi-auto-471020:st_ref_v2.dim_business_units
   curl http://localhost:8081/ingest-ref/business_units
   ```

---

## Adding New Reference Dimensions

To add a new reference dimension (e.g., `dim_job_types`):

### 1. Create Ingestor

Create `src/ingestors_ref/job_types.js`:

```javascript
import { BaseRefIngestor } from './base_ref_ingestor.js';

export class JobTypesIngestor extends BaseRefIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('job_types', stClient, bqClient, {
      tableId: 'dim_job_types',
      primaryKey: 'id',
      clusterFields: ['active', 'name'],
      ...config
    });
  }

  async fetch(options = {}) {
    return await this.stClient.getJobTypes({});
  }

  async transform(data) {
    return data.map(jt => ({
      id: jt.id,
      name: jt.name,
      active: jt.active !== undefined ? jt.active : true,
      _ingested_at: new Date().toISOString(),
      _ingestion_source: 'servicetitan_v2_ref'
    }));
  }

  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },
      { name: 'name', type: 'STRING', mode: 'NULLABLE' },
      { name: 'active', type: 'BOOL', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}
```

### 2. Export from Index

Add to `src/ingestors_ref/index.js`:
```javascript
export { JobTypesIngestor } from './job_types.js';
```

### 3. Register in Main Index

Add to `index.js`:
```javascript
import { JobTypesIngestor } from './src/ingestors_ref/index.js';

const refIngestors = {
  // ...existing...
  job_types: new JobTypesIngestor(stClient, bqClient)
};
```

### 4. Add to Schema Registry

Add to `schema_registry_ref.json`:
```json
"job_types": {
  "apiEndpoint": "settings/v2/tenant/{tenant}/job-types",
  "primaryKey": "id",
  "refreshMode": "full",
  "fields": {
    "id": { "type": "INT64", "nullable": false },
    "name": { "type": "STRING", "nullable": true },
    "active": { "type": "BOOL", "nullable": true }
  }
}
```

### 5. Test Locally

```bash
curl http://localhost:8081/ingest-ref/job_types
```

---

## Performance Metrics

**Expected Performance (per table):**
- Business Units: < 5 seconds
- Technicians: < 10 seconds
- Activity Codes: < 5 seconds

**Total refresh time:** < 20 seconds for all 3 tables

**Frequency:** Daily at 3 AM (after entity ingestion completes)

**BigQuery Costs:** Negligible (< $0.01/month for all reference tables combined)

---

## Production Deployment Checklist

- [ ] All reference ingestors tested locally
- [ ] BigQuery dataset `st_ref_v2` created
- [ ] Cloud Scheduler jobs created and tested
- [ ] Downstream dashboards updated to use reference joins
- [ ] Monitoring alerts configured for failed ref runs
- [ ] Documentation shared with analytics team
- [ ] Example queries validated with real data

---

## Support

**Questions?** Check the main [README.md](README.md) for general v2 ingestor documentation.

**Issues?** See [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) for troubleshooting.

**Feature Requests?** Add new reference dimensions using the guide above.

---

**Generated:** 2025-10-21
**Version:** 1.0.0
**Maintainer:** ST KPI Ingestor v2 Team
