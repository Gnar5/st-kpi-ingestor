# ServiceTitan v2 Ingestor

**Enterprise-grade ETL pipeline for ServiceTitan entity APIs → BigQuery**

Version: 2.0.0
Author: Your Company
Last Updated: 2025-10-21

---

## Overview

The ServiceTitan v2 Ingestor is a complete rewrite of the original report-based ingestion system. This version pulls directly from ServiceTitan's **entity APIs** (Jobs, Invoices, Estimates, Payments, Payroll, Customers, Locations, Campaigns) to create a comprehensive operational data warehouse in BigQuery.

### Key Features

- **Entity-level data access**: Direct API integration with ServiceTitan core entities
- **Incremental sync**: Efficient delta loads using `modifiedSince` timestamps
- **Idempotent upserts**: BigQuery MERGE operations prevent duplicates
- **Schema evolution**: Auto-detection and logging of schema drift
- **Fault tolerance**: Exponential backoff, circuit breakers, retry logic
- **Observability**: Structured logging, run tracking, health endpoints
- **Partitioning & clustering**: Optimized BigQuery table design
- **Parallel execution**: Run all ingestors concurrently or sequentially
- **Cloud-native**: Built for Google Cloud Run with auto-scaling

---

## Architecture

```
┌─────────────────┐
│  ServiceTitan   │
│   Entity APIs   │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│   ServiceTitan Client               │
│   • OAuth2 authentication           │
│   • Pagination handling             │
│   • Rate limiting (10 req/sec)      │
│   • Retry with exponential backoff  │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│   Entity Ingestors                  │
│   • Jobs, Invoices, Estimates       │
│   • Payments, Payroll               │
│   • Customers, Locations, Campaigns │
│   • Fetch → Transform → Validate    │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│   BigQuery Client                   │
│   • Table creation & schema mgmt    │
│   • MERGE upserts (idempotent)      │
│   • Sync state tracking             │
│   • Run logging                     │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│   BigQuery Datasets                 │
│   • st_raw_v2   (raw entity data)   │
│   • st_stage_v2 (transformed)       │
│   • st_mart_v2  (analytics-ready)   │
│   • st_logs_v2  (metadata & logs)   │
└─────────────────────────────────────┘
```

---

## Quick Start

### 1. Prerequisites

- **Node.js** ≥ 20.0.0
- **Google Cloud Project** with BigQuery enabled
- **ServiceTitan API credentials**:
  - Client ID
  - Client Secret
  - Tenant ID
  - App Key

### 2. Installation

```bash
cd v2_ingestor
npm install
```

### 3. Configuration

Copy the example environment file:

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```bash
# ServiceTitan
ST_CLIENT_ID=your_client_id
ST_CLIENT_SECRET=your_client_secret
ST_TENANT_ID=your_tenant_id
ST_APP_KEY=your_app_key

# BigQuery
BQ_PROJECT_ID=kpi-auto-471020
BQ_DATASET_RAW=st_raw_v2
BQ_DATASET_STAGE=st_stage_v2
BQ_DATASET_MART=st_mart_v2
BQ_DATASET_LOGS=st_logs_v2

# Application
PORT=8080
NODE_ENV=production
LOG_LEVEL=info
```

### 4. Create BigQuery Datasets & Tables

Run the DDL script:

```bash
bq query --use_legacy_sql=false < bigquery_schemas.sql
```

Or create datasets programmatically (first run will auto-create):

```bash
npm start
```

### 5. Run Locally

```bash
npm start
```

The service starts on `http://localhost:8080`

### 6. Test Endpoints

Health check:
```bash
curl http://localhost:8080/health
```

Ingest jobs (incremental):
```bash
curl http://localhost:8080/ingest/jobs
```

Ingest all entities in parallel:
```bash
curl "http://localhost:8080/ingest-all?parallel=true"
```

Full sync (all historical data):
```bash
curl -X POST http://localhost:8080/full-sync/invoices
```

---

## Deployment to Cloud Run

### Option 1: Using gcloud CLI

```bash
# Set your GCP project
gcloud config set project kpi-auto-471020

# Deploy to Cloud Run
gcloud run deploy st-v2-ingestor \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars="ST_CLIENT_ID=$ST_CLIENT_ID,ST_CLIENT_SECRET=$ST_CLIENT_SECRET,ST_TENANT_ID=$ST_TENANT_ID,ST_APP_KEY=$ST_APP_KEY,BQ_PROJECT_ID=kpi-auto-471020,BQ_DATASET_RAW=st_raw_v2,BQ_DATASET_STAGE=st_stage_v2,BQ_DATASET_MART=st_mart_v2,BQ_DATASET_LOGS=st_logs_v2" \
  --memory 2Gi \
  --cpu 2 \
  --timeout 3600 \
  --max-instances 5 \
  --min-instances 0
```

### Option 2: Using npm script

```bash
npm run deploy
```

### Verify Deployment

```bash
SERVICE_URL=$(gcloud run services describe st-v2-ingestor --region us-central1 --format 'value(status.url)')
curl $SERVICE_URL/health
```

---

## Scheduling with Cloud Scheduler

Create scheduled jobs for incremental syncs:

### Jobs (every 2 hours)
```bash
gcloud scheduler jobs create http jobs-sync \
  --schedule="0 */2 * * *" \
  --uri="$SERVICE_URL/ingest/jobs" \
  --http-method=GET \
  --location=us-central1 \
  --time-zone="America/Los_Angeles"
```

### Invoices (every 2 hours)
```bash
gcloud scheduler jobs create http invoices-sync \
  --schedule="0 */2 * * *" \
  --uri="$SERVICE_URL/ingest/invoices" \
  --http-method=GET \
  --location=us-central1 \
  --time-zone="America/Los_Angeles"
```

### All entities (daily at 2 AM, parallel)
```bash
gcloud scheduler jobs create http all-entities-sync \
  --schedule="0 2 * * *" \
  --uri="$SERVICE_URL/ingest-all?parallel=true" \
  --http-method=GET \
  --location=us-central1 \
  --time-zone="America/Los_Angeles" \
  --attempt-deadline=3600s
```

### Full sync (weekly on Sunday at 3 AM)
```bash
gcloud scheduler jobs create http weekly-full-sync \
  --schedule="0 3 * * 0" \
  --uri="$SERVICE_URL/ingest-all?mode=full&parallel=true" \
  --http-method=GET \
  --location=us-central1 \
  --time-zone="America/Los_Angeles" \
  --attempt-deadline=7200s
```

---

## API Endpoints

### Health Check
**GET** `/health`

Response:
```json
{
  "status": "healthy",
  "service": "st-v2-ingestor",
  "version": "2.0.0",
  "timestamp": "2025-10-21T12:00:00.000Z"
}
```

### List Entities
**GET** `/entities`

Response:
```json
{
  "entities": ["jobs", "invoices", "estimates", "payments", "payroll", "customers", "locations", "campaigns"],
  "count": 8
}
```

### Ingest Single Entity
**GET** `/ingest/:entity?mode=incremental`

Parameters:
- `entity`: One of the entities listed above
- `mode`: `incremental` (default) or `full`

Example:
```bash
curl "http://localhost:8080/ingest/jobs?mode=incremental"
```

Response:
```json
{
  "success": true,
  "entity": "jobs",
  "mode": "incremental",
  "recordsProcessed": 1247,
  "runId": "a1b2c3d4-e5f6-...",
  "duration": 12450
}
```

### Ingest All Entities
**GET** `/ingest-all?mode=incremental&parallel=true`

Parameters:
- `mode`: `incremental` (default) or `full`
- `parallel`: `true` or `false` (default: false)

Example:
```bash
curl "http://localhost:8080/ingest-all?parallel=true"
```

Response:
```json
{
  "success": true,
  "mode": "incremental",
  "parallel": true,
  "results": {
    "jobs": { "success": true, "recordsProcessed": 1247, "duration": 12450 },
    "invoices": { "success": true, "recordsProcessed": 3821, "duration": 18320 },
    ...
  },
  "summary": {
    "total": 8,
    "succeeded": 8,
    "failed": 0
  }
}
```

### Full Sync (Manual Trigger)
**POST** `/full-sync/:entity`

Example:
```bash
curl -X POST http://localhost:8080/full-sync/customers
```

### Get Ingestion Status
**GET** `/status/:entity`

Returns last 10 runs for the entity:
```json
{
  "entity": "jobs",
  "recentRuns": [
    {
      "run_id": "abc123",
      "start_time": "2025-10-21T10:00:00Z",
      "end_time": "2025-10-21T10:05:23Z",
      "status": "success",
      "records_fetched": 1247,
      "records_inserted": 1247,
      "duration_ms": 323000
    },
    ...
  ]
}
```

### Get Last Sync Time
**GET** `/last-sync/:entity`

```json
{
  "entity": "jobs",
  "lastSyncTime": "2025-10-21T10:00:00.000Z"
}
```

---

## Configuration Files

### `config.json`

Central configuration for all entities, sync schedules, and system settings.

Key sections:
- **entities**: Enable/disable, set sync mode, define schedules
- **sync**: Default modes, full sync day, lookback periods
- **bigquery**: Project, datasets, partitioning, clustering
- **servicetitan**: API URLs, rate limits, timeouts
- **logging**: Log levels, formats

### `schema_registry.json`

Tracks field definitions for all entities:
- Field names and types
- Nullability
- Descriptions
- API endpoints
- Primary keys
- Partition/cluster fields

Used for:
- Schema validation
- Auto-detection of new fields
- Documentation
- Migration planning

---

## Entity Ingestors

Each entity has a dedicated ingestor class that extends `BaseIngestor`:

| Entity | API Endpoint | Primary Key | Partition Field | Cluster Fields |
|--------|-------------|-------------|-----------------|----------------|
| **Jobs** | `jpm/v2/tenant/{tenant}/jobs` | `id` | `modifiedOn` | `businessUnitId`, `jobStatus` |
| **Invoices** | `accounting/v2/tenant/{tenant}/invoices` | `id` | `modifiedOn` | `businessUnitId`, `jobId`, `status` |
| **Estimates** | `sales/v2/tenant/{tenant}/estimates` | `id` | `modifiedOn` | `businessUnitId`, `jobId`, `status` |
| **Payments** | `accounting/v2/tenant/{tenant}/payments` | `id` | `modifiedOn` | `invoiceId`, `paymentTypeId`, `status` |
| **Payroll** | `payroll/v2/tenant/{tenant}/gross-pay-items` | `id` | `modifiedOn` | `employeeId`, `paidDate` |
| **Customers** | `crm/v2/tenant/{tenant}/customers` | `id` | `modifiedOn` | `type`, `active` |
| **Locations** | `crm/v2/tenant/{tenant}/locations` | `id` | `modifiedOn` | `customerId`, `active` |
| **Campaigns** | `marketing/v2/tenant/{tenant}/campaigns` | `id` | `modifiedOn` | `active`, `categoryId` |

### Adding New Entities

To add a new entity (e.g., `appointments`):

1. **Create ingestor class**: `src/ingestors/appointments.js`
   ```javascript
   import { BaseIngestor } from './base_ingestor.js';

   export class AppointmentsIngestor extends BaseIngestor {
     constructor(stClient, bqClient, config = {}) {
       super('appointments', stClient, bqClient, {
         tableId: 'raw_appointments',
         primaryKey: 'id',
         partitionField: 'modifiedOn',
         clusterFields: ['jobId', 'status'],
         ...config
       });
     }

     async fetch(options = {}) {
       const mode = options.mode || 'incremental';
       if (mode === 'full') {
         return await this.stClient.getAppointments();
       }
       const lastSync = await this.bqClient.getLastSyncTime(this.entityType);
       return await this.stClient.getAppointmentsIncremental(lastSync);
     }

     async transform(data) {
       return data.map(apt => ({
         id: apt.id,
         jobId: apt.jobId,
         start: this.parseDate(apt.start),
         end: this.parseDate(apt.end),
         status: apt.status,
         // ... other fields
         _ingested_at: new Date().toISOString(),
         _ingestion_source: 'servicetitan_v2'
       }));
     }

     getSchema() {
       return [
         { name: 'id', type: 'INT64', mode: 'REQUIRED' },
         { name: 'jobId', type: 'INT64', mode: 'NULLABLE' },
         // ... other fields
       ];
     }
   }
   ```

2. **Add to ServiceTitan client**: `src/api/servicetitan_client.js`
   ```javascript
   async getAppointments(params = {}) {
     return this.fetchAll('jpm/v2/tenant/{tenant}/appointments', params);
   }

   async getAppointmentsIncremental(modifiedSince) {
     return this.fetchIncremental(
       'jpm/v2/tenant/{tenant}/appointments',
       {},
       { modifiedSince }
     );
   }
   ```

3. **Register in orchestrator**: `index.js`
   ```javascript
   import { AppointmentsIngestor } from './src/ingestors/appointments.js';

   const ingestors = {
     // ... existing
     appointments: new AppointmentsIngestor(stClient, bqClient)
   };
   ```

4. **Add to config.json**:
   ```json
   "appointments": {
     "enabled": true,
     "syncMode": "incremental",
     "schedule": "0 */2 * * *",
     "priority": 1,
     "description": "Appointment scheduling data"
   }
   ```

5. **Add schema to registry**: Update `schema_registry.json`

6. **Create BigQuery table**: Add DDL to `bigquery_schemas.sql`

---

## Incremental Sync Logic

The system tracks the last successful sync time for each entity in `st_logs_v2.sync_state`.

**First Run**:
- No prior sync found
- Fetches data from `LOOKBACK_DAYS` ago (default: 7 days)

**Subsequent Runs**:
- Fetches only records with `modifiedOn >= lastSyncTime`
- Updates sync state on success

**Full Sync**:
- Triggered manually via `/full-sync/:entity` or `mode=full`
- Ignores lastSyncTime, fetches all data
- Useful for backfills or schema changes

---

## Error Handling & Resilience

### Retry Strategy
- **Exponential backoff**: 1s, 2s, 4s, 8s, 16s
- **Max retries**: 5 (configurable)
- **Jitter**: ±25% randomization to prevent thundering herd

### Circuit Breaker
- **Failure threshold**: 5 consecutive failures
- **Reset timeout**: 60 seconds
- **States**: CLOSED → OPEN → HALF_OPEN → CLOSED

### Rate Limiting
- **Token bucket algorithm**: 10 requests/second (ServiceTitan limit)
- **Bucket size**: 20 tokens (allows bursts)
- **Auto-refill**: Based on elapsed time

### Error Types
| Error Type | Retry? | Strategy |
|------------|--------|----------|
| Network timeout | ✅ | Exponential backoff |
| 429 Rate limit | ✅ | Longer backoff (30s+) |
| 5xx Server error | ✅ | Exponential backoff |
| 4xx Client error | ❌ | Fail fast (except 429) |
| Schema validation | ❌ | Log and alert |

---

## Monitoring & Observability

### Structured Logging

All logs are JSON-formatted for Cloud Logging:

```json
{
  "timestamp": "2025-10-21T10:00:00.000Z",
  "severity": "INFO",
  "context": "v2-ingestor:ingestor:jobs",
  "message": "Ingestion completed",
  "runId": "abc123",
  "entityType": "jobs",
  "recordsFetched": 1247,
  "recordsInserted": 1247,
  "durationMs": 12450
}
```

### Log Levels
- **DEBUG**: Detailed pagination, field mappings
- **INFO**: Run start/complete, record counts
- **WARN**: Schema drift, retries, non-fatal errors
- **ERROR**: Failures, exceptions
- **FATAL**: Critical system errors

### Run Tracking

Every ingestion run is logged to `st_logs_v2.ingestion_logs`:
- Run ID (UUID)
- Start/end time
- Status (success/failed)
- Records fetched/inserted
- Duration
- Error messages

### Query Examples

**Recent runs**:
```sql
SELECT entity_type, start_time, status, records_inserted, duration_ms
FROM `kpi-auto-471020.st_logs_v2.ingestion_logs`
WHERE DATE(start_time) = CURRENT_DATE()
ORDER BY start_time DESC;
```

**Failed runs**:
```sql
SELECT entity_type, start_time, error_message
FROM `kpi-auto-471020.st_logs_v2.ingestion_logs`
WHERE status = 'failed'
  AND start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);
```

**Sync state**:
```sql
SELECT entity_type, last_sync_time, records_processed
FROM `kpi-auto-471020.st_logs_v2.sync_state`
ORDER BY updated_at DESC;
```

---

## Migration from v1

### Parallel Operation

The v2 ingestor is designed to run **side-by-side** with the existing v1 system:

- **Different datasets**: `st_raw_v2`, `st_stage_v2`, `st_mart_v2`
- **Different Cloud Run service**: `st-v2-ingestor` (vs `st-kpi-ingestor`)
- **Different schedulers**: Create new Cloud Scheduler jobs with `-v2` suffix

### Migration Steps

1. **Deploy v2 alongside v1** (do not modify v1)
2. **Run v2 in parallel** for 2-4 weeks
3. **Validate data quality** in `st_raw_v2` vs `st_raw`
4. **Update dashboards** to point to v2 datasets
5. **Monitor performance** and fix any issues
6. **Decommission v1** once v2 is stable

### Data Validation

Compare record counts:
```sql
-- V1
SELECT COUNT(*) FROM `kpi-auto-471020.st_raw.raw_leads`;

-- V2 (equivalent entity: customers or jobs depending on mapping)
SELECT COUNT(*) FROM `kpi-auto-471020.st_raw_v2.raw_jobs`;
```

Compare totals:
```sql
-- V1 invoices
SELECT SUM(total) FROM `kpi-auto-471020.st_raw.raw_daily_wbr_v2`;

-- V2 invoices
SELECT SUM(total) FROM `kpi-auto-471020.st_raw_v2.raw_invoices`;
```

---

## Performance Optimization

### BigQuery Best Practices

1. **Partitioning**: All raw tables partitioned by `modifiedOn` (DAY)
2. **Clustering**: 2-4 fields per table (business_unit, job_id, status)
3. **Batch inserts**: 10,000 rows per chunk
4. **MERGE upserts**: Idempotent, prevents duplicates
5. **Partition expiration**: Logs expire after 365 days

### API Rate Limiting

ServiceTitan allows ~10 req/sec per tenant. The client enforces:
- Token bucket rate limiter
- Configurable via `RATE_LIMIT_PER_SECOND`
- Automatic backoff on 429 responses

### Parallel Execution

When using `parallel=true`:
- All ingestors run concurrently via `Promise.all()`
- Faster total execution time
- Higher API load (ensure rate limits are respected)
- Recommended for scheduled full syncs

Sequential execution:
- One entity at a time
- Lower API load
- Easier to debug
- Recommended for manual/ad-hoc syncs

---

## Troubleshooting

### Common Issues

**1. Authentication Failed**
```
Error: ServiceTitan authentication failed: 401 Unauthorized
```
**Solution**: Verify `ST_CLIENT_ID`, `ST_CLIENT_SECRET`, `ST_TENANT_ID` in `.env`

**2. Rate Limit Exceeded**
```
WARN: API request failed, retrying in 30000ms, status: 429
```
**Solution**: Reduce `RATE_LIMIT_PER_SECOND` or increase `MAX_CONCURRENT_REQUESTS`

**3. Schema Validation Failed**
```
ERROR: Schema validation failed: Required field missing: jobNumber
```
**Solution**: Update `schema_registry.json` or fix transform logic in ingestor

**4. BigQuery Insert Failed**
```
ERROR: Insert failed: No such table: kpi-auto-471020.st_raw_v2.raw_jobs
```
**Solution**: Run `bq query < bigquery_schemas.sql` to create tables

**5. Timeout on Large Sync**
```
ERROR: Cloud Run timeout after 3600s
```
**Solution**: Increase timeout: `gcloud run services update st-v2-ingestor --timeout=7200`

### Debug Mode

Enable debug logging:
```bash
LOG_LEVEL=debug npm start
```

Test single entity locally:
```bash
curl "http://localhost:8080/ingest/jobs?mode=full"
```

Check logs in Cloud Run:
```bash
gcloud run services logs read st-v2-ingestor --region us-central1
```

---

## Security Best Practices

### Credentials Management

- **Never commit `.env`** to version control
- Use **Google Secret Manager** for production:
  ```bash
  gcloud secrets create ST_CLIENT_SECRET --data-file=- <<< "$ST_CLIENT_SECRET"

  gcloud run services update st-v2-ingestor \
    --update-secrets=ST_CLIENT_SECRET=ST_CLIENT_SECRET:latest
  ```

### IAM Permissions

Required roles for Cloud Run service account:
- `roles/bigquery.dataEditor` (write to BQ)
- `roles/bigquery.jobUser` (run queries)
- `roles/logging.logWriter` (write logs)

### API Keys

- Rotate ServiceTitan credentials every 90 days
- Use separate credentials for dev/staging/prod
- Monitor API usage in ServiceTitan dashboard

---

## Support & Contribution

### Adding Features

To extend functionality:
1. Fork this project
2. Create feature branch: `git checkout -b feature/new-entity`
3. Implement changes
4. Test locally
5. Deploy to staging
6. Submit PR

### Reporting Issues

Include:
- Entity name
- Error message
- Run ID (from logs)
- Environment (local/Cloud Run)
- Steps to reproduce

---

## License

UNLICENSED - Internal use only

---

## Changelog

### v2.0.0 (2025-10-21)
- Initial release
- 8 entity ingestors (Jobs, Invoices, Estimates, Payments, Payroll, Customers, Locations, Campaigns)
- Incremental sync with state tracking
- BigQuery MERGE upserts
- Cloud Run deployment
- Schema registry
- Comprehensive logging

---

## Next Steps

1. **Deploy to Cloud Run**: Follow deployment guide above
2. **Schedule syncs**: Create Cloud Scheduler jobs
3. **Monitor runs**: Query `st_logs_v2.ingestion_logs`
4. **Build marts**: Create business-ready tables in `st_mart_v2`
5. **Update dashboards**: Point Looker Studio to v2 datasets
6. **Add entities**: Extend with additional ServiceTitan endpoints as needed

---

**Built with ❤️ for scalable, maintainable data pipelines**
