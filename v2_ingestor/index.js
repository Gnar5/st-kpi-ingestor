/**
 * ServiceTitan v2 Ingestor - Main Orchestrator
 * Coordinates all entity ingestion jobs and provides HTTP endpoints for Cloud Run
 */

import 'dotenv/config';
import express from 'express';
import { logger } from './src/utils/logger.js';
import ServiceTitanClient from './src/api/servicetitan_client.js';
import BigQueryClient from './src/bq/bigquery_client.js';
import {
  JobsIngestor,
  InvoicesIngestor,
  EstimatesIngestor,
  PaymentsIngestor,
  PayrollIngestor,
  CustomersIngestor,
  LocationsIngestor,
  CampaignsIngestor,
  AppointmentsIngestor,
  PurchaseOrdersIngestor,
  ReturnsIngestor
} from './src/ingestors/index.js';

// Reference/Dimension ingestors
import {
  BusinessUnitsIngestor,
  TechniciansIngestor,
  ActivityCodesIngestor
} from './src/ingestors_ref/index.js';

// Initialize clients
const stClient = new ServiceTitanClient();
const bqClient = new BigQueryClient();

// Initialize entity ingestors
const ingestors = {
  jobs: new JobsIngestor(stClient, bqClient),
  invoices: new InvoicesIngestor(stClient, bqClient),
  estimates: new EstimatesIngestor(stClient, bqClient),
  payments: new PaymentsIngestor(stClient, bqClient),
  payroll: new PayrollIngestor(stClient, bqClient),
  customers: new CustomersIngestor(stClient, bqClient),
  locations: new LocationsIngestor(stClient, bqClient),
  campaigns: new CampaignsIngestor(stClient, bqClient),
  appointments: new AppointmentsIngestor(stClient, bqClient),
  purchase_orders: new PurchaseOrdersIngestor(stClient, bqClient),
  returns: new ReturnsIngestor(stClient, bqClient)
};

// Initialize reference/dimension ingestors
const refIngestors = {
  business_units: new BusinessUnitsIngestor(stClient, bqClient),
  technicians: new TechniciansIngestor(stClient, bqClient),
  activity_codes: new ActivityCodesIngestor(stClient, bqClient)
};

// Express app
const app = express();
app.use(express.json());

/**
 * Health check endpoint
 */
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'healthy',
    service: 'st-v2-ingestor',
    version: '2.0.0',
    timestamp: new Date().toISOString()
  });
});

/**
 * Ingest specific entity
 * GET /ingest/:entity?mode=incremental
 */
app.get('/ingest/:entity', async (req, res) => {
  const { entity } = req.params;
  const mode = req.query.mode || 'incremental';

  if (!ingestors[entity]) {
    return res.status(404).json({
      error: 'Entity not found',
      availableEntities: Object.keys(ingestors)
    });
  }

  try {
    logger.info('Ingestion request received', { entity, mode });

    const result = await ingestors[entity].ingest({ mode });

    res.status(200).json({
      success: true,
      entity,
      mode,
      ...result
    });
  } catch (error) {
    logger.error('Ingestion request failed', {
      entity,
      mode,
      error: error.message
    });

    res.status(500).json({
      success: false,
      entity,
      mode,
      error: error.message
    });
  }
});

/**
 * Ingest all entities
 * GET /ingest-all?mode=incremental&parallel=true
 */
app.get('/ingest-all', async (req, res) => {
  const mode = req.query.mode || 'incremental';
  const parallel = req.query.parallel === 'true';

  logger.info('Ingest-all request received', { mode, parallel });

  try {
    const results = {};
    const errors = {};

    if (parallel) {
      // Run all ingestors in parallel
      const promises = Object.entries(ingestors).map(async ([entity, ingestor]) => {
        try {
          const result = await ingestor.ingest({ mode });
          results[entity] = result;
        } catch (error) {
          errors[entity] = error.message;
          logger.error('Parallel ingestion failed', { entity, error: error.message });
        }
      });

      await Promise.all(promises);
    } else {
      // Run sequentially
      for (const [entity, ingestor] of Object.entries(ingestors)) {
        try {
          const result = await ingestor.ingest({ mode });
          results[entity] = result;
        } catch (error) {
          errors[entity] = error.message;
          logger.error('Sequential ingestion failed', { entity, error: error.message });
        }
      }
    }

    const hasErrors = Object.keys(errors).length > 0;

    res.status(hasErrors ? 207 : 200).json({
      success: !hasErrors,
      mode,
      parallel,
      results,
      errors: hasErrors ? errors : undefined,
      summary: {
        total: Object.keys(ingestors).length,
        succeeded: Object.keys(results).length,
        failed: Object.keys(errors).length
      }
    });
  } catch (error) {
    logger.error('Ingest-all request failed', {
      mode,
      parallel,
      error: error.message
    });

    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * Get ingestion status/history
 * GET /status/:entity
 */
app.get('/status/:entity', async (req, res) => {
  const { entity } = req.params;

  try {
    const query = `
      SELECT
        entity_type,
        run_id,
        start_time,
        end_time,
        status,
        records_fetched,
        records_inserted,
        duration_ms,
        error_message
      FROM \`${bqClient.projectId}.${bqClient.datasetLogs}.ingestion_logs\`
      WHERE entity_type = @entity
      ORDER BY start_time DESC
      LIMIT 10
    `;

    const rows = await bqClient.query(query, {
      params: [{ name: 'entity', value: entity }]
    });

    res.status(200).json({
      entity,
      recentRuns: rows
    });
  } catch (error) {
    logger.error('Status request failed', { entity, error: error.message });

    res.status(500).json({
      error: error.message
    });
  }
});

/**
 * Get last sync time for entity
 * GET /last-sync/:entity
 */
app.get('/last-sync/:entity', async (req, res) => {
  const { entity } = req.params;

  try {
    const lastSync = await bqClient.getLastSyncTime(entity);

    res.status(200).json({
      entity,
      lastSyncTime: lastSync
    });
  } catch (error) {
    logger.error('Last sync request failed', { entity, error: error.message });

    res.status(500).json({
      error: error.message
    });
  }
});

/**
 * Manual trigger for full sync
 * POST /full-sync/:entity
 */
app.post('/full-sync/:entity', async (req, res) => {
  const { entity } = req.params;

  if (!ingestors[entity]) {
    return res.status(404).json({
      error: 'Entity not found',
      availableEntities: Object.keys(ingestors)
    });
  }

  try {
    logger.info('Full sync request received', { entity });

    const result = await ingestors[entity].ingest({ mode: 'full' });

    res.status(200).json({
      success: true,
      entity,
      mode: 'full',
      ...result
    });
  } catch (error) {
    logger.error('Full sync request failed', {
      entity,
      error: error.message
    });

    res.status(500).json({
      success: false,
      entity,
      error: error.message
    });
  }
});

/**
 * List available entities
 * GET /entities
 */
app.get('/entities', (req, res) => {
  res.status(200).json({
    entities: Object.keys(ingestors),
    count: Object.keys(ingestors).length
  });
});

/**
 * ==========================================================================
 * REFERENCE / DIMENSION ENDPOINTS
 * These ingest lookup tables (business units, technicians, activity codes)
 * ==========================================================================
 */

/**
 * Ingest specific reference dimension
 * GET /ingest-ref/:refEntity
 */
app.get('/ingest-ref/:refEntity', async (req, res) => {
  const { refEntity } = req.params;

  if (!refIngestors[refEntity]) {
    return res.status(404).json({
      error: 'Reference entity not found',
      availableRefEntities: Object.keys(refIngestors)
    });
  }

  try {
    logger.info('Reference ingestion request received', { refEntity });

    const result = await refIngestors[refEntity].ingest({});

    res.status(200).json({
      success: true,
      refEntity,
      ...result
    });
  } catch (error) {
    logger.error('Reference ingestion request failed', {
      refEntity,
      error: error.message
    });

    res.status(500).json({
      success: false,
      refEntity,
      error: error.message
    });
  }
});

/**
 * Ingest all reference dimensions
 * GET /ingest-ref-all
 */
app.get('/ingest-ref-all', async (req, res) => {
  logger.info('Ingest-ref-all request received');

  try {
    const results = {};
    const errors = {};

    // Run reference ingestors sequentially (they're fast)
    for (const [refEntity, ingestor] of Object.entries(refIngestors)) {
      try {
        const result = await ingestor.ingest({});
        results[refEntity] = result;
      } catch (error) {
        errors[refEntity] = error.message;
        logger.error('Reference ingestion failed', {
          refEntity,
          error: error.message
        });
      }
    }

    const hasErrors = Object.keys(errors).length > 0;

    res.status(hasErrors ? 207 : 200).json({
      success: !hasErrors,
      results,
      errors: hasErrors ? errors : undefined,
      summary: {
        total: Object.keys(refIngestors).length,
        succeeded: Object.keys(results).length,
        failed: Object.keys(errors).length
      }
    });
  } catch (error) {
    logger.error('Ingest-ref-all request failed', {
      error: error.message
    });

    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * List available reference entities
 * GET /ref-entities
 */
app.get('/ref-entities', (req, res) => {
  res.status(200).json({
    refEntities: Object.keys(refIngestors),
    count: Object.keys(refIngestors).length,
    description: 'Reference dimensions for ID-to-name lookups'
  });
});

/**
 * ==========================================================================
 * ASYNC BACKFILL ENDPOINT
 * Starts a full backfill in the background and returns immediately
 * ==========================================================================
 */

/**
 * Start full backfill asynchronously
 * GET /backfill-async
 *
 * This endpoint starts a sequential full backfill and returns immediately.
 * The backfill continues running in the background.
 * Use Cloud Logging or BigQuery logs to monitor progress.
 */
app.get('/backfill-async', async (req, res) => {
  // Return immediately to avoid HTTP timeout
  res.status(202).json({
    success: true,
    message: 'Full backfill started in background',
    status: 'running',
    entities: Object.keys(ingestors),
    estimatedDuration: '30-45 minutes',
    monitoring: {
      cloudLogs: 'gcloud run services logs read st-v2-ingestor --region=us-central1 --limit=50',
      bigQueryLogs: `SELECT * FROM \`${bqClient.projectId}.${bqClient.datasetLogs}.ingestion_logs\` ORDER BY start_time DESC LIMIT 50`,
      checkStatus: 'Run check_backfill_status.sh to verify data'
    }
  });

  // Start backfill in background (fire and forget)
  (async () => {
    logger.info('Starting async backfill - sequential mode');

    const startTime = Date.now();
    const results = {};
    const errors = {};
    const skipped = {};

    // Skip campaigns - already has full backfill (2020-05-18 to present)
    const skipEntities = ['campaigns'];

    for (const [entity, ingestor] of Object.entries(ingestors)) {
      if (skipEntities.includes(entity)) {
        logger.info(`Backfill: Skipping ${entity} (already has full data)`);
        skipped[entity] = 'Already has full backfill from 2020-05-18';
        continue;
      }

      try {
        logger.info(`Backfill: Starting ${entity}...`);
        const result = await ingestor.ingest({ mode: 'full' });
        results[entity] = result;
        logger.info(`Backfill: Completed ${entity}`, {
          recordsFetched: result.recordsFetched,
          recordsInserted: result.recordsInserted
        });
      } catch (error) {
        errors[entity] = error.message;
        logger.error(`Backfill: Failed ${entity}`, {
          entity,
          error: error.message
        });
      }
    }

    const duration = Date.now() - startTime;
    const hasErrors = Object.keys(errors).length > 0;

    logger.info('Async backfill completed', {
      durationMs: duration,
      durationMin: Math.round(duration / 60000),
      succeeded: Object.keys(results).length,
      skipped: Object.keys(skipped).length,
      failed: Object.keys(errors).length,
      skippedEntities: skipped,
      errors: hasErrors ? errors : undefined
    });
  })();
});

/**
 * Start server
 */
const PORT = process.env.PORT || 8080;

app.listen(PORT, () => {
  logger.info('ServiceTitan v2 Ingestor started', {
    port: PORT,
    entities: Object.keys(ingestors),
    environment: process.env.NODE_ENV || 'development'
  });
});

// Graceful shutdown
process.on('SIGTERM', () => {
  logger.info('SIGTERM received, shutting down gracefully');
  process.exit(0);
});

process.on('SIGINT', () => {
  logger.info('SIGINT received, shutting down gracefully');
  process.exit(0);
});

export default app;
