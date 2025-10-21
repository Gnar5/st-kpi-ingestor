/**
 * Base Reference Ingestor Class
 * Provides common functionality for all reference/dimension ingestors
 *
 * Key differences from BaseIngestor:
 * - Targets st_ref_v2 dataset instead of st_raw_v2
 * - Uses full refresh by default (reference data is small)
 * - Minimal partitioning (modifiedOn only if available)
 * - Simpler schema validation (dimensions are stable)
 */

import { logger } from '../utils/logger.js';
import { randomUUID } from 'crypto';

export class BaseRefIngestor {
  constructor(entityType, stClient, bqClient, config = {}) {
    this.entityType = entityType;
    this.stClient = stClient;
    this.bqClient = bqClient;
    this.config = config;

    this.log = logger.child(`ref-ingestor:${entityType}`);

    // Reference dimensions live in st_ref_v2 dataset
    this.datasetId = config.datasetId || process.env.BQ_DATASET_REF || 'st_ref_v2';
    this.tableId = config.tableId || `dim_${entityType}`;
    this.primaryKey = config.primaryKey;  // REQUIRED - no default
    this.partitionField = config.partitionField || null;  // Optional for refs
    this.clusterFields = config.clusterFields || [];

    // Reference tables typically use full refresh
    this.refreshMode = config.refreshMode || 'full';
  }

  /**
   * Main ingestion flow for reference dimensions
   * Simpler than entity ingestors - no schema validation, direct upsert
   */
  async ingest(options = {}) {
    const runId = randomUUID();
    const startTime = new Date();
    const mode = options.mode || this.refreshMode;

    this.log.info('Reference ingestion started', {
      runId,
      entityType: this.entityType,
      mode,
      dataset: this.datasetId,
      table: this.tableId
    });

    const metadata = {
      entity_type: this.entityType,
      run_id: runId,
      start_time: startTime.toISOString(),
      status: 'running',
      records_fetched: 0,
      records_inserted: 0
    };

    try {
      // 1. Fetch data from ServiceTitan
      const data = await this.fetch({ mode });
      metadata.records_fetched = data.length;

      this.log.info('Reference data fetched', {
        runId,
        entityType: this.entityType,
        recordCount: data.length
      });

      if (data.length === 0) {
        this.log.warn('No reference data returned - this may indicate an API issue', {
          runId,
          entityType: this.entityType
        });
        metadata.status = 'success';
        metadata.end_time = new Date().toISOString();
        metadata.duration_ms = Date.now() - startTime.getTime();

        // Log to st_logs_v2 (reuse existing logging infrastructure)
        await this.bqClient.logRun(`ref_${this.entityType}`, metadata);

        return {
          success: true,
          recordsProcessed: 0,
          warning: 'No records returned from API'
        };
      }

      // 2. Transform/normalize data
      const transformed = await this.transform(data);

      // 3. Ensure dataset and table exist
      await this.bqClient.ensureDataset(this.datasetId);

      const schema = this.getSchema();
      await this.bqClient.ensureTable(
        this.datasetId,
        this.tableId,
        schema,
        {
          partitionField: this.partitionField,
          clusterFields: this.clusterFields,
          updateSchema: true
        }
      );

      // 4. Load to BigQuery using MERGE for idempotency
      const result = await this.bqClient.upsert(
        this.datasetId,
        this.tableId,
        transformed,
        this.primaryKey
      );

      metadata.records_inserted = result.merged || result.inserted || 0;
      metadata.status = 'success';
      metadata.end_time = new Date().toISOString();
      metadata.duration_ms = Date.now() - startTime.getTime();

      // 5. Update sync state
      await this.bqClient.updateLastSyncTime(
        `ref_${this.entityType}`,
        new Date().toISOString(),
        'success',
        metadata.records_inserted
      );

      // 6. Log run
      await this.bqClient.logRun(`ref_${this.entityType}`, metadata);

      this.log.info('Reference ingestion completed', {
        runId,
        entityType: this.entityType,
        recordsFetched: metadata.records_fetched,
        recordsInserted: metadata.records_inserted,
        durationMs: metadata.duration_ms
      });

      return {
        success: true,
        recordsProcessed: metadata.records_inserted,
        runId,
        duration: metadata.duration_ms
      };

    } catch (error) {
      metadata.status = 'failed';
      metadata.end_time = new Date().toISOString();
      metadata.duration_ms = Date.now() - startTime.getTime();
      metadata.error_message = error.message;

      await this.bqClient.logRun(`ref_${this.entityType}`, metadata);

      this.log.error('Reference ingestion failed', {
        runId,
        entityType: this.entityType,
        error: error.message,
        stack: error.stack
      });

      throw error;
    }
  }

  /**
   * Fetch data from ServiceTitan - must be implemented by child classes
   */
  async fetch(options = {}) {
    throw new Error('fetch() must be implemented by child class');
  }

  /**
   * Transform data - can be overridden by child classes
   * Default adds metadata fields
   */
  async transform(data) {
    return data.map(item => ({
      ...item,
      _ingested_at: new Date().toISOString(),
      _ingestion_source: 'servicetitan_v2_ref'
    }));
  }

  /**
   * Get BigQuery schema - must be implemented by child classes
   */
  getSchema() {
    throw new Error('getSchema() must be implemented by child class');
  }

  /**
   * Helper: Convert ServiceTitan date to ISO string
   */
  parseDate(dateString) {
    if (!dateString) return null;
    try {
      return new Date(dateString).toISOString();
    } catch (error) {
      this.log.warn('Date parsing failed', { dateString, error: error.message });
      return null;
    }
  }

  /**
   * Helper: Safe JSON stringify
   */
  toJson(obj) {
    if (!obj) return null;
    try {
      return JSON.stringify(obj);
    } catch (error) {
      this.log.warn('JSON stringify failed', { error: error.message });
      return null;
    }
  }
}

export default BaseRefIngestor;
