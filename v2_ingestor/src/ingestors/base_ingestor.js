/**
 * Base Ingestor Class
 * Provides common functionality for all entity ingestors
 */

import { logger } from '../utils/logger.js';
import { SchemaValidator } from '../utils/schema_validator.js';
import { randomUUID } from 'crypto';

export class BaseIngestor {
  constructor(entityType, stClient, bqClient, config = {}) {
    this.entityType = entityType;
    this.stClient = stClient;
    this.bqClient = bqClient;
    this.config = config;

    this.schemaValidator = new SchemaValidator();
    this.log = logger.child(`ingestor:${entityType}`);

    this.tableId = config.tableId || `raw_${entityType}`;
    this.primaryKey = config.primaryKey || 'id';
    this.partitionField = config.partitionField || 'modifiedOn';
    this.clusterFields = config.clusterFields || [];
  }

  /**
   * Main ingestion flow - override this in child classes if needed
   */
  async ingest(options = {}) {
    const runId = randomUUID();
    const startTime = new Date();

    this.log.info('Ingestion started', {
      runId,
      entityType: this.entityType,
      mode: options.mode || 'incremental'
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
      const data = await this.fetch(options);
      metadata.records_fetched = data.length;

      this.log.info('Data fetched', {
        runId,
        entityType: this.entityType,
        recordCount: data.length
      });

      if (data.length === 0) {
        this.log.info('No new records to process', { runId, entityType: this.entityType });
        metadata.status = 'success';
        metadata.end_time = new Date().toISOString();
        metadata.duration_ms = Date.now() - startTime.getTime();
        await this.bqClient.logRun(this.entityType, metadata);
        return { success: true, recordsProcessed: 0 };
      }

      // 2. Transform/normalize data
      const transformed = await this.transform(data);

      // 3. Validate schema
      if (transformed.length > 0) {
        const validation = this.schemaValidator.validate(this.entityType, transformed[0]);
        if (!validation.valid) {
          throw new Error(`Schema validation failed: ${validation.errors.join(', ')}`);
        }
      }

      // 4. Ensure table exists
      const schema = this.getSchema();
      await this.bqClient.ensureTable(
        this.bqClient.datasetRaw,
        this.tableId,
        schema,
        {
          partitionField: this.partitionField,
          clusterFields: this.clusterFields,
          updateSchema: true
        }
      );

      // 5. Load to BigQuery (upsert for idempotency)
      const result = await this.bqClient.upsert(
        this.bqClient.datasetRaw,
        this.tableId,
        transformed,
        this.primaryKey
      );

      metadata.records_inserted = result.merged;
      metadata.status = 'success';
      metadata.end_time = new Date().toISOString();
      metadata.duration_ms = Date.now() - startTime.getTime();

      // 6. Update sync state
      await this.bqClient.updateLastSyncTime(
        this.entityType,
        new Date().toISOString(),
        'success',
        result.merged
      );

      // 7. Log run
      await this.bqClient.logRun(this.entityType, metadata);

      this.log.info('Ingestion completed', {
        runId,
        entityType: this.entityType,
        recordsFetched: metadata.records_fetched,
        recordsInserted: metadata.records_inserted,
        durationMs: metadata.duration_ms
      });

      return {
        success: true,
        recordsProcessed: result.merged,
        runId,
        duration: metadata.duration_ms
      };
    } catch (error) {
      metadata.status = 'failed';
      metadata.end_time = new Date().toISOString();
      metadata.duration_ms = Date.now() - startTime.getTime();
      metadata.error_message = error.message;

      await this.bqClient.logRun(this.entityType, metadata);

      this.log.error('Ingestion failed', {
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
   */
  async transform(data) {
    // Default: add metadata fields
    return data.map(item => ({
      ...item,
      _ingested_at: new Date().toISOString(),
      _ingestion_source: 'servicetitan_v2'
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

  /**
   * Helper: Extract nested field safely
   */
  getNestedField(obj, path, defaultValue = null) {
    try {
      const value = path.split('.').reduce((acc, part) => acc?.[part], obj);
      return value !== undefined ? value : defaultValue;
    } catch (error) {
      return defaultValue;
    }
  }
}

export default BaseIngestor;
