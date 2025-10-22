/**
 * BigQuery Client
 * Handles all BigQuery operations including table creation, MERGE upserts,
 * incremental state tracking, and partition management
 */

import { BigQuery } from '@google-cloud/bigquery';
import { logger } from '../utils/logger.js';
import { retryWithBackoff } from '../utils/backoff.js';

export class BigQueryClient {
  constructor(config = {}) {
    this.projectId = config.projectId || process.env.BQ_PROJECT_ID;
    this.datasetRaw = config.datasetRaw || process.env.BQ_DATASET_RAW || 'st_raw_v2';
    this.datasetStage = config.datasetStage || process.env.BQ_DATASET_STAGE || 'st_stage_v2';
    this.datasetMart = config.datasetMart || process.env.BQ_DATASET_MART || 'st_mart_v2';
    this.datasetLogs = config.datasetLogs || process.env.BQ_DATASET_LOGS || 'st_logs_v2';

    this.bq = new BigQuery({
      projectId: this.projectId
    });

    this.log = logger.child('bq-client');
  }

  /**
   * Ensure dataset exists
   */
  async ensureDataset(datasetId) {
    const dataset = this.bq.dataset(datasetId);

    try {
      const [exists] = await dataset.exists();

      if (!exists) {
        this.log.info('Creating dataset', { datasetId });
        await dataset.create({
          location: 'US',
          description: `ServiceTitan v2 Ingestor - ${datasetId}`
        });
        this.log.info('Dataset created', { datasetId });
      }
    } catch (error) {
      this.log.error('Dataset operation failed', {
        datasetId,
        error: error.message
      });
      throw error;
    }
  }

  /**
   * Ensure table exists with proper schema
   */
  async ensureTable(datasetId, tableId, schema, options = {}) {
    await this.ensureDataset(datasetId);

    const dataset = this.bq.dataset(datasetId);
    const table = dataset.table(tableId);

    try {
      const [exists] = await table.exists();

      if (!exists) {
        this.log.info('Creating table', { datasetId, tableId });

        const tableOptions = {
          schema,
          description: options.description || `ServiceTitan entity: ${tableId}`,
          labels: options.labels || { source: 'servicetitan_v2' }
        };

        // Add partitioning if specified
        if (options.partitionField) {
          tableOptions.timePartitioning = {
            type: 'DAY',
            field: options.partitionField
          };
          this.log.info('Table will be partitioned', {
            tableId,
            field: options.partitionField
          });
        }

        // Add clustering if specified
        if (options.clusterFields && options.clusterFields.length > 0) {
          tableOptions.clustering = {
            fields: options.clusterFields
          };
          this.log.info('Table will be clustered', {
            tableId,
            fields: options.clusterFields
          });
        }

        await table.create(tableOptions);
        this.log.info('Table created', { datasetId, tableId });
      } else {
        // Table exists - optionally update schema
        if (options.updateSchema) {
          await this.updateTableSchema(datasetId, tableId, schema);
        }
      }

      return table;
    } catch (error) {
      this.log.error('Table operation failed', {
        datasetId,
        tableId,
        error: error.message
      });
      throw error;
    }
  }

  /**
   * Update table schema (add new fields)
   */
  async updateTableSchema(datasetId, tableId, newSchema) {
    const dataset = this.bq.dataset(datasetId);
    const table = dataset.table(tableId);

    try {
      const [metadata] = await table.getMetadata();
      const currentSchema = metadata.schema.fields;

      // Merge schemas (only add new fields)
      const schemaMap = new Map(currentSchema.map(f => [f.name, f]));
      let hasChanges = false;

      for (const field of newSchema) {
        if (!schemaMap.has(field.name)) {
          currentSchema.push(field);
          hasChanges = true;
          this.log.info('Adding new field to schema', {
            tableId,
            field: field.name,
            type: field.type
          });
        }
      }

      if (hasChanges) {
        metadata.schema.fields = currentSchema;
        await table.setMetadata(metadata);
        this.log.info('Table schema updated', { datasetId, tableId });
      }
    } catch (error) {
      this.log.error('Schema update failed', {
        datasetId,
        tableId,
        error: error.message
      });
      throw error;
    }
  }

  /**
   * Insert rows into table (streaming insert)
   * Automatically batches large payloads to avoid 413 errors
   *
   * Two batching strategies:
   * 1. Row-based (default): Fixed number of rows per batch
   * 2. Byte-based: Dynamic batching based on JSON payload size
   *    - Use for entities with variable record sizes (estimates, invoices)
   *    - Prevents 413 errors when some records are very large
   */
  async insert(datasetId, tableId, rows, options = {}) {
    if (!rows || rows.length === 0) {
      this.log.debug('No rows to insert', { datasetId, tableId });
      return { inserted: 0 };
    }

    const dataset = this.bq.dataset(datasetId);
    const table = dataset.table(tableId);

    const useByteBatching = options.useByteBatching || false;
    const maxBytes = options.maxBytes || 8 * 1024 * 1024; // 8MB (2MB buffer from 10MB limit)
    const batchSize = options.batchSize || 1000;
    const totalRows = rows.length;

    // Helper function to insert a single batch
    const insertBatch = async (batch, batchNum, totalBatches) => {
      const batchBytes = Buffer.from(JSON.stringify(batch)).length;
      const batchMB = (batchBytes / (1024 * 1024)).toFixed(2);

      await retryWithBackoff(
        async () => {
          await table.insert(batch, {
            skipInvalidRows: options.skipInvalidRows || false,
            ignoreUnknownValues: options.ignoreUnknownValues || true
          });
        },
        {
          context: `BQ insert ${tableId} batch ${batchNum}/${totalBatches}`,
          maxRetries: 3
        }
      );

      this.log.debug('Batch inserted', {
        datasetId,
        tableId,
        batch: `${batchNum}/${totalBatches}`,
        batchRows: batch.length,
        batchMB
      });

      return batch.length;
    };

    // BYTE-BASED BATCHING (for entities with variable record sizes)
    if (useByteBatching) {
      this.log.info('Using byte-based batching', {
        datasetId,
        tableId,
        totalRows,
        maxMB: (maxBytes / (1024 * 1024)).toFixed(2)
      });

      const batches = [];
      let currentBatch = [];
      let currentSize = 0;

      for (const row of rows) {
        const rowJson = JSON.stringify(row);
        const rowSize = Buffer.from(rowJson).length;

        // If adding this row would exceed limit AND we have at least one row, start new batch
        if (currentSize + rowSize > maxBytes && currentBatch.length > 0) {
          batches.push([...currentBatch]);
          currentBatch = [row];
          currentSize = rowSize;
        } else {
          currentBatch.push(row);
          currentSize += rowSize;
        }
      }

      // Add final batch
      if (currentBatch.length > 0) {
        batches.push(currentBatch);
      }

      this.log.info('Byte-based batches created', {
        datasetId,
        tableId,
        totalBatches: batches.length,
        avgRowsPerBatch: Math.round(totalRows / batches.length)
      });

      let insertedCount = 0;

      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        try {
          const inserted = await insertBatch(batch, i + 1, batches.length);
          insertedCount += inserted;
        } catch (error) {
          this.log.error('Batch insert failed', {
            datasetId,
            tableId,
            batch: `${i + 1}/${batches.length}`,
            batchRows: batch.length,
            error: error.message
          });
          throw error;
        }
      }

      this.log.info('Byte-based batched insert complete', {
        datasetId,
        tableId,
        totalRows: insertedCount,
        totalBatches: batches.length
      });

      return { inserted: insertedCount };
    }

    // ROW-BASED BATCHING (original behavior)
    // If under batch size, insert directly
    if (totalRows <= batchSize) {
      try {
        await retryWithBackoff(
          async () => {
            await table.insert(rows, {
              skipInvalidRows: options.skipInvalidRows || false,
              ignoreUnknownValues: options.ignoreUnknownValues || true
            });
          },
          {
            context: `BQ insert ${tableId}`,
            maxRetries: 3
          }
        );

        this.log.info('Rows inserted', {
          datasetId,
          tableId,
          count: rows.length
        });

        return { inserted: rows.length };
      } catch (error) {
        this.log.error('Insert failed', {
          datasetId,
          tableId,
          rowCount: rows.length,
          error: error.message,
          errors: error.errors
        });
        throw error;
      }
    }

    // Batch insert for large datasets
    this.log.info('Using row-based batching', {
      datasetId,
      tableId,
      totalRows,
      batchSize,
      batches: Math.ceil(totalRows / batchSize)
    });

    let insertedCount = 0;
    const totalBatches = Math.ceil(totalRows / batchSize);

    for (let i = 0; i < totalRows; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      const batchNum = Math.floor(i / batchSize) + 1;

      try {
        const inserted = await insertBatch(batch, batchNum, totalBatches);
        insertedCount += inserted;
      } catch (error) {
        this.log.error('Batch insert failed', {
          datasetId,
          tableId,
          batch: `${batchNum}/${totalBatches}`,
          batchRows: batch.length,
          error: error.message
        });
        throw error;
      }
    }

    this.log.info('Row-based batched insert complete', {
      datasetId,
      tableId,
      totalRows: insertedCount
    });

    return { inserted: insertedCount };
  }

  /**
   * Upsert rows using MERGE statement (idempotent)
   */
  async upsert(datasetId, tableId, rows, primaryKey = 'id', options = {}) {
    if (!rows || rows.length === 0) {
      this.log.debug('No rows to upsert', { datasetId, tableId });
      return { merged: 0, inserted: 0, updated: 0 };
    }

    // First insert into temp table
    const tempTableId = `${tableId}_temp_${Date.now()}`;

    try {
      // Create temp table with same schema as target
      const dataset = this.bq.dataset(datasetId);
      const targetTable = dataset.table(tableId);
      const [metadata] = await targetTable.getMetadata();

      await this.ensureTable(
        datasetId,
        tempTableId,
        metadata.schema.fields,
        { description: 'Temporary table for MERGE operation' }
      );

      // Insert into temp table (pass through byte batching options)
      await this.insert(datasetId, tempTableId, rows, {
        skipInvalidRows: false,
        ignoreUnknownValues: true,
        useByteBatching: options.useByteBatching || false,
        maxBytes: options.maxBytes
      });

      // Build and execute MERGE query
      const mergeQuery = this.buildMergeQuery(
        datasetId,
        tableId,
        tempTableId,
        primaryKey,
        metadata.schema.fields,
        options
      );

      const [job] = await this.bq.createQueryJob({
        query: mergeQuery,
        location: 'US'
      });

      const [result] = await job.getQueryResults();

      this.log.info('Merge complete', {
        datasetId,
        tableId,
        rowsProcessed: rows.length,
        primaryKey
      });

      // Clean up temp table
      await dataset.table(tempTableId).delete();

      return {
        merged: rows.length,
        inserted: result.numDmlAffectedRows || 0
      };
    } catch (error) {
      // Check if this is a streaming buffer conflict
      if (error.message && error.message.includes('streaming buffer')) {
        this.log.warn('Streaming buffer conflict detected, falling back to direct insert', {
          datasetId,
          tableId
        });

        // Clean up temp table
        try {
          await this.bq.dataset(datasetId).table(tempTableId).delete();
        } catch (cleanupError) {
          // Ignore cleanup errors
        }

        // Fall back to direct insert (will create duplicates if run twice)
        await this.insert(datasetId, tableId, rows, {
          skipInvalidRows: false,
          ignoreUnknownValues: true
        });

        return {
          merged: rows.length,
          inserted: rows.length,
          fallback: true
        };
      }

      this.log.error('Upsert failed', {
        datasetId,
        tableId,
        primaryKey,
        rowCount: rows.length,
        error: error.message
      });

      // Attempt cleanup
      try {
        await this.bq.dataset(datasetId).table(tempTableId).delete();
      } catch (cleanupError) {
        this.log.warn('Temp table cleanup failed', {
          tempTableId,
          error: cleanupError.message
        });
      }

      throw error;
    }
  }

  /**
   * Build MERGE SQL statement
   */
  buildMergeQuery(datasetId, targetTable, sourceTable, primaryKey, schema, options = {}) {
    const fullTargetTable = `\`${this.projectId}.${datasetId}.${targetTable}\``;
    const fullSourceTable = `\`${this.projectId}.${datasetId}.${sourceTable}\``;

    // Get all field names
    const fields = schema.map(f => f.name);

    // Build field list for INSERT
    const fieldList = fields.join(', ');

    // Build field list for UPDATE (exclude primary key)
    const updateFields = fields
      .filter(f => f !== primaryKey)
      .map(f => `target.${f} = source.${f}`)
      .join(',\n    ');

    // Build source field list for INSERT VALUES
    const sourceFieldList = fields.map(f => `source.${f}`).join(', ');

    const mergeQuery = `
MERGE ${fullTargetTable} AS target
USING ${fullSourceTable} AS source
ON target.${primaryKey} = source.${primaryKey}
WHEN MATCHED THEN
  UPDATE SET
    ${updateFields}
WHEN NOT MATCHED THEN
  INSERT (${fieldList})
  VALUES (${sourceFieldList})
    `.trim();

    return mergeQuery;
  }

  /**
   * Execute arbitrary query
   */
  async query(sql, options = {}) {
    try {
      const [job] = await this.bq.createQueryJob({
        query: sql,
        location: options.location || 'US',
        params: options.params || [],
        useLegacySql: false
      });

      this.log.debug('Query job created', { jobId: job.id });

      const [rows] = await job.getQueryResults();

      this.log.info('Query complete', {
        jobId: job.id,
        rowCount: rows.length
      });

      return rows;
    } catch (error) {
      this.log.error('Query failed', {
        sql: sql.substring(0, 200),
        error: error.message
      });
      throw error;
    }
  }

  /**
   * Get last sync timestamp for an entity
   */
  async getLastSyncTime(entityType) {
    const stateTableId = 'sync_state';

    try {
      await this.ensureTable(
        this.datasetLogs,
        stateTableId,
        [
          { name: 'entity_type', type: 'STRING', mode: 'REQUIRED' },
          { name: 'last_sync_time', type: 'TIMESTAMP', mode: 'REQUIRED' },
          { name: 'last_sync_status', type: 'STRING', mode: 'NULLABLE' },
          { name: 'records_processed', type: 'INT64', mode: 'NULLABLE' },
          { name: 'updated_at', type: 'TIMESTAMP', mode: 'REQUIRED' }
        ],
        { description: 'Sync state tracking for incremental loads' }
      );

      const query = `
        SELECT last_sync_time
        FROM \`${this.projectId}.${this.datasetLogs}.${stateTableId}\`
        WHERE entity_type = '${entityType}'
        ORDER BY updated_at DESC
        LIMIT 1
      `;

      const rows = await this.query(query);

      if (rows.length > 0) {
        const lastSync = rows[0].last_sync_time.value;
        this.log.info('Last sync time retrieved', {
          entityType,
          lastSync
        });
        return lastSync;
      }

      // Default: look back 7 days for first sync
      const lookbackDays = parseInt(process.env.LOOKBACK_DAYS) || 7;
      const defaultSync = new Date(Date.now() - lookbackDays * 24 * 60 * 60 * 1000);

      this.log.info('No previous sync found, using default lookback', {
        entityType,
        lookbackDays,
        defaultSync: defaultSync.toISOString()
      });

      return defaultSync.toISOString();
    } catch (error) {
      this.log.error('Failed to get last sync time', {
        entityType,
        error: error.message
      });
      throw error;
    }
  }

  /**
   * Update last sync timestamp for an entity
   */
  async updateLastSyncTime(entityType, syncTime, status = 'success', recordsProcessed = 0) {
    const stateTableId = 'sync_state';

    try {
      const row = {
        entity_type: entityType,
        last_sync_time: syncTime,
        last_sync_status: status,
        records_processed: recordsProcessed,
        updated_at: new Date().toISOString()
      };

      await this.insert(this.datasetLogs, stateTableId, [row]);

      this.log.info('Sync state updated', {
        entityType,
        syncTime,
        status,
        recordsProcessed
      });
    } catch (error) {
      this.log.error('Failed to update sync state', {
        entityType,
        error: error.message
      });
      // Don't throw - this is not critical
    }
  }

  /**
   * Log ingestion run metadata
   */
  async logRun(entityType, metadata) {
    const logsTableId = 'ingestion_logs';

    try {
      await this.ensureTable(
        this.datasetLogs,
        logsTableId,
        [
          { name: 'entity_type', type: 'STRING', mode: 'REQUIRED' },
          { name: 'run_id', type: 'STRING', mode: 'REQUIRED' },
          { name: 'start_time', type: 'TIMESTAMP', mode: 'REQUIRED' },
          { name: 'end_time', type: 'TIMESTAMP', mode: 'NULLABLE' },
          { name: 'status', type: 'STRING', mode: 'REQUIRED' },
          { name: 'records_fetched', type: 'INT64', mode: 'NULLABLE' },
          { name: 'records_inserted', type: 'INT64', mode: 'NULLABLE' },
          { name: 'duration_ms', type: 'INT64', mode: 'NULLABLE' },
          { name: 'error_message', type: 'STRING', mode: 'NULLABLE' },
          { name: 'metadata', type: 'JSON', mode: 'NULLABLE' }
        ],
        {
          description: 'Ingestion run logs',
          partitionField: 'start_time',
          clusterFields: ['entity_type', 'status']
        }
      );

      await this.insert(this.datasetLogs, logsTableId, [metadata]);

      this.log.info('Run logged', { entityType, runId: metadata.run_id });
    } catch (error) {
      this.log.error('Failed to log run', {
        entityType,
        error: error.message
      });
      // Don't throw - logging failure shouldn't break the pipeline
    }
  }

  /**
   * Batch insert with automatic chunking
   */
  async batchInsert(datasetId, tableId, rows, options = {}) {
    const chunkSize = options.chunkSize || 10000;
    const chunks = this.chunkArray(rows, chunkSize);
    let totalInserted = 0;

    this.log.info('Starting batch insert', {
      datasetId,
      tableId,
      totalRows: rows.length,
      chunks: chunks.length,
      chunkSize
    });

    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i];

      this.log.debug('Inserting chunk', {
        tableId,
        chunkIndex: i + 1,
        totalChunks: chunks.length,
        chunkSize: chunk.length
      });

      const result = await this.insert(datasetId, tableId, chunk, options);
      totalInserted += result.inserted;
    }

    this.log.info('Batch insert complete', {
      datasetId,
      tableId,
      totalInserted
    });

    return { inserted: totalInserted };
  }

  /**
   * Helper: chunk array
   */
  chunkArray(array, size) {
    const chunks = [];
    for (let i = 0; i < array.length; i += size) {
      chunks.push(array.slice(i, i + size));
    }
    return chunks;
  }
}

export default BigQueryClient;
