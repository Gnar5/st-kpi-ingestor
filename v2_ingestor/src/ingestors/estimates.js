/**
 * Estimates Ingestor
 * Fetches estimate data from ServiceTitan Sales API
 */

import { BaseIngestor } from './base_ingestor.js';

export class EstimatesIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('estimates', stClient, bqClient, {
      tableId: 'raw_estimates',
      primaryKey: 'id',
      partitionField: 'createdOn',  // Partition by creation date (more stable than modifiedOn)
      clusterFields: ['businessUnitId', 'jobId', 'status'],
      ...config
    });
  }

  async fetch(options = {}) {
    const mode = options.mode || process.env.SYNC_MODE || 'incremental';

    if (mode === 'full') {
      return await this.stClient.getEstimates();
    }

    // Incremental sync with lookback window
    // API filters by BOTH createdOnOrAfter AND modifiedOnOrAfter
    // This catches estimates created or modified recently, but misses estimates
    // that were sold recently but haven't been modified (e.g., old estimate gets sold)
    const lastSync = await this.bqClient.getLastSyncTime(this.entityType);
    const lookbackHours = parseInt(process.env.INCREMENTAL_LOOKBACK_HOURS) || 4320; // 180 days default (increased from 7)
    const lookbackDate = new Date(new Date(lastSync).getTime() - (lookbackHours * 60 * 60 * 1000));

    this.log.info('Performing incremental sync with lookback', {
      lastSync,
      lookbackDate: lookbackDate.toISOString(),
      lookbackHours,
      lookbackDays: Math.round(lookbackHours / 24)
    });

    return await this.stClient.getEstimatesIncremental(lookbackDate.toISOString());
  }

  async transform(data) {
    return data.map(estimate => ({
      id: estimate.id,
      jobId: estimate.jobId,
      projectId: estimate.projectId,
      locationId: estimate.locationId,
      customerId: estimate.customerId,
      name: estimate.name,
      jobNumber: estimate.jobNumber,
      status: estimate.status?.name || null,  // Extract status name from object
      summary: estimate.summary,
      createdOn: this.parseDate(estimate.createdOn),
      modifiedOn: this.parseDate(estimate.modifiedOn),
      soldOn: this.parseDate(estimate.soldOn),
      soldById: estimate.soldBy || null,  // API uses 'soldBy' not 'soldById'
      estimateNumber: estimate.estimateNumber,
      businessUnitId: estimate.businessUnitId,
      // items field removed to reduce data size - not needed for KPI calculations
      subtotal: estimate.subtotal,
      totalTax: estimate.totalTax,
      total: estimate.total,
      _ingested_at: new Date().toISOString(),
      _ingestion_source: 'servicetitan_v2'
    }));
  }

  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },
      { name: 'jobId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'projectId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'locationId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'customerId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'name', type: 'STRING', mode: 'NULLABLE' },
      { name: 'jobNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'status', type: 'STRING', mode: 'NULLABLE' },
      { name: 'summary', type: 'STRING', mode: 'NULLABLE' },
      { name: 'createdOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'soldOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'soldById', type: 'INT64', mode: 'NULLABLE' },
      { name: 'estimateNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'businessUnitId', type: 'INT64', mode: 'NULLABLE' },
      // items field removed to reduce data size - not needed for KPI calculations
      { name: 'subtotal', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'totalTax', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'total', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default EstimatesIngestor;
