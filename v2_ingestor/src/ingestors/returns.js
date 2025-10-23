/**
 * Returns Ingestor
 * Fetches inventory returns data from ServiceTitan Inventory API
 * Used for job costing - material returns/credits that offset job costs
 */

import crypto from 'crypto';
import { BaseIngestor } from './base_ingestor.js';

export class ReturnsIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('returns', stClient, bqClient, {
      tableId: 'raw_returns',
      primaryKey: 'id',
      partitionField: 'modifiedOn',
      clusterFields: ['vendorId', 'jobId', 'returnDate'],
      ...config
    });
  }

  async fetch(options = {}) {
    const mode = options.mode || process.env.SYNC_MODE || 'incremental';

    if (mode === 'full') {
      return await this.stClient.getReturns();
    }

    const lastSync = await this.bqClient.getLastSyncTime(this.entityType);
    return await this.stClient.getReturnsIncremental(lastSync);
  }

  async transform(data) {
    return data.map((ret, index) => {
      // Generate hash-based unique ID
      // Returns may have multiple line items, so we need unique IDs
      const uniqueId = this.generateUniqueId(ret, index);

      return {
        id: uniqueId,
        returnId: ret.id,
        number: ret.number,
        syncStatus: ret.syncStatus,
        status: ret.status,
        vendorId: ret.vendorId,
        vendorName: ret.vendorName,
        jobId: ret.jobId,
        jobNumber: ret.jobNumber,
        businessUnitId: ret.businessUnitId,
        returnDate: this.parseDate(ret.returnDate),
        total: ret.total,
        tax: ret.tax,
        shipping: ret.shipping,
        subTotal: ret.subTotal,
        items: this.toJson(ret.items),
        memo: ret.memo,
        purchaseOrderId: ret.purchaseOrderId,
        purchaseOrderNumber: ret.purchaseOrderNumber,
        customFields: this.toJson(ret.customFields),
        createdOn: this.parseDate(ret.createdOn),
        modifiedOn: this.parseDate(ret.modifiedOn),
        _ingested_at: new Date().toISOString(),
        _ingestion_source: 'servicetitan_v2'
      };
    });
  }

  /**
   * Generate unique ID using hash of all fields
   * Returns may have multiple line items, so we hash all differentiating fields
   */
  generateUniqueId(ret, index) {
    const uniqueString = [
      ret.id || '',
      ret.number || '',
      ret.vendorId || '',
      ret.jobId || '',
      ret.returnDate || '',
      ret.total || '',
      ret.createdOn || '',
      index
    ].join('|');

    const hash = crypto.createHash('sha256').update(uniqueString).digest('hex');
    const uniqueId = parseInt(hash.substring(0, 15), 16);

    return uniqueId;
  }

  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },  // Unique hash-based ID
      { name: 'returnId', type: 'INT64', mode: 'NULLABLE' },  // Original return ID
      { name: 'number', type: 'STRING', mode: 'NULLABLE' },
      { name: 'syncStatus', type: 'STRING', mode: 'NULLABLE' },
      { name: 'status', type: 'STRING', mode: 'NULLABLE' },
      { name: 'vendorId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'vendorName', type: 'STRING', mode: 'NULLABLE' },
      { name: 'jobId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'jobNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'businessUnitId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'returnDate', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'total', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'tax', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'shipping', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'subTotal', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'items', type: 'JSON', mode: 'NULLABLE' },
      { name: 'memo', type: 'STRING', mode: 'NULLABLE' },
      { name: 'purchaseOrderId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'purchaseOrderNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'customFields', type: 'JSON', mode: 'NULLABLE' },
      { name: 'createdOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default ReturnsIngestor;
