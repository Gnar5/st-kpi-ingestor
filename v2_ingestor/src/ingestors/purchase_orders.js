/**
 * Purchase Orders Ingestor
 * Fetches purchase order data from ServiceTitan Inventory API
 * Used for job costing - material costs for jobs
 */

import crypto from 'crypto';
import { BaseIngestor } from './base_ingestor.js';

export class PurchaseOrdersIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('purchase_orders', stClient, bqClient, {
      tableId: 'raw_purchase_orders',
      primaryKey: 'id',
      partitionField: 'modifiedOn',
      clusterFields: ['vendorId', 'jobId', 'status'],
      ...config
    });
  }

  async fetch(options = {}) {
    const mode = options.mode || process.env.SYNC_MODE || 'incremental';

    if (mode === 'full') {
      return await this.stClient.getPurchaseOrders();
    }

    const lastSync = await this.bqClient.getLastSyncTime(this.entityType);
    return await this.stClient.getPurchaseOrdersIncremental(lastSync);
  }

  async transform(data) {
    return data.map((po, index) => {
      // Generate hash-based unique ID for line items
      // Purchase orders may have multiple line items, so we need unique IDs
      const uniqueId = this.generateUniqueId(po, index);

      return {
        id: uniqueId,
        purchaseOrderId: po.id,
        number: po.number,
        syncStatus: po.syncStatus,
        status: po.status,
        vendorId: po.vendorId,
        vendorName: po.vendorName,
        jobId: po.jobId,
        jobNumber: po.jobNumber,
        businessUnitId: po.businessUnitId,
        date: this.parseDate(po.date),
        requiredOn: this.parseDate(po.requiredOn),
        sentOn: this.parseDate(po.sentOn),
        receivedOn: this.parseDate(po.receivedOn),
        total: po.total,
        tax: po.tax,
        shipping: po.shipping,
        discount: po.discount,
        subTotal: po.subTotal,
        items: this.toJson(po.items),
        memo: po.memo,
        shipToAddress: this.toJson(po.shipToAddress),
        customFields: this.toJson(po.customFields),
        createdOn: this.parseDate(po.createdOn),
        modifiedOn: this.parseDate(po.modifiedOn),
        _ingested_at: new Date().toISOString(),
        _ingestion_source: 'servicetitan_v2'
      };
    });
  }

  /**
   * Generate unique ID using hash of all fields
   * POs may have multiple line items, so we hash all differentiating fields
   */
  generateUniqueId(po, index) {
    const uniqueString = [
      po.id || '',
      po.number || '',
      po.vendorId || '',
      po.jobId || '',
      po.date || '',
      po.total || '',
      po.createdOn || '',
      index
    ].join('|');

    const hash = crypto.createHash('sha256').update(uniqueString).digest('hex');
    const uniqueId = parseInt(hash.substring(0, 15), 16);

    return uniqueId;
  }

  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },  // Unique hash-based ID
      { name: 'purchaseOrderId', type: 'INT64', mode: 'NULLABLE' },  // Original PO ID
      { name: 'number', type: 'STRING', mode: 'NULLABLE' },
      { name: 'syncStatus', type: 'STRING', mode: 'NULLABLE' },
      { name: 'status', type: 'STRING', mode: 'NULLABLE' },
      { name: 'vendorId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'vendorName', type: 'STRING', mode: 'NULLABLE' },
      { name: 'jobId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'jobNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'businessUnitId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'date', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'requiredOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'sentOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'receivedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'total', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'tax', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'shipping', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'discount', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'subTotal', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'items', type: 'JSON', mode: 'NULLABLE' },
      { name: 'memo', type: 'STRING', mode: 'NULLABLE' },
      { name: 'shipToAddress', type: 'JSON', mode: 'NULLABLE' },
      { name: 'customFields', type: 'JSON', mode: 'NULLABLE' },
      { name: 'createdOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default PurchaseOrdersIngestor;
