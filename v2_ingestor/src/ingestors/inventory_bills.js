/**
 * Inventory Bills Ingestor
 * Fetches inventory bill data from ServiceTitan Accounting API
 * Used for job costing - tracks billed material costs that may differ from PO amounts
 */

import crypto from 'crypto';
import { BaseIngestor } from './base_ingestor.js';

export class InventoryBillsIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('inventory_bills', stClient, bqClient, {
      tableId: 'raw_inventory_bills',
      primaryKey: 'id',
      partitionField: 'modifiedOn',
      clusterFields: ['vendorId', 'jobId', 'businessUnitId'],
      ...config
    });
  }

  async fetch(options = {}) {
    const mode = options.mode || process.env.SYNC_MODE || 'incremental';

    if (mode === 'full') {
      return await this.stClient.getInventoryBills();
    }

    const lastSync = await this.bqClient.getLastSyncTime(this.entityType);
    return await this.stClient.getInventoryBillsIncremental(lastSync);
  }

  async transform(data) {
    return data.map((bill, index) => {
      // Generate hash-based unique ID for line items
      // Bills may have multiple line items, so we need unique IDs
      const uniqueId = this.generateUniqueId(bill, index);

      return {
        id: uniqueId,
        inventoryBillId: bill.id,
        purchaseOrderId: bill.purchaseOrderId,
        syncStatus: bill.syncStatus,
        referenceNumber: bill.referenceNumber,
        vendorId: bill.vendor?.id,
        vendorNumber: bill.vendorNumber,
        vendorName: bill.vendor?.name,
        jobId: bill.jobId,
        jobNumber: bill.jobNumber,
        businessUnitId: bill.businessUnit?.id,
        businessUnitName: bill.businessUnit?.name,
        summary: bill.summary,
        billDate: this.parseDate(bill.billDate),
        dueDate: this.parseDate(bill.dueDate),
        billAmount: parseFloat(bill.billAmount) || 0,
        taxAmount: parseFloat(bill.taxAmount) || 0,
        shippingAmount: parseFloat(bill.shippingAmount) || 0,
        total: (parseFloat(bill.billAmount) || 0) + (parseFloat(bill.taxAmount) || 0) + (parseFloat(bill.shippingAmount) || 0),
        termName: bill.termName,
        shipToDescription: bill.shipToDescription,
        shipTo: this.toJson(bill.shipTo),
        batch: this.toJson(bill.batch),
        taxZone: this.toJson(bill.taxZone),
        items: this.toJson(bill.items),
        customFields: this.toJson(bill.customFields),
        createdBy: bill.createdBy,
        createdOn: this.parseDate(bill.createdOn),
        modifiedOn: this.parseDate(bill.modifiedOn),
        active: bill.active,
        _ingested_at: new Date().toISOString(),
        _ingestion_source: 'servicetitan_v2'
      };
    });
  }

  /**
   * Generate unique ID using hash of all fields
   * Bills may have multiple line items, so we hash all differentiating fields
   */
  generateUniqueId(bill, index) {
    const uniqueString = [
      bill.id || '',
      bill.purchaseOrderId || '',
      bill.referenceNumber || '',
      bill.jobId || '',
      bill.billDate || '',
      bill.billAmount || '',
      bill.createdOn || '',
      index
    ].join('|');

    const hash = crypto.createHash('sha256').update(uniqueString).digest('hex');
    const uniqueId = parseInt(hash.substring(0, 15), 16);

    return uniqueId;
  }

  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },  // Unique hash-based ID
      { name: 'inventoryBillId', type: 'INT64', mode: 'NULLABLE' },  // Original bill ID
      { name: 'purchaseOrderId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'syncStatus', type: 'STRING', mode: 'NULLABLE' },
      { name: 'referenceNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'vendorId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'vendorNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'vendorName', type: 'STRING', mode: 'NULLABLE' },
      { name: 'jobId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'jobNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'businessUnitId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'businessUnitName', type: 'STRING', mode: 'NULLABLE' },
      { name: 'summary', type: 'STRING', mode: 'NULLABLE' },
      { name: 'billDate', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'dueDate', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'billAmount', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'taxAmount', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'shippingAmount', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'total', type: 'FLOAT64', mode: 'NULLABLE' },  // Calculated: billAmount + tax + shipping
      { name: 'termName', type: 'STRING', mode: 'NULLABLE' },
      { name: 'shipToDescription', type: 'STRING', mode: 'NULLABLE' },
      { name: 'shipTo', type: 'JSON', mode: 'NULLABLE' },
      { name: 'batch', type: 'JSON', mode: 'NULLABLE' },
      { name: 'taxZone', type: 'JSON', mode: 'NULLABLE' },
      { name: 'items', type: 'JSON', mode: 'NULLABLE' },
      { name: 'customFields', type: 'JSON', mode: 'NULLABLE' },
      { name: 'createdBy', type: 'STRING', mode: 'NULLABLE' },
      { name: 'createdOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'active', type: 'BOOLEAN', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default InventoryBillsIngestor;
