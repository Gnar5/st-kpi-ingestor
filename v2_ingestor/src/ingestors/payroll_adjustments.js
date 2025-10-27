/**
 * Payroll Adjustments Ingestor
 * Fetches payroll adjustments from ServiceTitan Payroll API
 * These are Direct Adjustments, Invoice Specific Bonuses, and other corrections
 * NOTE: Adjustments link to invoiceId, not jobId
 */

import crypto from 'crypto';
import { BaseIngestor } from './base_ingestor.js';

export class PayrollAdjustmentsIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('payroll_adjustments', stClient, bqClient, {
      tableId: 'raw_payroll_adjustments',
      primaryKey: 'id',
      partitionField: 'modifiedOn',
      clusterFields: ['employeeId', 'invoiceId', 'activityCodeId'],
      ...config
    });
  }

  async fetch(options = {}) {
    const mode = options.mode || process.env.SYNC_MODE || 'incremental';

    if (mode === 'full') {
      return await this.stClient.getPayrollAdjustments();
    }

    const lastSync = await this.bqClient.getLastSyncTime(this.entityType);
    return await this.stClient.getPayrollAdjustmentsIncremental(lastSync);
  }

  async transform(data) {
    return data.map((item, index) => {
      // Generate unique hash-based ID from all fields
      const uniqueId = this.generateUniqueId(item, index);

      return {
        id: uniqueId,  // Primary key (hash-based)
        adjustment_id: item.id,  // Original ST adjustment ID
        employeeId: item.employeeId,
        employeeType: item.employeeType?.name || null,
        postedOn: this.parseDate(item.postedOn),
        amount: item.amount,
        memo: item.memo,
        activityCodeId: item.activityCodeId,
        invoiceId: item.invoiceId,  // Links to invoice, NOT job
        hours: item.hours,
        rate: item.rate,
        createdOn: this.parseDate(item.createdOn),
        modifiedOn: this.parseDate(item.modifiedOn),
        active: item.active !== false,  // Default to true if not specified
        _ingested_at: new Date().toISOString(),
        _ingestion_source: 'servicetitan_v2'
      };
    });
  }

  /**
   * Generate unique ID using hash of all fields
   */
  generateUniqueId(item, index) {
    // Create a deterministic string from all fields that could differ
    const uniqueString = [
      item.id || '',
      item.employeeId || '',
      item.invoiceId || '',
      item.postedOn || '',
      item.amount || '',
      item.activityCodeId || '',
      item.hours || '',
      item.createdOn || '',
      index  // Fallback for truly identical records
    ].join('|');

    // Generate SHA256 hash and convert to BigInt
    const hash = crypto.createHash('sha256').update(uniqueString).digest('hex');

    // Take first 15 digits of hex as integer (fits in BigQuery INT64)
    const uniqueId = parseInt(hash.substring(0, 15), 16);

    return uniqueId;
  }

  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },  // Unique hash-based ID
      { name: 'adjustment_id', type: 'INT64', mode: 'NULLABLE' },  // Original ST ID
      { name: 'employeeId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'employeeType', type: 'STRING', mode: 'NULLABLE' },
      { name: 'postedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'amount', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'memo', type: 'STRING', mode: 'NULLABLE' },
      { name: 'activityCodeId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'invoiceId', type: 'INT64', mode: 'NULLABLE' },  // Links to invoice
      { name: 'hours', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'rate', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'createdOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'active', type: 'BOOLEAN', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default PayrollAdjustmentsIngestor;
