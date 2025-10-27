/**
 * Payroll Ingestor
 * Fetches payroll data from ServiceTitan Payroll API
 */

import crypto from 'crypto';
import { BaseIngestor } from './base_ingestor.js';

export class PayrollIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('payroll', stClient, bqClient, {
      tableId: 'raw_payroll',
      primaryKey: 'id',  // Use sourceEntityId as primary key
      partitionField: 'modifiedOn',
      clusterFields: ['employeeId', 'jobId', 'date'],
      ...config
    });
  }

  async fetch(options = {}) {
    const mode = options.mode || process.env.SYNC_MODE || 'incremental';

    if (mode === 'full') {
      return await this.stClient.getPayroll();
    }

    const lastSync = await this.bqClient.getLastSyncTime(this.entityType);
    return await this.stClient.getPayrollIncremental(lastSync);
  }

  async transform(data) {
    return data.map((item, index) => {
      // Generate unique hash-based ID from all fields
      // sourceEntityId is NOT unique - multiple line items can share same sourceEntityId
      // We need to hash all differentiating fields to ensure uniqueness
      const uniqueId = this.generateUniqueId(item, index);

      return {
        id: uniqueId,  // Primary key (hash-based)
        payrollId: item.payrollId,
        employeeId: item.employeeId,
        employeeType: item.employeeType,
        businessUnitName: item.businessUnitName,
        date: this.parseDate(item.date),
        activity: item.activity,
        amount: item.amount,
        paidDurationHours: item.paidDurationHours,
        paidTimeType: item.paidTimeType,
        jobId: item.jobId,
        jobNumber: item.jobNumber,
        invoiceId: item.invoiceId,
        invoiceNumber: item.invoiceNumber,
        customerId: item.customerId,
        locationId: item.locationId,
        sourceEntityId: item.sourceEntityId,  // Store this too
        createdOn: this.parseDate(item.createdOn),
        modifiedOn: this.parseDate(item.modifiedOn),
        _ingested_at: new Date().toISOString(),
        _ingestion_source: 'servicetitan_v2'
      };
    });
  }

  /**
   * Generate unique ID using hash of all fields
   * sourceEntityId is NOT unique, so we hash all differentiating fields
   * IMPORTANT: Do NOT include index - must be deterministic across syncs
   */
  generateUniqueId(item, index) {
    // Use sourceEntityId if available (preferred unique key from ServiceTitan)
    if (item.sourceEntityId) {
      return item.sourceEntityId;
    }

    // Otherwise create deterministic hash from all fields
    // DO NOT include index - it changes between syncs and causes duplicates
    const uniqueString = [
      item.payrollId || '',
      item.employeeId || '',
      item.jobId || '',
      item.date || '',
      item.activity || '',
      item.amount || '',
      item.paidDurationHours || '',
      item.invoiceId || '',
      item.createdOn || ''
    ].join('|');

    // Generate SHA256 hash and convert to BigInt
    const hash = crypto.createHash('sha256').update(uniqueString).digest('hex');

    // Take first 15 digits of hex as integer (fits in BigQuery INT64)
    // This gives us 60 bits of uniqueness (2^60 = 1.15 quintillion possible values)
    const uniqueId = parseInt(hash.substring(0, 15), 16);

    return uniqueId;
  }

  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },  // Unique hash-based ID
      { name: 'payrollId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'employeeId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'employeeType', type: 'STRING', mode: 'NULLABLE' },
      { name: 'businessUnitName', type: 'STRING', mode: 'NULLABLE' },
      { name: 'date', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'activity', type: 'STRING', mode: 'NULLABLE' },
      { name: 'amount', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'paidDurationHours', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'paidTimeType', type: 'STRING', mode: 'NULLABLE' },
      { name: 'jobId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'jobNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'invoiceId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'invoiceNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'customerId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'locationId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'sourceEntityId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'createdOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default PayrollIngestor;
