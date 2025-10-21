/**
 * Payroll Ingestor
 * Fetches payroll data from ServiceTitan Payroll API
 */

import { BaseIngestor } from './base_ingestor.js';

export class PayrollIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('payroll', stClient, bqClient, {
      tableId: 'raw_payroll',
      primaryKey: 'payrollId',  // Changed from 'id' to 'payrollId'
      partitionField: 'modifiedOn',
      clusterFields: ['employeeId', 'date'],  // Changed 'paidDate' to 'date'
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
    return data.map(item => ({
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
      createdOn: this.parseDate(item.createdOn),
      modifiedOn: this.parseDate(item.modifiedOn),
      _ingested_at: new Date().toISOString(),
      _ingestion_source: 'servicetitan_v2'
    }));
  }

  getSchema() {
    return [
      { name: 'payrollId', type: 'INT64', mode: 'REQUIRED' },
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
      { name: 'createdOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default PayrollIngestor;
