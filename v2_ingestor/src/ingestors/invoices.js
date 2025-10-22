/**
 * Invoices Ingestor
 * Fetches invoice data from ServiceTitan Accounting API
 */

import { BaseIngestor } from './base_ingestor.js';

export class InvoicesIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('invoices', stClient, bqClient, {
      tableId: 'raw_invoices',
      primaryKey: 'id',
      partitionField: 'modifiedOn',
      clusterFields: ['businessUnitId', 'jobId', 'status'],
      ...config
    });
  }

  async fetch(options = {}) {
    const mode = options.mode || process.env.SYNC_MODE || 'incremental';

    if (mode === 'full') {
      return await this.stClient.getInvoices();
    }

    const lastSync = await this.bqClient.getLastSyncTime(this.entityType);
    return await this.stClient.getInvoicesIncremental(lastSync);
  }

  async transform(data) {
    return data.map(invoice => ({
      id: invoice.id,
      syncStatus: invoice.syncStatus,
      summary: invoice.summary,
      referenceNumber: invoice.referenceNumber,
      invoiceDate: this.parseDate(invoice.invoiceDate),
      dueDate: this.parseDate(invoice.dueDate),
      subTotal: invoice.subTotal,
      salesTax: invoice.salesTax,
      total: invoice.total,
      balance: invoice.balance,
      invoiceTypeId: invoice.invoiceTypeId,
      jobId: invoice.job?.id,  // FIX: Extract job ID from nested job object
      projectId: invoice.projectId,
      businessUnitId: invoice.businessUnitId,
      locationId: invoice.locationId,
      customerId: invoice.customerId,
      depositedOn: this.parseDate(invoice.depositedOn),
      createdOn: this.parseDate(invoice.createdOn),
      modifiedOn: this.parseDate(invoice.modifiedOn),
      adjustmentToId: invoice.adjustmentToId,
      status: invoice.status,
      employeeId: invoice.employeeId,
      commissionEligibilityDate: this.parseDate(invoice.commissionEligibilityDate),
      items: this.toJson(invoice.items),
      customFields: this.toJson(invoice.customFields),
      _ingested_at: new Date().toISOString(),
      _ingestion_source: 'servicetitan_v2'
    }));
  }

  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },
      { name: 'syncStatus', type: 'STRING', mode: 'NULLABLE' },
      { name: 'summary', type: 'STRING', mode: 'NULLABLE' },
      { name: 'referenceNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'invoiceDate', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'dueDate', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'subTotal', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'salesTax', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'total', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'balance', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'invoiceTypeId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'jobId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'projectId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'businessUnitId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'locationId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'customerId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'depositedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'createdOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'adjustmentToId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'status', type: 'STRING', mode: 'NULLABLE' },
      { name: 'employeeId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'commissionEligibilityDate', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'items', type: 'JSON', mode: 'NULLABLE' },
      { name: 'customFields', type: 'JSON', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default InvoicesIngestor;
