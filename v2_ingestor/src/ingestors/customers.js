/**
 * Customers Ingestor
 * Fetches customer data from ServiceTitan CRM API
 */

import { BaseIngestor } from './base_ingestor.js';

export class CustomersIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('customers', stClient, bqClient, {
      tableId: 'raw_customers',
      primaryKey: 'id',
      partitionField: 'modifiedOn',
      clusterFields: ['type', 'active'],
      ...config
    });
  }

  async fetch(options = {}) {
    const mode = options.mode || process.env.SYNC_MODE || 'incremental';

    if (mode === 'full') {
      return await this.stClient.getCustomers();
    }

    const lastSync = await this.bqClient.getLastSyncTime(this.entityType);
    return await this.stClient.getCustomersIncremental(lastSync);
  }

  async transform(data) {
    return data.map(customer => ({
      id: customer.id,
      active: customer.active,
      name: customer.name,
      type: customer.type,
      address: this.toJson(customer.address),
      email: customer.email,
      phoneNumber: customer.phoneNumber,
      balance: customer.balance,
      customFields: this.toJson(customer.customFields),
      createdOn: this.parseDate(customer.createdOn),
      createdById: customer.createdById,
      modifiedOn: this.parseDate(customer.modifiedOn),
      mergedToId: customer.mergedToId,
      _ingested_at: new Date().toISOString(),
      _ingestion_source: 'servicetitan_v2'
    }));
  }

  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },
      { name: 'active', type: 'BOOL', mode: 'NULLABLE' },
      { name: 'name', type: 'STRING', mode: 'NULLABLE' },
      { name: 'type', type: 'STRING', mode: 'NULLABLE' },
      { name: 'address', type: 'JSON', mode: 'NULLABLE' },
      { name: 'email', type: 'STRING', mode: 'NULLABLE' },
      { name: 'phoneNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'balance', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'customFields', type: 'JSON', mode: 'NULLABLE' },
      { name: 'createdOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'createdById', type: 'INT64', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'mergedToId', type: 'INT64', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default CustomersIngestor;
