/**
 * Payments Ingestor
 * Fetches payment data from ServiceTitan Accounting API
 *
 * ACTUAL API Response Structure (per ServiceTitan docs):
 * {
 *   id: 777888999,
 *   invoiceId: 987654321,           // Direct field, not in splits array
 *   amount: 1356.25,                // Direct field, not in splits array
 *   paymentTypeId: 1,
 *   status: "Applied",
 *   memo: "Credit card payment",
 *   referenceNumber: "CH-20251022-001",
 *   unappliedAmount: 0.00,
 *   createdOn: "2025-10-22T10:00:00Z",
 *   modifiedOn: "2025-10-22T10:00:00Z",
 *   businessUnitId: 1,
 *   batchId: 12345
 * }
 *
 * NOTE: The OpenAPI spec shows a "splits" array structure, but the actual API
 * returns a flat structure with invoiceId and amount at the root level.
 */

import { BaseIngestor } from './base_ingestor.js';

export class PaymentsIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('payments', stClient, bqClient, {
      tableId: 'raw_payments',
      primaryKey: 'id',
      partitionField: 'createdOn',  // Use createdOn as partition field
      clusterFields: ['invoiceId', 'paymentTypeId', 'businessUnitId'],
      ...config
    });
  }

  async fetch(options = {}) {
    const mode = options.mode || process.env.SYNC_MODE || 'incremental';

    if (mode === 'full') {
      return await this.stClient.getPayments();
    }

    const lastSync = await this.bqClient.getLastSyncTime(this.entityType);
    return await this.stClient.getPaymentsIncremental(lastSync);
  }

  async transform(data) {
    return data.map(payment => ({
      id: payment.id,
      invoiceId: payment.invoiceId,
      amount: payment.amount,
      paymentTypeId: payment.paymentTypeId || payment.typeId,
      status: payment.status,
      memo: payment.memo,
      referenceNumber: payment.referenceNumber,
      unappliedAmount: payment.unappliedAmount,
      businessUnitId: payment.businessUnitId,
      batchId: payment.batchId,
      createdOn: this.parseDate(payment.createdOn),
      modifiedOn: this.parseDate(payment.modifiedOn),
      _ingested_at: new Date().toISOString(),
      _ingestion_source: 'servicetitan_v2'
    }));
  }

  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },
      { name: 'invoiceId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'amount', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'paymentTypeId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'status', type: 'STRING', mode: 'NULLABLE' },
      { name: 'memo', type: 'STRING', mode: 'NULLABLE' },
      { name: 'referenceNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'unappliedAmount', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'businessUnitId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'batchId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'createdOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default PaymentsIngestor;
