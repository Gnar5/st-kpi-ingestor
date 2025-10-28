/**
 * Payments Ingestor
 * Fetches payment data from ServiceTitan Accounting API
 *
 * IMPORTANT: Payments in ServiceTitan have a "splits" structure where one payment
 * can be split across multiple invoices. We flatten this by creating one row per split.
 *
 * API Response Structure:
 * {
 *   id: 123,
 *   typeId: 1,
 *   paidOn: "2025-08-18T...",
 *   status: "Posted",
 *   memo: "...",
 *   splits: [
 *     { invoiceId: 456, amount: 100.00 },
 *     { invoiceId: 789, amount: 50.00 }
 *   ]
 * }
 *
 * We transform this into multiple rows (one per split) for easier querying.
 */

import { BaseIngestor } from './base_ingestor.js';

export class PaymentsIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('payments', stClient, bqClient, {
      tableId: 'raw_payments',
      primaryKey: 'id', // Use id as primary key for BaseIngestor
      partitionField: 'paidOn',
      clusterFields: ['invoiceId', 'paymentTypeId', 'status'],
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
    const rows = [];

    for (const payment of data) {
      const splits = payment.splits || [];

      // If payment has no splits, create one row with NULL invoiceId
      if (splits.length === 0) {
        rows.push({
          id: `${payment.id}-0`,
          paymentId: payment.id,
          invoiceId: null,
          amount: null,
          paymentTypeId: payment.typeId,
          status: payment.status,
          memo: payment.memo,
          paidOn: this.parseDate(payment.paidOn),
          createdOn: this.parseDate(payment.createdOn),
          modifiedOn: this.parseDate(payment.modifiedOn),
          _ingested_at: new Date().toISOString(),
          _ingestion_source: 'servicetitan_v2'
        });
      } else {
        // Create one row per split
        splits.forEach((split, index) => {
          rows.push({
            id: `${payment.id}-${split.invoiceId || index}`,
            paymentId: payment.id,
            invoiceId: split.invoiceId,
            amount: split.amount,
            paymentTypeId: payment.typeId,
            status: payment.status,
            memo: payment.memo,
            paidOn: this.parseDate(payment.paidOn),
            createdOn: this.parseDate(payment.createdOn),
            modifiedOn: this.parseDate(payment.modifiedOn),
            _ingested_at: new Date().toISOString(),
            _ingestion_source: 'servicetitan_v2'
          });
        });
      }
    }

    return rows;
  }

  getSchema() {
    return [
      { name: 'id', type: 'STRING', mode: 'REQUIRED' },
      { name: 'paymentId', type: 'INT64', mode: 'REQUIRED' },
      { name: 'invoiceId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'amount', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'paymentTypeId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'status', type: 'STRING', mode: 'NULLABLE' },
      { name: 'memo', type: 'STRING', mode: 'NULLABLE' },
      { name: 'paidOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'createdOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default PaymentsIngestor;
