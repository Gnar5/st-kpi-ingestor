/**
 * Collections Ingestor (Reporting API)
 * Fetches collections data from ServiceTitan Reporting API
 *
 * NOTE: Uses Reporting API instead of entity API because the Payments v2 entity API
 * does not return amount, invoiceId, or businessUnitId fields needed for collections tracking.
 *
 * Report ID: 26117979 (Collections Report)
 * Category: report-category/accounting
 *
 * Date Parameters:
 * - From: Start date (YYYY-MM-DD)
 * - To: End date (YYYY-MM-DD)
 * - DateType: 2 (payment date)
 */

import { BaseIngestor } from '../ingestors/base_ingestor.js';
import { randomUUID } from 'crypto';

export class CollectionsIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('collections', stClient, bqClient, {
      tableId: 'raw_collections',
      primaryKey: 'id',  // Composite key generated from payment details
      partitionField: 'payment_date',
      clusterFields: ['business_unit', 'job_id'],
      ...config
    });

    // Report configuration
    this.reportId = config.reportId || process.env.COLLECTIONS_REPORT_ID || '26117979';
    this.categoryPath = 'report-category/accounting';
  }

  /**
   * Fetch collections data from Reporting API
   * For reports, we typically fetch by date range rather than incremental sync
   */
  async fetch(options = {}) {
    const mode = options.mode || process.env.SYNC_MODE || 'incremental';

    let fromDate, toDate;

    if (mode === 'full') {
      // Full sync: get all historical data (or a reasonable range)
      // Default to last 2 years
      toDate = new Date();
      fromDate = new Date(toDate);
      fromDate.setFullYear(fromDate.getFullYear() - 2);
    } else {
      // Incremental: get last 30 days to catch any late updates
      toDate = new Date();
      fromDate = new Date(toDate);
      fromDate.setDate(fromDate.getDate() - 30);
    }

    // Allow override via options
    if (options.from) fromDate = new Date(options.from);
    if (options.to) toDate = new Date(options.to);

    const parameters = {
      From: fromDate.toISOString().slice(0, 10),
      To: toDate.toISOString().slice(0, 10),
      DateType: 2  // Payment date
    };

    this.log.info('Fetching collections report', {
      reportId: this.reportId,
      parameters
    });

    const result = await this.stClient.fetchReport(
      this.categoryPath,
      this.reportId,
      parameters
    );

    return result.items || [];
  }

  /**
   * Transform collections report data into table rows
   * Report format (array): [payment_date, customer_name, amount, job_id, ..., business_unit, ...]
   * Array indices based on actual API response:
   * [0] = payment_date
   * [1] = customer_name
   * [2] = amount
   * [3] = job_id
   * [7] = business_unit (bu_key)
   */
  async transform(data) {
    return data.map((row, index) => {
      // Collections report returns arrays, not objects
      const paymentDate = this.parseDate(row[0]);
      const amount = parseFloat(row[2] || 0);
      const jobId = row[3] ? String(row[3]) : null;
      const businessUnit = row[7] || '';

      // Create composite ID (handle case where parseDate returns null)
      const dateStr = paymentDate instanceof Date && !isNaN(paymentDate)
        ? paymentDate.toISOString()
        : 'null';
      const id = `${businessUnit}-${dateStr}-${amount}-${index}`;

      return {
        id,
        business_unit: businessUnit,
        payment_date: paymentDate,
        amount,
        job_id: jobId,
        _ingested_at: new Date().toISOString(),
        _ingestion_source: 'servicetitan_reporting_api',
        raw: this.toJson(row)  // Store raw array for debugging
      };
    });
  }

  getSchema() {
    return [
      { name: 'id', type: 'STRING', mode: 'REQUIRED' },
      { name: 'business_unit', type: 'STRING', mode: 'NULLABLE' },
      { name: 'payment_date', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'amount', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'job_id', type: 'INT64', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' },
      { name: 'raw', type: 'JSON', mode: 'NULLABLE' }
    ];
  }

  /**
   * Override ingest method to handle report-specific logic
   * Reports need special handling for date ranges and deduplication
   */
  async ingest(options = {}) {
    const startTime = Date.now();
    const runId = randomUUID();

    try {
      this.log.info('Collections ingestion started', {
        entityType: this.entityType,
        runId,
        mode: options.mode || 'incremental'
      });

      // Fetch report data
      const rawData = await this.fetch(options);
      this.log.info('Collections data fetched', {
        recordsFound: rawData.length
      });

      // Transform data
      const transformedData = await this.transform(rawData);
      this.log.info('Collections data transformed', {
        recordsTransformed: transformedData.length
      });

      // For reports, we typically truncate and reload for the date range
      // rather than upsert, to avoid duplicates
      if (transformedData.length > 0) {
        // Delete existing records for this date range
        const dates = transformedData.map(r => r.payment_date).filter(d => d);
        if (dates.length > 0) {
          const minDate = new Date(Math.min(...dates.map(d => d.getTime())));
          const maxDate = new Date(Math.max(...dates.map(d => d.getTime())));

          await this.truncateDateRange(minDate, maxDate);
        }

        // Load new data
        await this.load(transformedData);
      }

      const duration = Date.now() - startTime;

      // Log run
      await this.logRun({
        runId,
        status: 'success',
        recordsFetched: rawData.length,
        recordsInserted: transformedData.length,
        duration
      });

      return {
        recordsProcessed: transformedData.length,
        runId,
        duration
      };

    } catch (error) {
      const duration = Date.now() - startTime;

      this.log.error('Collections ingestion failed', {
        entityType: this.entityType,
        runId,
        error: error.message
      });

      await this.logRun({
        runId,
        status: 'error',
        errorMessage: error.message,
        duration
      });

      throw error;
    }
  }

  /**
   * Truncate data for a specific date range
   * Used before loading new report data to avoid duplicates
   */
  async truncateDateRange(fromDate, toDate) {
    const fromStr = fromDate.toISOString().slice(0, 10);
    const toStr = toDate.toISOString().slice(0, 10);

    this.log.info('Truncating collections date range', {
      from: fromStr,
      to: toStr
    });

    const query = `
      DELETE FROM \`${this.bqClient.projectId}.${this.bqClient.datasetRaw}.${this.tableId}\`
      WHERE DATE(payment_date) >= '${fromStr}'
        AND DATE(payment_date) <= '${toStr}'
    `;

    await this.bqClient.query(query);
  }
}

export default CollectionsIngestor;
