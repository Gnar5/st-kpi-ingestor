/**
 * Jobs Ingestor
 * Fetches job data from ServiceTitan JPM API
 */

import { BaseIngestor } from './base_ingestor.js';

export class JobsIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('jobs', stClient, bqClient, {
      tableId: 'raw_jobs',
      primaryKey: 'id',
      partitionField: 'modifiedOn',
      clusterFields: ['businessUnitId', 'jobStatus'],
      ...config
    });
  }

  async fetch(options = {}) {
    const mode = options.mode || process.env.SYNC_MODE || 'incremental';

    if (mode === 'full') {
      this.log.info('Performing full sync');
      return await this.stClient.getJobs();
    }

    // Incremental sync
    const lastSync = await this.bqClient.getLastSyncTime(this.entityType);
    this.log.info('Performing incremental sync', { since: lastSync });

    return await this.stClient.getJobsIncremental(lastSync);
  }

  async transform(data) {
    return data.map(job => ({
      id: job.id,
      jobNumber: job.jobNumber,
      projectId: job.projectId,
      customerId: job.customerId,
      locationId: job.locationId,
      jobStatus: job.jobStatus,
      completedOn: this.parseDate(job.completedOn),
      businessUnitId: job.businessUnitId,
      jobTypeId: job.jobTypeId,
      priority: job.priority,
      campaignId: job.campaignId,
      summary: job.summary,
      customFields: this.toJson(job.customFields),
      createdOn: this.parseDate(job.createdOn),
      createdById: job.createdById,
      modifiedOn: this.parseDate(job.modifiedOn),
      tagTypeIds: this.toJson(job.tagTypeIds),
      leadCallId: job.leadCallId,
      bookingId: job.bookingId,
      soldById: job.soldById,
      _ingested_at: new Date().toISOString(),
      _ingestion_source: 'servicetitan_v2'
    }));
  }

  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },
      { name: 'jobNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'projectId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'customerId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'locationId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'jobStatus', type: 'STRING', mode: 'NULLABLE' },
      { name: 'completedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'businessUnitId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'jobTypeId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'priority', type: 'STRING', mode: 'NULLABLE' },
      { name: 'campaignId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'summary', type: 'STRING', mode: 'NULLABLE' },
      { name: 'customFields', type: 'JSON', mode: 'NULLABLE' },
      { name: 'createdOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'createdById', type: 'INT64', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'tagTypeIds', type: 'JSON', mode: 'NULLABLE' },
      { name: 'leadCallId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'bookingId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'soldById', type: 'INT64', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default JobsIngestor;
