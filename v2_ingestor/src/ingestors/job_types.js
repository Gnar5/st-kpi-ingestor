/**
 * Job Types Ingestor (Reference Data)
 * Fetches job type definitions from ServiceTitan Settings API
 */

import { BaseIngestor } from './base_ingestor.js';

export class JobTypesIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('job_types', stClient, bqClient, {
      tableId: 'ref_job_types',
      primaryKey: 'id',
      partitionField: null, // Reference data doesn't need partitioning
      clusterFields: ['active', 'name'],
      ...config
    });
  }

  async fetch(options = {}) {
    // Reference data always does full sync
    // Try JPM endpoint instead of settings
    return await this.stClient.fetchAll('jpm/v2/tenant/{tenant}/job-types');
  }

  async transform(data) {
    return data.map(jobType => ({
      id: jobType.id,
      name: jobType.name,
      description: jobType.description || null,
      active: jobType.active !== undefined ? jobType.active : true,
      createdOn: this.parseDate(jobType.createdOn),
      modifiedOn: this.parseDate(jobType.modifiedOn),
      _ingested_at: new Date().toISOString(),
      _ingestion_source: 'servicetitan_v2_ref'
    }));
  }

  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },
      { name: 'name', type: 'STRING', mode: 'REQUIRED' },
      { name: 'description', type: 'STRING', mode: 'NULLABLE' },
      { name: 'active', type: 'BOOLEAN', mode: 'NULLABLE' },
      { name: 'createdOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }

  // Helper method from BaseIngestor
  parseDate(dateString) {
    if (!dateString) return null;
    try {
      const date = new Date(dateString);
      return isNaN(date.getTime()) ? null : date.toISOString();
    } catch {
      return null;
    }
  }

  toJson(obj) {
    if (!obj) return null;
    return JSON.stringify(obj);
  }
}

export default JobTypesIngestor;
