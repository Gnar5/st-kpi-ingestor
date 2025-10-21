/**
 * Activity Codes Reference Ingestor
 * Fetches activity code metadata from ServiceTitan Settings API
 *
 * Purpose: Provides human-readable activity names for payroll/timesheet data
 * Usage: JOIN raw_payroll.activityCodeId = dim_activity_codes.id
 *
 * Refresh Strategy: Full refresh nightly (small, stable dataset)
 *
 * Note: Activity codes describe what technicians were doing during their paid time
 * (e.g., "Working", "Idle", "Travel", "Training", etc.)
 */

import { BaseRefIngestor } from './base_ref_ingestor.js';

export class ActivityCodesIngestor extends BaseRefIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('activity_codes', stClient, bqClient, {
      tableId: 'dim_activity_codes',
      primaryKey: 'id',
      partitionField: null,  // No partitioning needed
      clusterFields: ['active', 'name'],
      refreshMode: 'full',
      ...config
    });
  }

  /**
   * Fetch activity codes from ServiceTitan
   */
  async fetch(options = {}) {
    this.log.info('Fetching all activity codes from ServiceTitan');

    const allCodes = await this.stClient.getActivityCodes({});

    this.log.info('Activity codes fetched', {
      total: allCodes.length,
      active: allCodes.filter(c => c.active).length
    });

    return allCodes;
  }

  /**
   * Transform ServiceTitan activity code data to BigQuery schema
   */
  async transform(data) {
    return data.map(code => ({
      id: code.id,
      name: code.name,
      active: code.active !== undefined ? code.active : true,
      description: code.description || null,
      code: code.code || null,
      isPaid: code.isPaid !== undefined ? code.isPaid : null,
      modifiedOn: this.parseDate(code.modifiedOn),
      _ingested_at: new Date().toISOString(),
      _ingestion_source: 'servicetitan_v2_ref'
    }));
  }

  /**
   * BigQuery schema for dim_activity_codes
   */
  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },
      { name: 'name', type: 'STRING', mode: 'NULLABLE' },
      { name: 'active', type: 'BOOL', mode: 'NULLABLE' },
      { name: 'description', type: 'STRING', mode: 'NULLABLE' },
      { name: 'code', type: 'STRING', mode: 'NULLABLE' },
      { name: 'isPaid', type: 'BOOL', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default ActivityCodesIngestor;
