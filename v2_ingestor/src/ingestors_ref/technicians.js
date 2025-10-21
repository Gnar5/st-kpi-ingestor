/**
 * Technicians Reference Ingestor
 * Fetches technician/employee metadata from ServiceTitan Settings API
 *
 * Purpose: Provides human-readable technician names for joining with jobs, payroll, etc.
 * Usage: JOIN raw_jobs.technicianId = dim_technicians.id
 *
 * Refresh Strategy: Full refresh nightly (moderate size, semi-stable dataset)
 */

import { BaseRefIngestor } from './base_ref_ingestor.js';

export class TechniciansIngestor extends BaseRefIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('technicians', stClient, bqClient, {
      tableId: 'dim_technicians',
      primaryKey: 'id',
      partitionField: null,  // No partitioning for reference table
      clusterFields: ['active', 'businessUnitId'],
      refreshMode: 'full',
      ...config
    });
  }

  /**
   * Fetch technicians from ServiceTitan
   * Note: Fetches all technicians to ensure deleted/inactive ones are tracked
   */
  async fetch(options = {}) {
    this.log.info('Fetching all technicians from ServiceTitan');

    const allTechs = await this.stClient.getTechnicians({});

    this.log.info('Technicians fetched', {
      total: allTechs.length,
      active: allTechs.filter(t => t.active).length
    });

    return allTechs;
  }

  /**
   * Transform ServiceTitan technician data to BigQuery schema
   */
  async transform(data) {
    return data.map(tech => ({
      id: tech.id,
      name: tech.name,
      active: tech.active !== undefined ? tech.active : true,
      businessUnitId: tech.businessUnitId || null,
      businessUnitName: tech.businessUnitName || null,
      email: tech.email || null,
      phoneNumber: tech.phoneNumber || null,
      employeeId: tech.employeeId || null,
      role: tech.role || null,
      team: tech.team || null,
      modifiedOn: this.parseDate(tech.modifiedOn),
      _ingested_at: new Date().toISOString(),
      _ingestion_source: 'servicetitan_v2_ref'
    }));
  }

  /**
   * BigQuery schema for dim_technicians
   */
  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },
      { name: 'name', type: 'STRING', mode: 'NULLABLE' },
      { name: 'active', type: 'BOOL', mode: 'NULLABLE' },
      { name: 'businessUnitId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'businessUnitName', type: 'STRING', mode: 'NULLABLE' },
      { name: 'email', type: 'STRING', mode: 'NULLABLE' },
      { name: 'phoneNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'employeeId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'role', type: 'STRING', mode: 'NULLABLE' },
      { name: 'team', type: 'STRING', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default TechniciansIngestor;
