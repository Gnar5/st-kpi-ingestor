/**
 * Business Units Reference Ingestor
 * Fetches organizational business unit metadata from ServiceTitan Settings API
 *
 * Purpose: Provides human-readable business unit names for joining with fact tables
 * Usage: JOIN raw_invoices.businessUnitId = dim_business_units.id
 *
 * Refresh Strategy: Full refresh nightly (small, stable dataset)
 */

import { BaseRefIngestor } from './base_ref_ingestor.js';

export class BusinessUnitsIngestor extends BaseRefIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('business_units', stClient, bqClient, {
      tableId: 'dim_business_units',
      primaryKey: 'id',
      partitionField: null,  // No partitioning needed for small reference table
      clusterFields: ['active', 'name'],
      refreshMode: 'full',  // Always full refresh
      ...config
    });
  }

  /**
   * Fetch business units from ServiceTitan
   * Note: Business units API doesn't support modifiedSince, so always fetch all
   */
  async fetch(options = {}) {
    this.log.info('Fetching all business units from ServiceTitan');

    // Fetch active business units
    // Note: The API may support ?active=true filter, but fetching all ensures completeness
    const allUnits = await this.stClient.getBusinessUnits({});

    this.log.info('Business units fetched', {
      total: allUnits.length,
      active: allUnits.filter(u => u.active).length
    });

    return allUnits;
  }

  /**
   * Transform ServiceTitan business unit data to BigQuery schema
   */
  async transform(data) {
    return data.map(unit => ({
      id: unit.id,
      name: unit.name,
      active: unit.active !== undefined ? unit.active : true,
      officialName: unit.officialName || null,
      phoneNumber: unit.phoneNumber || null,
      email: unit.email || null,
      address: this.toJson(unit.address),
      timezone: unit.timezone?.name || null,
      _ingested_at: new Date().toISOString(),
      _ingestion_source: 'servicetitan_v2_ref'
    }));
  }

  /**
   * BigQuery schema for dim_business_units
   */
  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },
      { name: 'name', type: 'STRING', mode: 'NULLABLE' },
      { name: 'active', type: 'BOOL', mode: 'NULLABLE' },
      { name: 'officialName', type: 'STRING', mode: 'NULLABLE' },
      { name: 'phoneNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'email', type: 'STRING', mode: 'NULLABLE' },
      { name: 'address', type: 'JSON', mode: 'NULLABLE' },
      { name: 'timezone', type: 'STRING', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default BusinessUnitsIngestor;
