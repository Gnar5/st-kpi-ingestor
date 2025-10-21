/**
 * Locations Ingestor
 * Fetches location data from ServiceTitan CRM API
 */

import { BaseIngestor } from './base_ingestor.js';

export class LocationsIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('locations', stClient, bqClient, {
      tableId: 'raw_locations',
      primaryKey: 'id',
      partitionField: 'modifiedOn',
      clusterFields: ['customerId', 'active'],
      ...config
    });
  }

  async fetch(options = {}) {
    const mode = options.mode || process.env.SYNC_MODE || 'incremental';

    if (mode === 'full') {
      return await this.stClient.getLocations();
    }

    const lastSync = await this.bqClient.getLastSyncTime(this.entityType);
    return await this.stClient.getLocationsIncremental(lastSync);
  }

  async transform(data) {
    return data.map(location => ({
      id: location.id,
      customerId: location.customerId,
      active: location.active,
      name: location.name,
      address: this.toJson(location.address),
      taxZoneId: location.taxZoneId,
      zoneId: location.zoneId,
      createdOn: this.parseDate(location.createdOn),
      modifiedOn: this.parseDate(location.modifiedOn),
      customFields: this.toJson(location.customFields),
      _ingested_at: new Date().toISOString(),
      _ingestion_source: 'servicetitan_v2'
    }));
  }

  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },
      { name: 'customerId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'active', type: 'BOOL', mode: 'NULLABLE' },
      { name: 'name', type: 'STRING', mode: 'NULLABLE' },
      { name: 'address', type: 'JSON', mode: 'NULLABLE' },
      { name: 'taxZoneId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'zoneId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'createdOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'customFields', type: 'JSON', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default LocationsIngestor;
