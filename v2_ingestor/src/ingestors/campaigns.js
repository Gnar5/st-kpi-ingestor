/**
 * Campaigns Ingestor
 * Fetches campaign data from ServiceTitan Marketing API
 */

import { BaseIngestor } from './base_ingestor.js';

export class CampaignsIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('campaigns', stClient, bqClient, {
      tableId: 'raw_campaigns',
      primaryKey: 'id',
      partitionField: 'modifiedOn',
      clusterFields: ['active', 'categoryId'],
      ...config
    });
  }

  async fetch(options = {}) {
    const mode = options.mode || process.env.SYNC_MODE || 'incremental';

    if (mode === 'full') {
      return await this.stClient.getCampaigns();
    }

    const lastSync = await this.bqClient.getLastSyncTime(this.entityType);
    return await this.stClient.getCampaignsIncremental(lastSync);
  }

  async transform(data) {
    return data.map(campaign => ({
      id: campaign.id,
      active: campaign.active,
      name: campaign.name,
      categoryId: campaign.category?.id || null,
      category: campaign.category?.name || null,
      createdOn: this.parseDate(campaign.createdOn),
      modifiedOn: this.parseDate(campaign.modifiedOn),
      _ingested_at: new Date().toISOString(),
      _ingestion_source: 'servicetitan_v2'
    }));
  }

  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },
      { name: 'active', type: 'BOOL', mode: 'NULLABLE' },
      { name: 'name', type: 'STRING', mode: 'NULLABLE' },
      { name: 'categoryId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'category', type: 'STRING', mode: 'NULLABLE' },
      { name: 'createdOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' }
    ];
  }
}

export default CampaignsIngestor;
