/**
 * Appointments Ingestor
 * Fetches appointment/scheduling data from ServiceTitan JPM API
 * Used for determining "job start date" for Dollars Produced calculations
 */

import { BaseIngestor } from './base_ingestor.js';

export class AppointmentsIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('appointments', stClient, bqClient, {
      tableId: 'raw_appointments',
      primaryKey: 'id',
      partitionField: 'modifiedOn',
      clusterFields: ['jobId', 'status'],
      ...config
    });
  }

  async fetch(options = {}) {
    const mode = options.mode || process.env.SYNC_MODE || 'incremental';

    if (mode === 'full') {
      this.log.info('Performing full sync of appointments');
      const params = {
        startsOnOrAfter: options.startDate || '2020-01-01T00:00:00Z',
        active: 'Any'
      };
      if (options.endDate) {
        params.startsBefore = options.endDate;
      }
      return await this.stClient.getAppointments(params);
    }

    // Incremental sync - use modifiedOn for efficiency
    const lastSync = await this.bqClient.getLastSyncTime(this.entityType);
    this.log.info('Performing incremental sync of appointments', { since: lastSync });

    return await this.stClient.getAppointmentsIncremental(lastSync);
  }

  async transform(data) {
    return data.map(appt => ({
      id: appt.id,
      jobId: appt.jobId,
      appointmentNumber: appt.appointmentNumber,
      start: this.parseDate(appt.start),
      end: this.parseDate(appt.end),
      arrivalWindowStart: this.parseDate(appt.arrivalWindowStart),
      arrivalWindowEnd: this.parseDate(appt.arrivalWindowEnd),
      status: appt.status,
      specialInstructions: appt.specialInstructions,
      createdOn: this.parseDate(appt.createdOn),
      modifiedOn: this.parseDate(appt.modifiedOn),
      customerId: appt.customerId,
      customerMemoId: appt.customerMemoId,
      leadCallId: appt.leadCallId,
      bookingProviderId: appt.bookingProviderId,
      createdById: appt.createdById,
      modifiedById: appt.modifiedById,
      unused: appt.unused,
      isConfirmed: appt.isConfirmed,
      assignedTechnicianIds: this.toJson(appt.assignedTechnicianIds),
      _ingested_at: new Date().toISOString(),
      _ingestion_source: 'servicetitan_v2'
    }));
  }

  getSchema() {
    return [
      { name: 'id', type: 'INT64', mode: 'REQUIRED' },
      { name: 'jobId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'appointmentNumber', type: 'STRING', mode: 'NULLABLE' },
      { name: 'start', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'end', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'arrivalWindowStart', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'arrivalWindowEnd', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'status', type: 'STRING', mode: 'NULLABLE' },
      { name: 'specialInstructions', type: 'STRING', mode: 'NULLABLE' },
      { name: 'createdOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'modifiedOn', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'customerId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'customerMemoId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'leadCallId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'bookingProviderId', type: 'INT64', mode: 'NULLABLE' },
      { name: 'createdById', type: 'INT64', mode: 'NULLABLE' },
      { name: 'modifiedById', type: 'INT64', mode: 'NULLABLE' },
      { name: 'unused', type: 'BOOLEAN', mode: 'NULLABLE' },
      { name: 'isConfirmed', type: 'BOOLEAN', mode: 'NULLABLE' },
      { name: 'assignedTechnicianIds', type: 'JSON', mode: 'NULLABLE' },
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: '_ingestion_source', type: 'STRING', mode: 'NULLABLE' }
    ];
  }

}

export default AppointmentsIngestor;
