/**
 * Job Costing Report Ingestor (Reporting API)
 * Fetches job costing data with pre-calculated GPM from ServiceTitan Reporting API
 *
 * NOTE: Uses Reporting API to get ServiceTitan's official job costing calculations.
 * This avoids data sync issues when trying to reconstruct job costing from raw tables.
 *
 * Report ID: 389438975 (*FOREMAN Job Cost - THIS WEEK ONLY* - API)
 * Category: report-category/operations
 *
 * Date Parameters:
 * - From: Start date (YYYY-MM-DD)
 * - To: End date (YYYY-MM-DD)
 * - DateType: 1 (scheduled date)
 *
 * Report Fields (15 columns):
 * [0] ScheduledDate - Job scheduled date
 * [1] JobBusinessUnit - Business unit name
 * [2] SoldBy - Sales representative
 * [3] PrimaryTechnician - Lead technician
 * [4] JobType - Job type/category
 * [5] CustomerName - Customer name
 * [6] JobNumber - Job ID
 * [7] Subtotal - Revenue subtotal (before tax)
 * [8] LaborPay - Labor gross pay
 * [9] PayrollAdjustments - Payroll adjustments
 * [10] MaterialEquipmentPurchaseOrderCosts - Material costs
 * [11] ReturnCosts - Return credits
 * [12] TotalCosts - Total job costs
 * [13] GrossMarginPercentage - GPM% (pre-calculated by ST)
 * [14] JobStatus - Job status (Completed, etc.)
 */

import { BaseIngestor } from '../ingestors/base_ingestor.js';

export class JobCostingReportIngestor extends BaseIngestor {
  constructor(stClient, bqClient, config = {}) {
    super('job_costing_report', stClient, bqClient, {
      tableId: 'raw_job_costing_report',
      primaryKey: 'job_id',
      partitionField: 'scheduled_date',
      clusterFields: ['business_unit', 'job_status'],
      ...config
    });

    // Report configuration
    this.reportId = config.reportId || process.env.JOB_COSTING_REPORT_ID || '389438975';
    this.categoryPath = 'report-category/operations';
  }

  /**
   * Fetch job costing data from Reporting API
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
      DateType: 1  // Scheduled date
    };

    this.log.info('Fetching job costing report', {
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
   * Transform job costing report data into table rows
   * Report format (array): [scheduled_date, bu, sold_by, tech, job_type, customer, job_number, ...]
   * Array indices based on actual API response - see field list in header comment
   */
  async transform(data) {
    return data.map((row) => {
      // Parse date and numeric fields
      const scheduledDate = this.parseDate(row[0]);
      const jobNumber = row[6] ? String(row[6]) : null;
      const subtotal = parseFloat(row[7] || 0);
      const laborPay = parseFloat(row[8] || 0);
      const payrollAdjustments = parseFloat(row[9] || 0);
      const materialCosts = parseFloat(row[10] || 0);
      const returnCosts = parseFloat(row[11] || 0);
      const totalCosts = parseFloat(row[12] || 0);
      const gpmPercent = parseFloat(row[13] || 0);

      // Normalize business unit name (match job_costing_v4 logic)
      let businessUnit = row[1] || '';
      if (businessUnit === "Andy's Painting-Production") {
        businessUnit = "Andy's Painting-Production";
      } else if (businessUnit === 'Commercial-AZ-Production') {
        businessUnit = 'Commercial-AZ-Production';
      } else if (businessUnit === 'Phoenix-Production') {
        businessUnit = 'Phoenix-Production';
      } else if (businessUnit === 'Tucson-Production') {
        businessUnit = 'Tucson-Production';
      } else if (businessUnit === 'Guaranteed Painting-Production') {
        businessUnit = 'Guaranteed Painting-Production';
      } else if (businessUnit === 'Nevada-Production') {
        businessUnit = 'Nevada-Production';
      }

      return {
        job_id: jobNumber,
        scheduled_date: scheduledDate,
        business_unit: businessUnit,
        sold_by: row[2] || null,
        primary_technician: row[3] || null,
        job_type: row[4] || null,
        customer_name: row[5] || null,
        job_status: row[14] || null,

        // Revenue
        revenue_subtotal: subtotal,

        // Costs
        labor_pay: laborPay,
        payroll_adjustments: payrollAdjustments,
        labor_total: laborPay + payrollAdjustments,
        material_costs: materialCosts,
        return_costs: returnCosts,
        total_costs: totalCosts,

        // Profit metrics
        gross_profit: subtotal - totalCosts,
        gpm_percent: gpmPercent,

        // Metadata
        _ingested_at: new Date().toISOString(),
        _ingestion_source: 'servicetitan_reporting_api',
        raw: this.toJson(row)  // Store raw array for debugging
      };
    });
  }

  getSchema() {
    return [
      { name: 'job_id', type: 'STRING', mode: 'REQUIRED' },
      { name: 'scheduled_date', type: 'TIMESTAMP', mode: 'NULLABLE' },
      { name: 'business_unit', type: 'STRING', mode: 'NULLABLE' },
      { name: 'sold_by', type: 'STRING', mode: 'NULLABLE' },
      { name: 'primary_technician', type: 'STRING', mode: 'NULLABLE' },
      { name: 'job_type', type: 'STRING', mode: 'NULLABLE' },
      { name: 'customer_name', type: 'STRING', mode: 'NULLABLE' },
      { name: 'job_status', type: 'STRING', mode: 'NULLABLE' },

      // Revenue
      { name: 'revenue_subtotal', type: 'FLOAT64', mode: 'NULLABLE' },

      // Costs
      { name: 'labor_pay', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'payroll_adjustments', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'labor_total', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'material_costs', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'return_costs', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'total_costs', type: 'FLOAT64', mode: 'NULLABLE' },

      // Profit metrics
      { name: 'gross_profit', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'gpm_percent', type: 'FLOAT64', mode: 'NULLABLE' },

      // Metadata
      { name: '_ingested_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: '_ingestion_source', type: 'STRING', mode: 'REQUIRED' },
      { name: 'raw', type: 'JSON', mode: 'NULLABLE' }
    ];
  }

}

export default JobCostingReportIngestor;
