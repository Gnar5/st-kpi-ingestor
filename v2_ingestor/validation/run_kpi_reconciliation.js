#!/usr/bin/env node

/**
 * KPI Reconciliation Runner
 * Executes BigQuery validation queries and generates a markdown report
 * comparing ServiceTitan exports to BigQuery KPIs
 */

const { BigQuery } = require('@google-cloud/bigquery');
const fs = require('fs').promises;
const path = require('path');

// Configuration
const config = {
  projectId: 'kpi-auto-471020',
  datasetId: 'st_mart_v2',
  startDate: '2025-08-18',
  endDate: '2025-08-24',
  outputFile: 'validation_results.md'
};

// ServiceTitan Expected Values (from exports)
const expectedValues = {
  'Phoenix-Sales': {
    total_booked: 116551.26,
    success_rate: 39.74,
    closed_opportunities: 31,
    sales_opportunities: 78
  },
  'Tucson-Sales': {
    total_booked: 89990.11,
    success_rate: 51.22,
    closed_opportunities: 21,
    sales_opportunities: 41
  },
  'Nevada-Sales': {
    total_booked: 105890.00,
    success_rate: 60.87,
    closed_opportunities: 14,
    sales_opportunities: 23
  },
  'Andy\'s Painting-Sales': {
    total_booked: 30896.91,
    success_rate: 35.71,
    closed_opportunities: 10,
    sales_opportunities: 28
  },
  'Commercial-AZ-Sales': {
    total_booked: 119803.60,
    success_rate: 26.92,
    closed_opportunities: 7,
    sales_opportunities: 26
  },
  'Guaranteed Painting-Sales': {
    total_booked: 26067.40,
    success_rate: 77.78,
    closed_opportunities: 7,
    sales_opportunities: 9
  },
  'Phoenix-Production': {
    dollars_produced: 232891.98
  },
  'Tucson-Production': {
    dollars_produced: 83761.16
  },
  'Nevada-Production': {
    dollars_produced: 23975.00
  },
  'Andy\'s Painting-Production': {
    dollars_produced: 53752.56
  },
  'Commercial-AZ-Production': {
    dollars_produced: 77345.25
  },
  'Guaranteed Painting-Production': {
    dollars_produced: 30472.30
  }
};

class KPIReconciliation {
  constructor() {
    this.bigquery = new BigQuery({ projectId: config.projectId });
    this.results = [];
  }

  /**
   * Main execution function
   */
  async run() {
    console.log('🚀 Starting KPI Reconciliation...\n');

    try {
      // Run validation queries
      await this.validateSalesKPIs();
      await this.validateProductionKPIs();
      await this.validateSecondaryKPIs();

      // Generate report
      const report = this.generateReport();

      // Save to file
      await fs.writeFile(config.outputFile, report);
      console.log(`\n✅ Report saved to ${config.outputFile}`);

      // Print summary
      this.printSummary();

    } catch (error) {
      console.error('❌ Error during reconciliation:', error);
      process.exit(1);
    }
  }

  /**
   * Validate Sales KPIs
   */
  async validateSalesKPIs() {
    console.log('📊 Validating Sales KPIs...');

    const query = `
      SELECT
        business_unit,
        SUM(total_booked) as total_booked,
        AVG(close_rate) * 100 as success_rate,
        SUM(lead_count) as opportunities,
        SUM(estimate_count) as estimates
      FROM \`${config.projectId}.${config.datasetId}.daily_kpis\`
      WHERE business_unit LIKE '%Sales'
        AND event_date BETWEEN @startDate AND @endDate
      GROUP BY business_unit
      ORDER BY business_unit
    `;

    const options = {
      query: query,
      params: {
        startDate: config.startDate,
        endDate: config.endDate
      }
    };

    const [rows] = await this.bigquery.query(options);

    rows.forEach(row => {
      const expected = expectedValues[row.business_unit] || {};
      this.results.push({
        business_unit: row.business_unit,
        kpi: 'Total Booked',
        expected: expected.total_booked,
        actual: row.total_booked,
        variance: this.calculateVariance(expected.total_booked, row.total_booked)
      });

      this.results.push({
        business_unit: row.business_unit,
        kpi: 'Success Rate',
        expected: expected.success_rate,
        actual: row.success_rate,
        variance: this.calculateVariance(expected.success_rate, row.success_rate)
      });
    });
  }

  /**
   * Validate Production KPIs
   */
  async validateProductionKPIs() {
    console.log('🏭 Validating Production KPIs...');

    const query = `
      SELECT
        business_unit,
        SUM(dollars_produced) as dollars_produced,
        AVG(gpm_percent) as gpm_percent,
        AVG(warranty_percent) as warranty_percent
      FROM \`${config.projectId}.${config.datasetId}.daily_kpis\`
      WHERE business_unit LIKE '%Production'
        AND event_date BETWEEN @startDate AND @endDate
      GROUP BY business_unit
      ORDER BY business_unit
    `;

    const options = {
      query: query,
      params: {
        startDate: config.startDate,
        endDate: config.endDate
      }
    };

    const [rows] = await this.bigquery.query(options);

    rows.forEach(row => {
      const expected = expectedValues[row.business_unit] || {};
      this.results.push({
        business_unit: row.business_unit,
        kpi: 'Dollars Produced',
        expected: expected.dollars_produced,
        actual: row.dollars_produced,
        variance: this.calculateVariance(expected.dollars_produced, row.dollars_produced)
      });
    });
  }

  /**
   * Validate Secondary KPIs
   */
  async validateSecondaryKPIs() {
    console.log('📈 Validating Secondary KPIs...');

    // Check for dollars collected
    const collectionQuery = `
      SELECT
        business_unit,
        SUM(dollars_collected) as dollars_collected
      FROM \`${config.projectId}.${config.datasetId}.daily_kpis\`
      WHERE business_unit LIKE '%Production'
        AND event_date BETWEEN @startDate AND @endDate
        AND dollars_collected > 0
      GROUP BY business_unit
    `;

    const options = {
      query: collectionQuery,
      params: {
        startDate: config.startDate,
        endDate: config.endDate
      }
    };

    const [rows] = await this.bigquery.query(options);
    console.log(`  Found ${rows.length} BUs with collection data`);
  }

  /**
   * Calculate variance between expected and actual
   */
  calculateVariance(expected, actual) {
    if (!expected || !actual) return { amount: null, percent: null, status: '⚠️' };

    const amount = actual - expected;
    const percent = ((amount / expected) * 100).toFixed(2);

    let status = '✅';
    if (Math.abs(percent) > 1) status = '⚠️';
    if (Math.abs(percent) > 10) status = '🔴';

    return {
      amount: amount.toFixed(2),
      percent: percent,
      status: status
    };
  }

  /**
   * Generate markdown report
   */
  generateReport() {
    let report = `# KPI Reconciliation Results\n`;
    report += `**Date Range:** ${config.startDate} to ${config.endDate}\n`;
    report += `**Generated:** ${new Date().toISOString()}\n\n`;

    report += `## Summary\n\n`;

    // Group results by business unit
    const byBU = {};
    this.results.forEach(r => {
      if (!byBU[r.business_unit]) byBU[r.business_unit] = [];
      byBU[r.business_unit].push(r);
    });

    // Create table
    report += `| Business Unit | KPI | ST Expected | BQ Actual | Variance | % | Status |\n`;
    report += `|--------------|-----|------------|-----------|----------|---|--------|\n`;

    Object.keys(byBU).sort().forEach(bu => {
      byBU[bu].forEach(result => {
        const exp = result.expected ? result.expected.toFixed(2) : 'N/A';
        const act = result.actual ? result.actual.toFixed(2) : 'N/A';
        const var_amt = result.variance.amount || 'N/A';
        const var_pct = result.variance.percent ? `${result.variance.percent}%` : 'N/A';

        report += `| ${bu} | ${result.kpi} | ${exp} | ${act} | ${var_amt} | ${var_pct} | ${result.variance.status} |\n`;
      });
    });

    report += `\n## Validation Status\n\n`;

    const perfect = this.results.filter(r => r.variance.status === '✅').length;
    const minor = this.results.filter(r => r.variance.status === '⚠️').length;
    const major = this.results.filter(r => r.variance.status === '🔴').length;

    report += `- ✅ Perfect Match: ${perfect} KPIs\n`;
    report += `- ⚠️ Minor Variance: ${minor} KPIs\n`;
    report += `- 🔴 Major Variance: ${major} KPIs\n`;

    return report;
  }

  /**
   * Print summary to console
   */
  printSummary() {
    console.log('\n📋 Validation Summary:');

    const perfect = this.results.filter(r => r.variance.status === '✅').length;
    const minor = this.results.filter(r => r.variance.status === '⚠️').length;
    const major = this.results.filter(r => r.variance.status === '🔴').length;

    console.log(`  ✅ Perfect Match: ${perfect} KPIs`);
    console.log(`  ⚠️ Minor Variance: ${minor} KPIs`);
    console.log(`  🔴 Major Variance: ${major} KPIs`);

    if (major > 0) {
      console.log('\n⚠️ Major variances detected. Review the report for details.');
    }

    if (perfect === this.results.length) {
      console.log('\n🎉 All KPIs match perfectly!');
    }
  }
}

// Execute if run directly
if (require.main === module) {
  const reconciliation = new KPIReconciliation();
  reconciliation.run();
}

module.exports = KPIReconciliation;