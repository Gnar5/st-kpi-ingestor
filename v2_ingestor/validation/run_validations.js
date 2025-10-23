#!/usr/bin/env node

/**
 * Validation Suite Runner
 * Executes SQL validation queries and outputs JSON summary
 *
 * Usage: node run_validations.js [--output=json|table] [--threshold-fail]
 */

import { BigQuery } from '@google-cloud/bigquery';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const PROJECT_ID = process.env.BQ_PROJECT_ID || 'kpi-auto-471020';
const VALIDATIONS_DIR = __dirname;

// Parse command line arguments
const args = process.argv.slice(2);
const outputFormat = args.find(a => a.startsWith('--output='))?.split('=')[1] || 'json';
const failOnThreshold = args.includes('--threshold-fail');

// Initialize BigQuery client
const bigquery = new BigQuery({
  projectId: PROJECT_ID
});

// Validation queries to run
const VALIDATIONS = [
  {
    name: 'Coverage by Month',
    file: 'coverage_by_month.sql',
    description: 'Entity record counts by month',
    criticalField: 'status',
    failCondition: (rows) => rows.some(r => r.status === '🔴 NO DATA')
  },
  {
    name: 'Join Integrity',
    file: 'joins_integrity.sql',
    description: 'Critical joins coverage validation',
    criticalField: 'status',
    failCondition: (rows) => rows.some(r => r.status === '🔴 CRITICAL')
  },
  {
    name: 'KPI Weekly Trends',
    file: 'kpi_weekly_checks.sql',
    description: 'KPI anomaly detection for last 12 weeks',
    criticalField: 'overall_status',
    failCondition: (rows) => rows.some(r => r.overall_status?.includes('🔴🔴'))
  },
  {
    name: 'Data Quality Stoplight',
    file: 'bq_stoplight.sql',
    description: 'RED/YELLOW/GREEN status for critical metrics',
    criticalField: 'status',
    failCondition: (rows) => rows.filter(r => r.status === '🔴 RED').length > 2
  }
];

/**
 * Run a single validation query
 */
async function runValidation(validation) {
  const filePath = path.join(VALIDATIONS_DIR, validation.file);

  try {
    // Read SQL file
    const sql = fs.readFileSync(filePath, 'utf8');

    // Execute query
    const startTime = Date.now();
    const [rows] = await bigquery.query({
      query: sql,
      location: 'US',
      useLegacySql: false
    });
    const executionTime = Date.now() - startTime;

    // Check for failure conditions
    const failed = validation.failCondition ? validation.failCondition(rows) : false;

    // Count status indicators
    const statusCounts = {
      red: 0,
      yellow: 0,
      green: 0
    };

    if (validation.criticalField) {
      rows.forEach(row => {
        const status = row[validation.criticalField]?.toString() || '';
        if (status.includes('🔴')) statusCounts.red++;
        else if (status.includes('🟡')) statusCounts.yellow++;
        else if (status.includes('🟢')) statusCounts.green++;
      });
    }

    return {
      name: validation.name,
      description: validation.description,
      status: failed ? 'FAILED' : 'PASSED',
      executionTimeMs: executionTime,
      rowCount: rows.length,
      statusCounts,
      sample: rows.slice(0, 5),  // Include first 5 rows as sample
      failed,
      timestamp: new Date().toISOString()
    };

  } catch (error) {
    return {
      name: validation.name,
      description: validation.description,
      status: 'ERROR',
      error: error.message,
      failed: true,
      timestamp: new Date().toISOString()
    };
  }
}

/**
 * Format output as table (for console)
 */
function formatTable(results) {
  console.log('\n' + '='.repeat(80));
  console.log('DATA VALIDATION REPORT');
  console.log('='.repeat(80));
  console.log(`Timestamp: ${new Date().toISOString()}`);
  console.log(`Project: ${PROJECT_ID}`);
  console.log('='.repeat(80) + '\n');

  results.forEach(result => {
    const statusIcon = result.status === 'PASSED' ? '✅' :
                       result.status === 'FAILED' ? '❌' : '⚠️';

    console.log(`${statusIcon} ${result.name}`);
    console.log(`   ${result.description}`);
    console.log(`   Status: ${result.status}`);

    if (result.executionTimeMs) {
      console.log(`   Execution: ${result.executionTimeMs}ms`);
      console.log(`   Rows: ${result.rowCount}`);
    }

    if (result.statusCounts) {
      console.log(`   Results: 🔴 ${result.statusCounts.red} | 🟡 ${result.statusCounts.yellow} | 🟢 ${result.statusCounts.green}`);
    }

    if (result.error) {
      console.log(`   Error: ${result.error}`);
    }

    console.log();
  });

  // Summary
  const passed = results.filter(r => r.status === 'PASSED').length;
  const failed = results.filter(r => r.status === 'FAILED').length;
  const errors = results.filter(r => r.status === 'ERROR').length;

  console.log('='.repeat(80));
  console.log('SUMMARY');
  console.log(`Total: ${results.length} | Passed: ${passed} | Failed: ${failed} | Errors: ${errors}`);
  console.log('='.repeat(80) + '\n');
}

/**
 * Main execution
 */
async function main() {
  console.log('Starting validation suite...\n');

  const results = [];

  // Run all validations
  for (const validation of VALIDATIONS) {
    console.log(`Running: ${validation.name}...`);
    const result = await runValidation(validation);
    results.push(result);
  }

  // Output results
  if (outputFormat === 'table') {
    formatTable(results);
  } else {
    // JSON output
    const summary = {
      project: PROJECT_ID,
      timestamp: new Date().toISOString(),
      totalValidations: results.length,
      passed: results.filter(r => r.status === 'PASSED').length,
      failed: results.filter(r => r.status === 'FAILED').length,
      errors: results.filter(r => r.status === 'ERROR').length,
      overallStatus: results.every(r => r.status === 'PASSED') ? 'SUCCESS' : 'FAILURE',
      validations: results
    };

    console.log(JSON.stringify(summary, null, 2));
  }

  // Exit with appropriate code
  const hasFailures = results.some(r => r.failed);

  if (failOnThreshold && hasFailures) {
    console.error('\n❌ Validation failed - thresholds exceeded');
    process.exit(1);
  } else if (hasFailures) {
    console.warn('\n⚠️  Some validations failed but not exiting with error');
    process.exit(0);
  } else {
    console.log('\n✅ All validations passed');
    process.exit(0);
  }
}

// Run if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}

export { runValidation, VALIDATIONS };