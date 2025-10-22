/**
 * Smart Backfill Strategy
 *
 * - Skip campaigns (already complete)
 * - Small entities: Run full backfill directly
 * - Large entities: Process in yearly chunks to avoid memory issues
 */

import 'dotenv/config';
import { logger } from './src/utils/logger.js';
import ServiceTitanClient from './src/api/servicetitan_client.js';
import BigQueryClient from './src/bq/bigquery_client.js';
import {
  JobsIngestor,
  InvoicesIngestor,
  EstimatesIngestor,
  PaymentsIngestor,
  PayrollIngestor,
  CustomersIngestor,
  LocationsIngestor
} from './src/ingestors/index.js';

const stClient = new ServiceTitanClient();
const bqClient = new BigQueryClient();

const ingestors = {
  // Small entities - can handle full backfill
  customers: new CustomersIngestor(stClient, bqClient),
  locations: new LocationsIngestor(stClient, bqClient),
  estimates: new EstimatesIngestor(stClient, bqClient),

  // Large entities - need windowed backfill
  jobs: new JobsIngestor(stClient, bqClient),
  invoices: new InvoicesIngestor(stClient, bqClient),
  payments: new PaymentsIngestor(stClient, bqClient),
  payroll: new PayrollIngestor(stClient, bqClient)
};

// Skip completed entities (campaigns, customers, locations already have full data)
const SMALL_ENTITIES = ['estimates'];
const LARGE_ENTITIES = ['jobs', 'invoices', 'payments', 'payroll'];

// Generate yearly windows (less granular than monthly)
function generateYearlyWindows() {
  const windows = [];
  const currentYear = new Date().getFullYear();

  for (let year = 2020; year <= currentYear; year++) {
    const startDate = new Date(year, 0, 1).toISOString(); // Jan 1
    const endDate = year < currentYear
      ? new Date(year + 1, 0, 1).toISOString() // Jan 1 next year
      : new Date().toISOString(); // Today

    windows.push({
      startDate,
      endDate,
      label: `${year}`,
      year
    });
  }

  return windows;
}

async function backfillSmallEntity(entity) {
  console.log(`\n📦 ${entity} (small entity - full backfill)`);

  const ingestor = ingestors[entity];

  try {
    const result = await ingestor.ingest({ mode: 'full' });
    console.log(`   ✓ Complete: ${result.recordsInserted || 0} records inserted`);

    return {
      entity,
      success: true,
      totalInserted: result.recordsInserted || 0
    };
  } catch (error) {
    console.error(`   ✗ FAILED: ${error.message}`);
    logger.error('Small entity backfill failed', { entity, error: error.message });

    return {
      entity,
      success: false,
      error: error.message
    };
  }
}

async function backfillLargeEntity(entity, windows) {
  console.log(`\n📦 ${entity} (large entity - windowed backfill)`);
  console.log(`   Processing ${windows.length} yearly windows...`);

  const ingestor = ingestors[entity];
  let totalInserted = 0;
  let successCount = 0;

  for (const window of windows) {
    console.log(`     ${window.label}...`, { end: '' });

    try {
      const result = await ingestor.ingest({
        mode: 'incremental',
        modifiedSince: window.startDate
      });

      const inserted = result.recordsInserted || 0;
      totalInserted += inserted;
      successCount++;

      console.log(` ✓ ${inserted} records`);

      // Wait between windows
      await new Promise(resolve => setTimeout(resolve, 1000));
    } catch (error) {
      console.log(` ✗ FAILED: ${error.message}`);
      logger.error('Window backfill failed', {
        entity,
        window: window.label,
        error: error.message
      });
    }
  }

  console.log(`   Complete: ${totalInserted} total records (${successCount}/${windows.length} windows succeeded)`);

  return {
    entity,
    success: successCount > 0,
    totalInserted,
    successCount,
    totalWindows: windows.length
  };
}

async function main() {
  console.log(`
╔═══════════════════════════════════════════════════════╗
║       ServiceTitan v2 Smart Backfill                  ║
╚═══════════════════════════════════════════════════════╝

Strategy:
  • Campaigns: SKIP (already has full data from 2020-05-18)
  • Small entities (${SMALL_ENTITIES.length}): Full backfill
  • Large entities (${LARGE_ENTITIES.length}): Yearly windows (2020-${new Date().getFullYear()})

Estimated time: 15-30 minutes
`);

  const windows = generateYearlyWindows();
  console.log(`Generated ${windows.length} yearly windows: ${windows.map(w => w.label).join(', ')}\n`);

  const startTime = Date.now();
  const results = [];

  console.log('=' .repeat(60));
  console.log('PHASE 1: Small Entities (Full Backfill)');
  console.log('='.repeat(60));

  for (const entity of SMALL_ENTITIES) {
    const result = await backfillSmallEntity(entity);
    results.push(result);
    await new Promise(resolve => setTimeout(resolve, 2000));
  }

  console.log('\n' + '='.repeat(60));
  console.log('PHASE 2: Large Entities (Windowed Backfill)');
  console.log('='.repeat(60));

  for (const entity of LARGE_ENTITIES) {
    const result = await backfillLargeEntity(entity, windows);
    results.push(result);
    await new Promise(resolve => setTimeout(resolve, 3000));
  }

  const duration = Math.round((Date.now() - startTime) / 1000);

  console.log(`\n
╔═══════════════════════════════════════════════════════╗
║              Backfill Complete!                       ║
╚═══════════════════════════════════════════════════════╝

Duration: ${Math.floor(duration / 60)}m ${duration % 60}s

Results:
`);

  results.forEach(r => {
    const status = r.success ? '✓' : '✗';
    const windows = r.totalWindows ? ` (${r.successCount}/${r.totalWindows} windows)` : '';
    console.log(`  ${status} ${r.entity.padEnd(12)} ${r.totalInserted.toLocaleString().padStart(8)} records${windows}`);
  });

  const grandTotal = results.reduce((sum, r) => sum + r.totalInserted, 0);
  const failedCount = results.filter(r => !r.success).length;

  console.log(`\n  TOTAL: ${grandTotal.toLocaleString()} records inserted`);
  if (failedCount > 0) {
    console.log(`  ⚠ Warning: ${failedCount} entities had failures\n`);
  }

  console.log('\nNext steps:');
  console.log('  1. Verify: ./check_backfill_status.sh');
  console.log('  2. Deploy KPI marts: bq query < st_mart_v2_kpis.sql\n');
}

main().catch(error => {
  console.error('\n❌ Backfill failed:', error.message);
  logger.error('Backfill script failed', { error: error.message });
  process.exit(1);
});
