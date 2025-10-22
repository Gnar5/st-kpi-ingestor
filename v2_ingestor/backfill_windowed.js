/**
 * Windowed Backfill Script
 *
 * Backfills data in monthly windows to avoid memory issues
 * Runs locally and calls Cloud Run endpoints with date filters
 */

import 'dotenv/config';

const SERVICE_URL = process.env.SERVICE_URL || 'https://st-v2-ingestor-gnz5sx34ba-uc.a.run.app';

// Generate monthly windows from 2020-01-01 to now
function generateMonthlyWindows(startYear = 2020) {
  const windows = [];
  const now = new Date();
  const currentYear = now.getFullYear();
  const currentMonth = now.getMonth() + 1; // 1-12

  for (let year = startYear; year <= currentYear; year++) {
    const endMonth = year === currentYear ? currentMonth : 12;

    for (let month = 1; month <= endMonth; month++) {
      const startDate = `${year}-${String(month).padStart(2, '0')}-01`;

      // Calculate end date (first day of next month)
      let nextMonth = month + 1;
      let nextYear = year;
      if (nextMonth > 12) {
        nextMonth = 1;
        nextYear = year + 1;
      }
      const endDate = `${nextYear}-${String(nextMonth).padStart(2, '0')}-01`;

      windows.push({ startDate, endDate, label: `${year}-${String(month).padStart(2, '0')}` });
    }
  }

  return windows;
}

// Entities to backfill (skip campaigns - already has full data)
const ENTITIES = [
  'customers',
  'locations',
  'estimates',
  'jobs',
  'invoices',
  'payments',
  'payroll'
];

async function backfillWindow(entity, startDate, endDate) {
  const url = `${SERVICE_URL}/ingest/${entity}?mode=incremental&modifiedSince=${startDate}`;

  console.log(`  Fetching ${entity} from ${startDate} to ${endDate}...`);

  try {
    const response = await fetch(url);
    const result = await response.json();

    if (!response.ok) {
      console.error(`  ✗ FAILED: ${result.error || 'Unknown error'}`);
      return { success: false, error: result.error };
    }

    console.log(`  ✓ Success: ${result.recordsInserted || 0} records inserted`);
    return { success: true, recordsInserted: result.recordsInserted || 0 };
  } catch (error) {
    console.error(`  ✗ ERROR: ${error.message}`);
    return { success: false, error: error.message };
  }
}

async function backfillEntity(entity, windows) {
  console.log(`\n========================================`);
  console.log(`Entity: ${entity}`);
  console.log(`Windows: ${windows.length} months`);
  console.log(`========================================\n`);

  let totalInserted = 0;
  let successCount = 0;
  let failCount = 0;

  for (const window of windows) {
    const result = await backfillWindow(entity, window.startDate, window.endDate);

    if (result.success) {
      successCount++;
      totalInserted += result.recordsInserted;
    } else {
      failCount++;
    }

    // Wait 2 seconds between windows to avoid rate limits
    await new Promise(resolve => setTimeout(resolve, 2000));
  }

  console.log(`\n${entity} Complete:`);
  console.log(`  Total inserted: ${totalInserted}`);
  console.log(`  Successful windows: ${successCount}/${windows.length}`);
  console.log(`  Failed windows: ${failCount}`);

  return {
    entity,
    totalInserted,
    successCount,
    failCount
  };
}

async function main() {
  console.log(`
╔═══════════════════════════════════════════════════════╗
║       ServiceTitan v2 Windowed Backfill              ║
╚═══════════════════════════════════════════════════════╝

Service URL: ${SERVICE_URL}
Strategy: Monthly windows from 2020-01-01 to present
Entities: ${ENTITIES.length} (skipping campaigns - already complete)

`);

  const windows = generateMonthlyWindows(2020);
  console.log(`Generated ${windows.length} monthly windows\n`);

  const startTime = Date.now();
  const entityResults = [];

  for (const entity of ENTITIES) {
    const result = await backfillEntity(entity, windows);
    entityResults.push(result);

    // Wait 5 seconds between entities
    console.log('\nWaiting 5 seconds before next entity...\n');
    await new Promise(resolve => setTimeout(resolve, 5000));
  }

  const duration = Math.round((Date.now() - startTime) / 1000);

  console.log(`\n
╔═══════════════════════════════════════════════════════╗
║              Backfill Complete!                       ║
╚═══════════════════════════════════════════════════════╝

Duration: ${Math.floor(duration / 60)} minutes ${duration % 60} seconds

Results by Entity:
`);

  entityResults.forEach(r => {
    console.log(`  ${r.entity.padEnd(12)} - ${r.totalInserted.toLocaleString().padStart(8)} records  (${r.successCount}/${r.successCount + r.failCount} windows succeeded)`);
  });

  const grandTotal = entityResults.reduce((sum, r) => sum + r.totalInserted, 0);
  console.log(`\n  TOTAL: ${grandTotal.toLocaleString()} records inserted\n`);

  console.log('\nNext steps:');
  console.log('  1. Run: ./check_backfill_status.sh');
  console.log('  2. Deploy KPI marts: bq query < st_mart_v2_kpis.sql');
  console.log('  3. Set up Cloud Scheduler for daily incremental sync\n');
}

main().catch(error => {
  console.error('\n❌ Backfill failed:', error.message);
  process.exit(1);
});
