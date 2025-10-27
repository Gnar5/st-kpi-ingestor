/**
 * Targeted Payroll Re-sync for August 2025
 * Uses modifiedOn date filtering to catch records that may have been
 * created/modified outside the normal createdOn windows
 */

import 'dotenv/config';
import { ServiceTitanClient } from './src/api/servicetitan_client.js';
import { BigQueryClient } from './src/bq/bigquery_client.js';
import { PayrollIngestor } from './src/ingestors/payroll.js';

async function main() {
  console.log('\n' + '='.repeat(70));
  console.log('  TARGETED PAYROLL RE-SYNC - August 2025');
  console.log('  Strategy: Fetch by modifiedOn to catch late entries/adjustments');
  console.log('='.repeat(70));

  const stClient = new ServiceTitanClient();
  const bqClient = new BigQueryClient();
  const ingestor = new PayrollIngestor(stClient, bqClient);

  // Get current record count
  console.log('\n📊 Current state in BigQuery:');
  const beforeQuery = `
    SELECT
      COUNT(*) as record_count,
      COUNT(DISTINCT jobId) as unique_jobs,
      SUM(amount) as total_labor
    FROM \`kpi-auto-471020.st_raw_v2.raw_payroll\`
    WHERE DATE(date) BETWEEN '2025-08-01' AND '2025-08-31'
  `;

  const [beforeStats] = await bqClient.query(beforeQuery);
  console.log(`   Records: ${(beforeStats.record_count || 0).toLocaleString()}`);
  console.log(`   Unique jobs: ${(beforeStats.unique_jobs || 0).toLocaleString()}`);
  console.log(`   Total labor: $${(beforeStats.total_labor || 0).toLocaleString()}`);

  // Strategy 1: Fetch by modifiedOn for July-October 2025
  // This will catch any payroll for August work that was modified late
  console.log('\n🔍 Strategy 1: Fetching by modifiedOnOrAfter (July 1 - Oct 31, 2025)');
  console.log('   This catches records modified during/after the August period...\n');

  const modifiedRecords = await stClient.fetchAll('payroll/v2/tenant/{tenant}/gross-pay-items', {
    modifiedOnOrAfter: '2025-07-01T00:00:00Z',
    modifiedBefore: '2025-11-01T00:00:00Z'
  });

  console.log(`✅ Fetched ${modifiedRecords.length.toLocaleString()} records (all work dates)`);

  // Filter to only August work dates
  const augustWorkRecords = modifiedRecords.filter(r => {
    if (!r.date) return false;
    const workDate = new Date(r.date);
    return workDate >= new Date('2025-08-01') && workDate < new Date('2025-09-01');
  });

  console.log(`   Filtered to ${augustWorkRecords.length.toLocaleString()} records with August work dates`);

  if (augustWorkRecords.length === 0) {
    console.log('\n⚠️  No new records found. Current data is complete.');
    return;
  }

  // Transform data
  console.log('\n🔄 Transforming records...');
  const transformed = await ingestor.transform(augustWorkRecords);

  // Insert/upsert to BigQuery
  console.log('\n📊 Upserting to BigQuery...');
  const result = await bqClient.upsert(
    bqClient.datasetRaw,
    'raw_payroll',
    transformed,
    'id',
    {
      useByteBatching: true,
      maxBytes: 8 * 1024 * 1024
    }
  );

  console.log(`\n✅ Upsert complete!`);
  console.log(`   Records processed: ${result.merged.toLocaleString()}`);

  // Get new record count
  console.log('\n📊 New state in BigQuery:');
  const [afterStats] = await bqClient.query(beforeQuery);
  console.log(`   Records: ${(afterStats.record_count || 0).toLocaleString()}`);
  console.log(`   Unique jobs: ${(afterStats.unique_jobs || 0).toLocaleString()}`);
  console.log(`   Total labor: $${(afterStats.total_labor || 0).toLocaleString()}`);

  // Calculate delta
  console.log('\n📈 Changes:');
  const recordDelta = (afterStats.record_count || 0) - (beforeStats.record_count || 0);
  const jobDelta = (afterStats.unique_jobs || 0) - (beforeStats.unique_jobs || 0);
  const laborDelta = (afterStats.total_labor || 0) - (beforeStats.total_labor || 0);

  console.log(`   New records: ${recordDelta >= 0 ? '+' : ''}${recordDelta.toLocaleString()}`);
  console.log(`   New jobs: ${jobDelta >= 0 ? '+' : ''}${jobDelta.toLocaleString()}`);
  console.log(`   Labor change: ${laborDelta >= 0 ? '+' : ''}$${laborDelta.toLocaleString()}`);

  // Check specific job 386171256
  console.log('\n🔍 Checking job 386171256 (UDLR Healthcare):');
  const jobQuery = `
    SELECT
      COUNT(*) as record_count,
      SUM(amount) as total_labor,
      STRING_AGG(DISTINCT CAST(employeeId AS STRING)) as employee_ids
    FROM \`kpi-auto-471020.st_raw_v2.raw_payroll\`
    WHERE jobId = 386171256
  `;

  const [jobStats] = await bqClient.query(jobQuery);
  console.log(`   Records: ${jobStats.record_count}`);
  console.log(`   Total labor: $${jobStats.total_labor.toFixed(2)}`);
  console.log(`   Employees: ${jobStats.employee_ids}`);
  console.log(`   Expected: $1,114.80 (ServiceTitan PDF)`);
  console.log(`   Gap: $${(1114.80 - jobStats.total_labor).toFixed(2)}`);

  console.log('\n✨ Re-sync complete!\n');
}

main().catch(error => {
  console.error('\n❌ Re-sync failed:', error);
  process.exit(1);
});
