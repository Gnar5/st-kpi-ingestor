/**
 * Full Sync Script for Estimates
 * Runs a complete sync of all estimates from ServiceTitan
 * This will fetch ALL estimates (no date filtering)
 */

import 'dotenv/config';
import ServiceTitanClient from './src/api/servicetitan_client.js';
import BigQueryClient from './src/bq/bigquery_client.js';
import { EstimatesIngestor } from './src/ingestors/index.js';

async function main() {
  console.log('\n' + '='.repeat(70));
  console.log('  ESTIMATES FULL SYNC');
  console.log('='.repeat(70));
  console.log('\nThis will sync ALL estimates from ServiceTitan to BigQuery.');
  console.log('Note: items field has been removed to reduce data size.\n');

  const stClient = new ServiceTitanClient();
  const bqClient = new BigQueryClient();
  const estimatesIngestor = new EstimatesIngestor(stClient, bqClient);

  try {
    console.log('🚀 Starting full sync of estimates...\n');

    const startTime = Date.now();
    const result = await estimatesIngestor.ingest({ mode: 'full' });
    const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(2);

    console.log('\n' + '='.repeat(70));
    console.log('  SYNC COMPLETE');
    console.log('='.repeat(70));
    console.log(`\n✅ Success!`);
    console.log(`   Records synced: ${result.recordsInserted?.toLocaleString() || 'N/A'}`);
    console.log(`   Duration: ${duration} minutes`);
    console.log('\n');

  } catch (error) {
    console.error('\n❌ Full sync failed:', error.message);
    console.error('\nError details:', error);
    process.exit(1);
  }
}

main();
