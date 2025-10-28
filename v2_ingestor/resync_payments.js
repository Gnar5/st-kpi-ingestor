/**
 * Re-sync Payments with corrected ingestor
 * This script drops the old payments table and re-syncs with the new schema
 */

import { PaymentsIngestor } from './src/ingestors/payments.js';
import { ServiceTitanClient } from './src/api/servicetitan_client.js';
import { BigQueryClient } from './src/bq/bigquery_client.js';
import { BigQuery } from '@google-cloud/bigquery';

async function resyncPayments() {
  try {
    console.log('=== Payments Re-sync ===\n');

    const stClient = new ServiceTitanClient();
    const bqClient = new BigQueryClient();
    const bq = new BigQuery();

    // Step 1: Drop the old table
    console.log('Step 1: Dropping old raw_payments table...');
    try {
      await bq
        .dataset('st_raw_v2')
        .table('raw_payments')
        .delete();
      console.log('✓ Old table dropped\n');
    } catch (error) {
      if (error.code === 404) {
        console.log('✓ Table does not exist (OK)\n');
      } else {
        throw error;
      }
    }

    // Step 2: Create ingestor (will create new table with correct schema)
    console.log('Step 2: Initializing Payments ingestor...');
    const ingestor = new PaymentsIngestor(stClient, bqClient);

    // Step 3: Fetch payments (limiting to recent data for initial test)
    console.log('Step 3: Fetching payments from ServiceTitan API...');
    console.log('Date range: Week of 8/18/2025 for testing\n');

    const payments = await stClient.getPayments({
      createdOnOrAfter: '2025-08-18T00:00:00Z',
      createdBefore: '2025-08-25T00:00:00Z',
      pageSize: 500
    });

    console.log(`✓ Fetched ${payments.length} payments\n`);

    if (payments.length > 0) {
      console.log('Sample payment structure:');
      console.log(JSON.stringify(payments[0], null, 2));
      console.log('');
    }

    // Step 4: Transform the data
    console.log('Step 4: Transforming payments...');
    const transformed = await ingestor.transform(payments);
    console.log(`✓ Transformed into ${transformed.length} payment rows\n`);

    if (transformed.length > 0) {
      console.log('Sample transformed row:');
      console.log(JSON.stringify(transformed[0], null, 2));
      console.log('');
    }

    // Step 5: Load to BigQuery
    console.log('Step 5: Loading to BigQuery...');
    await ingestor.load(transformed);
    console.log(`✓ Loaded ${transformed.length} rows to st_raw_v2.raw_payments\n`);

    console.log('=== Re-sync Complete ===');
    console.log(`Total payments: ${payments.length}`);
    console.log(`Total payment splits: ${transformed.length}`);
    console.log('\nNext steps:');
    console.log('1. Query the table to verify data');
    console.log('2. Run a full sync for all historical data if needed');
    console.log('3. Create the collections view');

  } catch (error) {
    console.error('Error during resync:', error);
    throw error;
  }
}

resyncPayments().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
