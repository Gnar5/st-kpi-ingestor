/**
 * Test script to backfill a small sample of payroll data
 * Helps diagnose the BigQuery insertion issue
 */

import 'dotenv/config';
import { ServiceTitanClient } from './src/api/servicetitan_client.js';
import { BigQueryClient } from './src/bq/bigquery_client.js';
import { PayrollIngestor } from './src/ingestors/payroll.js';

async function main() {
  console.log('🔍 Testing Payroll Backfill - Small Sample\n');

  const stClient = new ServiceTitanClient();
  const bqClient = new BigQueryClient();
  const ingestor = new PayrollIngestor(stClient, bqClient);

  try {
    // Fetch just 3 days of data from gross-pay-items
    console.log('⏳ Fetching payroll for Oct 19-22, 2025...');
    const records = await stClient.fetchAll('payroll/v2/tenant/{tenant}/gross-pay-items', {
      paidOnOrAfter: '2025-10-19T00:00:00Z',
      paidBefore: '2025-10-22T00:00:00Z'
    });

    console.log(`✅ Fetched ${records.length} payroll records\n`);

    if (records.length === 0) {
      console.log('No records to test with');
      return;
    }

    // Show sample record structure
    console.log('📄 Sample raw record from API:');
    console.log(JSON.stringify(records[0], null, 2));
    console.log('');

    // Transform
    console.log('🔄 Transforming records...');
    const transformed = await ingestor.transform(records);
    console.log(`✅ Transformed ${transformed.length} records\n`);

    // Show sample transformed record
    console.log('📄 Sample transformed record:');
    console.log(JSON.stringify(transformed[0], null, 2));
    console.log('');

    // Try inserting just the first 10 records
    const sample = transformed.slice(0, 10);
    console.log(`📊 Attempting to insert ${sample.length} sample records to BigQuery...`);

    const result = await bqClient.upsert(
      'st_raw_v2',
      'raw_payroll',
      sample,
      'payrollId',
      {
        useByteBatching: true,
        maxBytes: 8 * 1024 * 1024
      }
    );

    console.log(`\n✅ SUCCESS! Inserted ${result.merged} records`);
    console.log(JSON.stringify(result, null, 2));

  } catch (error) {
    console.error('\n❌ ERROR:', error.message);
    console.error('Stack:', error.stack);
  }
}

main().catch(console.error);
