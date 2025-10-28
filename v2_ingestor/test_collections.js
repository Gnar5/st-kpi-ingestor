/**
 * Test Collections Ingestor
 * Tests the Collections ingestor using Reporting API
 */

import 'dotenv/config';
import ServiceTitanClient from './src/api/servicetitan_client.js';
import BigQueryClient from './src/bq/bigquery_client.js';
import { CollectionsIngestor } from './src/ingestors_reports/collections.js';

async function testCollections() {
  try {
    console.log('=== Testing Collections Ingestor ===\n');

    // Initialize clients
    const stClient = new ServiceTitanClient();
    const bqClient = new BigQueryClient();

    // Create ingestor
    const ingestor = new CollectionsIngestor(stClient, bqClient);

    // Test fetch for week of 8/18-8/24
    console.log('Step 1: Fetching collections data for week of 8/18-8/24...');
    const data = await ingestor.fetch({
      from: '2025-08-18',
      to: '2025-08-24'
    });

    console.log(`✓ Fetched ${data.length} payment records\n`);

    if (data.length > 0) {
      console.log('Sample payment:');
      console.log(JSON.stringify(data[0], null, 2));
      console.log('');
    }

    // Test transform
    console.log('Step 2: Transforming data...');
    const transformed = await ingestor.transform(data);
    console.log(`✓ Transformed ${transformed.length} records\n`);

    if (transformed.length > 0) {
      console.log('Sample transformed record:');
      console.log(JSON.stringify(transformed[0], null, 2));
      console.log('');
    }

    // Calculate totals by business unit
    console.log('Step 3: Calculating totals by business unit...');
    const totals = {};
    transformed.forEach(record => {
      const bu = record.business_unit;
      if (!totals[bu]) {
        totals[bu] = { count: 0, total: 0 };
      }
      totals[bu].count++;
      totals[bu].total += record.amount || 0;
    });

    console.log('\nTotals by Business Unit:');
    Object.keys(totals).sort().forEach(bu => {
      console.log(`  ${bu}: $${totals[bu].total.toFixed(2)} (${totals[bu].count} payments)`);
    });

    const grandTotal = Object.values(totals).reduce((sum, t) => sum + t.total, 0);
    console.log(`\n  TOTAL: $${grandTotal.toFixed(2)}`);

    // Expected total from ServiceTitan export
    const expected = 632585.38;
    const diff = Math.abs(grandTotal - expected);

    console.log(`\n  Expected: $${expected.toFixed(2)}`);
    console.log(`  Difference: $${diff.toFixed(2)}`);

    if (diff < 1) {
      console.log('  ✓ MATCH! Data is accurate.\n');
    } else {
      console.log(`  ⚠ MISMATCH! Off by $${diff.toFixed(2)}\n`);
    }

    console.log('=== Test Complete ===');

  } catch (error) {
    console.error('Error:', error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

testCollections();
