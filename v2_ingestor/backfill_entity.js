/**
 * Universal Entity Backfill Script
 * Backfills any entity using createdOn date ranges
 * Usage: node backfill_entity.js <entity> [startYear]
 * Example: node backfill_entity.js jobs 2020
 */

import 'dotenv/config';
import { ServiceTitanClient } from './src/api/servicetitan_client.js';
import { BigQueryClient } from './src/bq/bigquery_client.js';
import { JobsIngestor } from './src/ingestors/jobs.js';
import { InvoicesIngestor } from './src/ingestors/invoices.js';
import { PaymentsIngestor } from './src/ingestors/payments.js';
import { PayrollIngestor } from './src/ingestors/payroll.js';
import { PayrollAdjustmentsIngestor } from './src/ingestors/payroll_adjustments.js';
import { PurchaseOrdersIngestor } from './src/ingestors/purchase_orders.js';
import { ReturnsIngestor } from './src/ingestors/returns.js';

// Entity configuration
const ENTITY_CONFIG = {
  jobs: {
    ingestor: JobsIngestor,
    endpoint: 'jpm/v2/tenant/{tenant}/jobs',
    tableId: 'raw_jobs',
    primaryKey: 'id',
    useByteBatching: false
  },
  invoices: {
    ingestor: InvoicesIngestor,
    endpoint: 'accounting/v2/tenant/{tenant}/invoices',
    tableId: 'raw_invoices',
    primaryKey: 'id',
    useByteBatching: true  // Invoices can have large line items
  },
  payments: {
    ingestor: PaymentsIngestor,
    endpoint: 'accounting/v2/tenant/{tenant}/payments',
    tableId: 'raw_payments',
    primaryKey: 'id',
    useByteBatching: false
  },
  payroll: {
    ingestor: PayrollIngestor,
    endpoint: 'payroll/v2/tenant/{tenant}/gross-pay-items',  // Use gross-pay-items for line-item detail
    tableId: 'raw_payroll',
    primaryKey: 'id',  // Use hash-based unique ID
    useByteBatching: true  // Enable byte-size batching to avoid 10MB payload limit
  },
  payroll_adjustments: {
    ingestor: PayrollAdjustmentsIngestor,
    endpoint: 'payroll/v2/tenant/{tenant}/payroll-adjustments',  // Direct adjustments, bonuses, corrections
    tableId: 'raw_payroll_adjustments',
    primaryKey: 'id',  // Use hash-based unique ID
    useByteBatching: false  // Adjustments are typically small records
  },
  purchase_orders: {
    ingestor: PurchaseOrdersIngestor,
    endpoint: 'inventory/v2/tenant/{tenant}/purchase-orders',
    tableId: 'raw_purchase_orders',
    primaryKey: 'id',  // Use hash-based unique ID
    useByteBatching: true  // Purchase orders can have multiple line items
  },
  returns: {
    ingestor: ReturnsIngestor,
    endpoint: 'inventory/v2/tenant/{tenant}/returns',
    tableId: 'raw_returns',
    primaryKey: 'id',  // Use hash-based unique ID
    useByteBatching: true  // Returns can have multiple line items
  }
};

// Generate yearly date ranges
function generateYearlyChunks(startYear = 2020) {
  const chunks = [];
  const currentYear = new Date().getFullYear();

  for (let year = startYear; year <= currentYear; year++) {
    const startDate = new Date(year, 0, 1); // Jan 1
    const endDate = year < currentYear
      ? new Date(year + 1, 0, 1) // Jan 1 next year
      : new Date(); // Today

    chunks.push({
      year,
      startDate: startDate.toISOString(),
      endDate: endDate.toISOString(),
      label: `${year}`
    });
  }

  return chunks;
}

async function backfillChunk(entityName, config, chunk, chunkNum, totalChunks, ensureTable = false) {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`📦 CHUNK ${chunkNum}/${totalChunks}: ${chunk.label}`);
  console.log(`   Date range: ${chunk.startDate.split('T')[0]} to ${chunk.endDate.split('T')[0]}`);
  console.log('='.repeat(60));

  const stClient = new ServiceTitanClient();
  const bqClient = new BigQueryClient();
  const ingestor = new config.ingestor(stClient, bqClient);

  try {
    // Ensure table exists on first chunk
    if (ensureTable) {
      console.log('\n🔧 Ensuring table exists with correct schema...');
      await bqClient.ensureTable(
        bqClient.datasetRaw,
        config.tableId,
        ingestor.getSchema(),
        {
          description: `ServiceTitan ${entityName} data`,
          partitionField: ingestor.config.partitionField,
          clusterFields: ingestor.config.clusterFields
        }
      );
    }

    // Fetch records for this date range
    console.log(`\n⏳ Fetching ${entityName} created in ${chunk.label}...`);
    const records = await stClient.fetchAll(config.endpoint, {
      createdOnOrAfter: chunk.startDate,
      createdBefore: chunk.endDate
    });

    console.log(`✅ Fetched ${records.length.toLocaleString()} ${entityName}`);

    if (records.length === 0) {
      console.log('   ℹ️  No records for this period, skipping...');
      return { chunk: chunk.label, fetched: 0, inserted: 0 };
    }

    // Transform data
    console.log(`\n🔄 Transforming ${records.length.toLocaleString()} ${entityName}...`);
    const transformed = await ingestor.transform(records);

    // Insert with appropriate batching strategy
    const batchingMsg = config.useByteBatching
      ? 'BYTE-BASED BATCHING (variable record sizes)'
      : 'ROW-BASED BATCHING (standard)';
    console.log(`\n📊 Inserting to BigQuery with ${batchingMsg}...`);

    const result = await bqClient.upsert(
      bqClient.datasetRaw,
      config.tableId,
      transformed,
      config.primaryKey || 'id',
      {
        useByteBatching: config.useByteBatching,
        maxBytes: 8 * 1024 * 1024  // 8MB limit
      }
    );

    console.log(`\n✅ Chunk ${chunk.label} complete!`);
    console.log(`   Records inserted: ${result.merged.toLocaleString()}`);

    return {
      chunk: chunk.label,
      fetched: records.length,
      inserted: result.merged
    };

  } catch (error) {
    console.error(`\n❌ Chunk ${chunk.label} failed:`, error.message);
    return {
      chunk: chunk.label,
      fetched: 0,
      inserted: 0,
      error: error.message
    };
  }
}

async function main() {
  const args = process.argv.slice(2);
  const entityName = args[0];
  const startYear = parseInt(args[1]) || 2020;

  if (!entityName || !ENTITY_CONFIG[entityName]) {
    console.error('\n❌ Usage: node backfill_entity.js <entity> [startYear]');
    console.error('\nAvailable entities:');
    Object.keys(ENTITY_CONFIG).forEach(entity => {
      console.error(`  - ${entity}`);
    });
    console.error('\nExample: node backfill_entity.js jobs 2020\n');
    process.exit(1);
  }

  const config = ENTITY_CONFIG[entityName];

  console.log('\n' + '='.repeat(70));
  console.log(`  ${entityName.toUpperCase()} BACKFILL - By createdOn (Historical)`);
  console.log('='.repeat(70));

  const chunks = generateYearlyChunks(startYear);

  console.log(`\n📅 Generated ${chunks.length} yearly chunks (${startYear}-present):`);
  chunks.forEach((chunk, i) => {
    console.log(`   ${i + 1}. ${chunk.label}`);
  });

  console.log(`\n🚀 Starting chunked backfill...`);
  console.log(`   You can stop at any time with Ctrl+C`);
  console.log(`   Progress will be saved after each chunk\n`);

  const results = [];

  for (let i = 0; i < chunks.length; i++) {
    // Ensure table exists on first chunk
    const ensureTable = (i === 0);
    const result = await backfillChunk(entityName, config, chunks[i], i + 1, chunks.length, ensureTable);
    results.push(result);

    // Short pause between chunks
    if (i < chunks.length - 1) {
      console.log(`\n⏸️  Pausing 2 seconds before next chunk...`);
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }

  // Summary
  console.log('\n' + '='.repeat(70));
  console.log('  BACKFILL COMPLETE - SUMMARY');
  console.log('='.repeat(70));

  const totalFetched = results.reduce((sum, r) => sum + r.fetched, 0);
  const totalInserted = results.reduce((sum, r) => sum + r.inserted, 0);
  const totalErrors = results.filter(r => r.error).length;

  console.log(`\n📊 Results by chunk:`);
  results.forEach(r => {
    const status = r.error ? '❌' : '✅';
    console.log(`   ${status} ${r.chunk}: ${r.inserted.toLocaleString()} records ${r.error ? `(Error: ${r.error})` : ''}`);
  });

  console.log(`\n📈 Totals:`);
  console.log(`   Total fetched: ${totalFetched.toLocaleString()}`);
  console.log(`   Total inserted: ${totalInserted.toLocaleString()}`);
  console.log(`   Successful chunks: ${chunks.length - totalErrors}/${chunks.length}`);
  console.log(`   Failed chunks: ${totalErrors}`);

  console.log('\n✨ Done!\n');
}

main().catch(error => {
  console.error('\n❌ Backfill script failed:', error);
  process.exit(1);
});
