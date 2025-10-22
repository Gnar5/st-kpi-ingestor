/**
 * Chunked Backfill Script
 * Processes estimates in yearly chunks to avoid long-running processes
 * Shows byte-based batching in action
 */

import 'dotenv/config';
import { EstimatesIngestor } from './src/ingestors/estimates.js';
import { ServiceTitanClient } from './src/api/servicetitan_client.js';
import { BigQueryClient } from './src/bq/bigquery_client.js';

// Generate yearly date ranges from 2020 to present
function generateYearlyChunks() {
  const chunks = [];
  const currentYear = new Date().getFullYear();

  for (let year = 2020; year <= currentYear; year++) {
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

async function backfillChunk(chunk, chunkNum, totalChunks) {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`📦 CHUNK ${chunkNum}/${totalChunks}: ${chunk.label}`);
  console.log(`   Date range: ${chunk.startDate.split('T')[0]} to ${chunk.endDate.split('T')[0]}`);
  console.log('='.repeat(60));

  const stClient = new ServiceTitanClient();
  const bqClient = new BigQueryClient();
  const ingestor = new EstimatesIngestor(stClient, bqClient);

  try {
    // Fetch estimates for this date range
    console.log(`\n⏳ Fetching estimates created in ${chunk.label}...`);
    const estimates = await stClient.fetchAll('sales/v2/tenant/{tenant}/estimates', {
      createdOnOrAfter: chunk.startDate,
      createdBefore: chunk.endDate
    });

    console.log(`✅ Fetched ${estimates.length.toLocaleString()} estimates`);

    if (estimates.length === 0) {
      console.log('   ℹ️  No estimates for this period, skipping...');
      return { chunk: chunk.label, fetched: 0, inserted: 0 };
    }

    // Transform data
    console.log(`\n🔄 Transforming ${estimates.length.toLocaleString()} estimates...`);
    const transformed = await ingestor.transform(estimates);

    // Insert with byte-based batching
    console.log(`\n📊 Inserting to BigQuery with BYTE-BASED BATCHING...`);
    console.log(`   (Watch for batch size in MB in the logs below)`);

    const result = await bqClient.upsert(
      bqClient.datasetRaw,
      'raw_estimates',
      transformed,
      'id',
      {
        useByteBatching: true,  // Enable byte-based batching
        maxBytes: 8 * 1024 * 1024  // 8MB limit
      }
    );

    console.log(`\n✅ Chunk ${chunk.label} complete!`);
    console.log(`   Records inserted: ${result.merged.toLocaleString()}`);

    return {
      chunk: chunk.label,
      fetched: estimates.length,
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
  console.log('\n' + '='.repeat(70));
  console.log('  CHUNKED ESTIMATES BACKFILL - By createdOn (Historical)');
  console.log('='.repeat(70));

  const chunks = generateYearlyChunks();

  console.log(`\n📅 Generated ${chunks.length} yearly chunks:`);
  chunks.forEach((chunk, i) => {
    console.log(`   ${i + 1}. ${chunk.label}`);
  });

  console.log(`\n🚀 Starting chunked backfill...`);
  console.log(`   You can stop at any time with Ctrl+C`);
  console.log(`   Progress will be saved after each chunk\n`);

  const results = [];

  for (let i = 0; i < chunks.length; i++) {
    const result = await backfillChunk(chunks[i], i + 1, chunks.length);
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
