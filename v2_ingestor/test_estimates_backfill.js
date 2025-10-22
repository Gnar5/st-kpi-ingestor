import { EstimatesIngestor } from './src/ingestors/estimates.js';
import { ServiceTitanClient } from './src/api/servicetitan_client.js';
import { BigQueryClient } from './src/bq/bigquery_client.js';

async function main() {
  console.log('🧪 Testing estimates backfill with byte-based batching...\n');
  
  const stClient = new ServiceTitanClient();
  const bqClient = new BigQueryClient();
  const ingestor = new EstimatesIngestor(stClient, bqClient);
  
  const result = await ingestor.ingest({ mode: 'full' });
  
  console.log('\n✅ Backfill complete!');
  console.log('Records processed:', result.recordsProcessed);
  console.log('Duration (ms):', result.duration);
}

main().catch(err => {
  console.error('\n❌ Backfill failed:', err.message);
  process.exit(1);
});
