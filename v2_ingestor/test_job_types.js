/**
 * Test script to ingest job types reference data
 */

import 'dotenv/config';
import { ServiceTitanClient } from './src/api/servicetitan_client.js';
import { BigQueryClient } from './src/bq/bigquery_client.js';
import { JobTypesIngestor } from './src/ingestors/job_types.js';

async function main() {
  console.log('\n='.repeat(70));
  console.log('  JOB TYPES REFERENCE DATA INGESTION');
  console.log('='.repeat(70));

  const stClient = new ServiceTitanClient();
  const bqClient = new BigQueryClient();
  const ingestor = new JobTypesIngestor(stClient, bqClient);

  try {
    console.log('\n⏳ Fetching job types from ServiceTitan...');
    const jobTypes = await stClient.fetchAll('jpm/v2/tenant/{tenant}/job-types');

    console.log(`✅ Fetched ${jobTypes.length} job types`);

    // Show sample
    console.log('\n📋 Sample job types:');
    jobTypes.slice(0, 10).forEach(jt => {
      console.log(`   - ${jt.name} (ID: ${jt.id}, Active: ${jt.active})`);
    });

    // Check for estimate-related types
    const estimateTypes = jobTypes.filter(jt =>
      jt.name && jt.name.toUpperCase().includes('ESTIMATE')
    );
    console.log(`\n🎯 Found ${estimateTypes.length} estimate-related job types`);

    // Check for warranty types
    const warrantyTypes = jobTypes.filter(jt =>
      jt.name && (jt.name.toUpperCase().includes('WARRANTY') || jt.name.toUpperCase().includes('TOUCHUP'))
    );
    console.log(`🔧 Found ${warrantyTypes.length} warranty/touchup job types`);

    console.log('\n🔄 Transforming job types...');
    const transformed = await ingestor.transform(jobTypes);

    console.log(`\n📊 Inserting ${transformed.length} job types to BigQuery...`);

    // Ensure table exists first
    const schema = ingestor.getSchema();
    await bqClient.ensureTable('st_ref_v2', 'dim_job_types', schema, {
      clusterFields: ['active', 'name']
    });

    // Use st_ref_v2 dataset like business_units
    const result = await bqClient.upsert(
      'st_ref_v2',  // Use reference dataset
      'dim_job_types',  // Match naming convention of dim_business_units
      transformed,
      'id'
    );

    console.log(`\n✅ Successfully inserted ${result.merged} job types!`);

    console.log('\n📈 Summary:');
    console.log(`   Total job types: ${jobTypes.length}`);
    console.log(`   Active: ${jobTypes.filter(jt => jt.active).length}`);
    console.log(`   Inactive: ${jobTypes.filter(jt => !jt.active).length}`);
    console.log(`   Estimate types: ${estimateTypes.length}`);
    console.log(`   Warranty types: ${warrantyTypes.length}`);

    console.log('\n✨ Done!\n');

  } catch (error) {
    console.error('\n❌ Job types ingestion failed:', error.message);
    console.error('Stack:', error.stack);
    process.exit(1);
  }
}

main();
