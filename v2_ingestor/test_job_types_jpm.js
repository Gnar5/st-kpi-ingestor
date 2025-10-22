/**
 * Test JPM job-types endpoint
 */

import 'dotenv/config';
import { ServiceTitanClient } from './src/api/servicetitan_client.js';

async function main() {
  console.log('\n='.repeat(70));
  console.log('  TESTING JPM JOB-TYPES ENDPOINT');
  console.log('='.repeat(70));

  const client = new ServiceTitanClient();

  try {
    console.log('\n⏳ Fetching job types from jpm/v2/tenant/{tenant}/job-types...');
    const jobTypes = await client.fetchAll('jpm/v2/tenant/{tenant}/job-types', { pageSize: 10 });

    console.log(`\n✅ Success! Found ${jobTypes.length} job types`);

    if (jobTypes.length > 0) {
      console.log('\n📋 Sample job types:');
      jobTypes.slice(0, 10).forEach(jt => {
        console.log(`   - ID: ${jt.id}, Name: ${jt.name}, Active: ${jt.active}`);
      });

      console.log('\n📄 Full first record:');
      console.log(JSON.stringify(jobTypes[0], null, 2));
    }

    console.log('\n✨ Done!\n');

  } catch (error) {
    console.error('\n❌ Failed:', error.message);
    process.exit(1);
  }
}

main();
