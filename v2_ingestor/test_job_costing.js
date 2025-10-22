/**
 * Test script to find job costing data in ServiceTitan API
 * Trying various endpoints that might have cost/financial data
 */

import 'dotenv/config';
import { ServiceTitanClient } from './src/api/servicetitan_client.js';

async function testEndpoint(client, endpoint, description) {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`Testing: ${description}`);
  console.log(`Endpoint: ${endpoint}`);
  console.log('='.repeat(60));

  try {
    const data = await client.fetchAll(endpoint, { pageSize: 5 });
    console.log(`✅ Success! Found ${data.length} records`);

    if (data.length > 0) {
      console.log('\n📋 Sample record:');
      console.log(JSON.stringify(data[0], null, 2));
    }

    return { endpoint, success: true, count: data.length };
  } catch (error) {
    console.log(`❌ Failed: ${error.message}`);
    return { endpoint, success: false, error: error.message };
  }
}

async function main() {
  console.log('\n' + '='.repeat(70));
  console.log('  JOB COSTING DATA DISCOVERY');
  console.log('='.repeat(70));

  const client = new ServiceTitanClient();

  const endpoints = [
    // Accounting exports that might have job cost data
    { path: 'accounting/v2/tenant/{tenant}/export/job-costing', name: 'Job Costing Export' },
    { path: 'accounting/v2/tenant/{tenant}/export/jobs', name: 'Jobs Export (Accounting)' },

    // Pricebook / Budget codes
    { path: 'pricebook/v2/tenant/{tenant}/materials', name: 'Materials (Pricebook)' },
    { path: 'pricebook/v2/tenant/{tenant}/services', name: 'Services (Pricebook)' },
    { path: 'pricebook/v2/tenant/{tenant}/equipment', name: 'Equipment (Pricebook)' },

    // Inventory might track materials used
    { path: 'inventory/v2/tenant/{tenant}/adjustments', name: 'Inventory Adjustments' },

    // Settings for budget codes
    { path: 'settings/v2/tenant/{tenant}/business-units', name: 'Business Units' },
    { path: 'settings/v2/tenant/{tenant}/tag-types', name: 'Tag Types' },

    // Reporting API
    { path: 'reporting/v2/tenant/{tenant}/report-categories', name: 'Report Categories' }
  ];

  const results = [];

  for (const endpoint of endpoints) {
    const result = await testEndpoint(client, endpoint.path, endpoint.name);
    results.push({ ...result, name: endpoint.name });

    // Pause between requests
    await new Promise(resolve => setTimeout(resolve, 1000));
  }

  console.log('\n' + '='.repeat(70));
  console.log('  SUMMARY');
  console.log('='.repeat(70));

  console.log('\n✅ Working endpoints:');
  results.filter(r => r.success).forEach(r => {
    console.log(`   - ${r.name} (${r.count} records)`);
  });

  console.log('\n❌ Failed endpoints:');
  results.filter(r => !r.success).forEach(r => {
    console.log(`   - ${r.name}: ${r.error}`);
  });

  console.log('\n✨ Done!\n');
}

main();
