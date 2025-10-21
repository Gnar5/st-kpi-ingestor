import dotenv from 'dotenv';
import { ServiceTitanClient } from './src/api/servicetitan_client.js';

dotenv.config();

const stClient = new ServiceTitanClient({
  clientId: process.env.ST_CLIENT_ID,
  clientSecret: process.env.ST_CLIENT_SECRET,
  appKey: process.env.ST_APP_KEY,
  tenantId: process.env.ST_TENANT_ID
});

async function test() {
  console.log('Testing Estimates API...\n');

  try {
    const estimates = await stClient.getEstimates();
    console.log(`✅ Estimates: ${estimates.length} records`);
    if (estimates.length > 0) {
      console.log('Sample estimate:', JSON.stringify(estimates[0], null, 2));
    }
  } catch (error) {
    console.error(`❌ Estimates failed:`, error.message);
    console.error('Status:', error.response?.status);
    console.error('Data:', error.response?.data);
  }

  console.log('\n---\n');
  console.log('Testing Payroll API...\n');

  try {
    const payroll = await stClient.getPayroll();
    console.log(`✅ Payroll: ${payroll.length} records`);
    if (payroll.length > 0) {
      console.log('Sample payroll:', JSON.stringify(payroll[0], null, 2));
    }
  } catch (error) {
    console.error(`❌ Payroll failed:`, error.message);
    console.error('Status:', error.response?.status);
    console.error('Data:', error.response?.data);
  }
}

test().catch(console.error);
