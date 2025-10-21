#!/usr/bin/env node
/**
 * Direct API test - shows full ServiceTitan response including error details
 */

import 'dotenv/config';
import axios from 'axios';

const ST_CLIENT_ID = process.env.ST_CLIENT_ID;
const ST_CLIENT_SECRET = process.env.ST_CLIENT_SECRET;
const ST_TENANT_ID = process.env.ST_TENANT_ID;
const ST_APP_KEY = process.env.ST_APP_KEY;

console.log('Testing ServiceTitan API directly...\n');
console.log('Credentials:');
console.log('  Client ID:', ST_CLIENT_ID?.substring(0, 15) + '...');
console.log('  Tenant ID:', ST_TENANT_ID);
console.log('  App Key:', ST_APP_KEY?.substring(0, 15) + '...\n');

async function testAPI() {
  try {
    // Step 1: Authenticate
    console.log('Step 1: Authenticating...');
    const authResponse = await axios.post(
      'https://auth.servicetitan.io/connect/token',
      new URLSearchParams({
        grant_type: 'client_credentials',
        client_id: ST_CLIENT_ID,
        client_secret: ST_CLIENT_SECRET
      }),
      {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
      }
    );

    const token = authResponse.data.access_token;
    console.log('✅ Authentication successful!');
    console.log('   Token:', token.substring(0, 30) + '...');
    console.log('   Expires in:', authResponse.data.expires_in, 'seconds\n');

    // Step 2: Try different endpoints
    const endpoints = [
      { name: 'Campaigns', url: `https://api.servicetitan.io/marketing/v2/tenant/${ST_TENANT_ID}/campaigns` },
      { name: 'Customers', url: `https://api.servicetitan.io/crm/v2/tenant/${ST_TENANT_ID}/customers` },
      { name: 'Jobs', url: `https://api.servicetitan.io/jpm/v2/tenant/${ST_TENANT_ID}/jobs` },
      { name: 'Invoices', url: `https://api.servicetitan.io/accounting/v2/tenant/${ST_TENANT_ID}/invoices` }
    ];

    for (const endpoint of endpoints) {
      console.log(`\nStep 2: Testing ${endpoint.name} endpoint...`);
      console.log(`URL: ${endpoint.url}?pageSize=1`);

      try {
        const response = await axios.get(endpoint.url, {
          headers: {
            'Authorization': `Bearer ${token}`,
            'ST-App-Key': ST_APP_KEY,
            'Content-Type': 'application/json'
          },
          params: {
            page: 1,
            pageSize: 1
          }
        });

        console.log(`✅ ${endpoint.name} SUCCESS!`);
        console.log('   Status:', response.status);
        console.log('   Data count:', response.data?.data?.length || 0);
        console.log('   Total count:', response.data?.totalCount || 'N/A');
        console.log('   Sample:', JSON.stringify(response.data?.data?.[0] || {}, null, 2).substring(0, 200) + '...');

      } catch (error) {
        console.log(`❌ ${endpoint.name} FAILED`);
        console.log('   Status:', error.response?.status || 'N/A');
        console.log('   Status Text:', error.response?.statusText || 'N/A');

        // Show detailed error response
        if (error.response?.data) {
          console.log('   Error Details:', JSON.stringify(error.response.data, null, 2));
        } else {
          console.log('   Error:', error.message);
        }
      }
    }

  } catch (error) {
    console.error('❌ Test failed:', error.message);
    if (error.response?.data) {
      console.error('Error details:', error.response.data);
    }
  }
}

testAPI();
