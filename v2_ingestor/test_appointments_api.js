/**
 * Test ServiceTitan Appointments API
 * Check what fields are available for job costing
 */

import axios from 'axios';
import 'dotenv/config';

const ST_CLIENT_ID = process.env.ST_CLIENT_ID || 'cid.0sx24a627mi8qx2wsuwbo4c68';
const ST_CLIENT_SECRET = process.env.ST_CLIENT_SECRET || 'cs1.k1jvmtimzb87n3phzewn1hbs662sncs6kr16nliwe9jwmbbpyh';
const ST_TENANT_ID = process.env.ST_TENANT_ID || '636913317';
const ST_APP_KEY = process.env.ST_APP_KEY || 'ak1.ustiqwarpotilgkmx5dhqzu6k';

async function getAccessToken() {
  const response = await axios.post(
    'https://auth.servicetitan.io/connect/token',
    'grant_type=client_credentials' +
    `&client_id=${ST_CLIENT_ID}` +
    `&client_secret=${ST_CLIENT_SECRET}`,
    {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
    }
  );
  return response.data.access_token;
}

async function testAppointmentsAPI() {
  console.log('Getting access token...');
  const token = await getAccessToken();

  console.log('\nFetching appointments for 8/18/2025...');
  const response = await axios.get(
    `https://api.servicetitan.io/jpm/v2/tenant/${ST_TENANT_ID}/appointments`,
    {
      headers: {
        'Authorization': `Bearer ${token}`,
        'ST-App-Key': ST_APP_KEY
      },
      params: {
        page: 1,
        pageSize: 2,
        startsOnOrAfter: '2025-08-18T00:00:00Z',
        startsBefore: '2025-08-19T00:00:00Z'
      }
    }
  );

  console.log('\nAppointments response:');
  console.log(JSON.stringify(response.data, null, 2));

  if (response.data.data && response.data.data.length > 0) {
    console.log('\n\nFirst appointment fields:');
    console.log(Object.keys(response.data.data[0]));
  }
}

testAppointmentsAPI().catch(console.error);
