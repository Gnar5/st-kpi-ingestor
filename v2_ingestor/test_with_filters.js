import 'dotenv/config';
import axios from 'axios';

const ST_CLIENT_ID = process.env.ST_CLIENT_ID;
const ST_CLIENT_SECRET = process.env.ST_CLIENT_SECRET;
const ST_TENANT_ID = process.env.ST_TENANT_ID;
const ST_APP_KEY = process.env.ST_APP_KEY;

async function test() {
  // Auth
  const authResponse = await axios.post(
    'https://auth.servicetitan.io/connect/token',
    new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: ST_CLIENT_ID,
      client_secret: ST_CLIENT_SECRET
    }),
    { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
  );

  const token = authResponse.data.access_token;
  console.log('✅ Authenticated\n');

  // Test campaigns with date filter
  const testDate = '2025-10-14T00:00:00Z';
  
  console.log('Testing campaigns WITH date filter:', testDate);
  try {
    const response = await axios.get(
      `https://api.servicetitan.io/marketing/v2/tenant/${ST_TENANT_ID}/campaigns`,
      {
        headers: {
          'Authorization': `Bearer ${token}`,
          'ST-App-Key': ST_APP_KEY
        },
        params: {
          modifiedOnOrAfter: testDate,
          page: 1,
          pageSize: 1
        }
      }
    );
    console.log('✅ SUCCESS with modifiedOnOrAfter');
    console.log('   Records:', response.data.data.length);
  } catch (error) {
    console.log('❌ FAILED with modifiedOnOrAfter');
    console.log('   Error:', error.response?.data || error.message);
  }
}

test();
