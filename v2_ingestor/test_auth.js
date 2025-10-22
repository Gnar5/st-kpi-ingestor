import axios from 'axios';
import dotenv from 'dotenv';

dotenv.config();

async function testAuth() {
  console.log('Testing ServiceTitan Authentication...\n');
  
  console.log('Environment Variables:');
  console.log('  ST_CLIENT_ID:', process.env.ST_CLIENT_ID?.substring(0, 10) + '...');
  console.log('  ST_CLIENT_SECRET:', process.env.ST_CLIENT_SECRET?.substring(0, 10) + '...');
  console.log('  ST_TENANT_ID:', process.env.ST_TENANT_ID);
  console.log('  ST_APP_KEY:', process.env.ST_APP_KEY?.substring(0, 10) + '...');
  console.log('');
  
  const authUrl = 'https://auth.servicetitan.io/connect/token';
  const params = new URLSearchParams({
    grant_type: 'client_credentials',
    client_id: process.env.ST_CLIENT_ID,
    client_secret: process.env.ST_CLIENT_SECRET
  });
  
  try {
    console.log('Sending auth request to:', authUrl);
    console.log('Grant type: client_credentials\n');
    
    const response = await axios.post(authUrl, params, {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
    });
    
    console.log('✅ Authentication successful!');
    console.log('Token type:', response.data.token_type);
    console.log('Expires in:', response.data.expires_in, 'seconds');
    console.log('Access token:', response.data.access_token?.substring(0, 20) + '...');
    
  } catch (error) {
    console.error('❌ Authentication failed!');
    console.error('Status:', error.response?.status);
    console.error('Status text:', error.response?.statusText);
    console.error('Error data:', JSON.stringify(error.response?.data, null, 2));
  }
}

testAuth();
