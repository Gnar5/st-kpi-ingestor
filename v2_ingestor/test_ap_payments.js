#!/usr/bin/env node

import fetch from 'node-fetch';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config({ path: path.join(__dirname, '.env') });

const CLIENT_ID = process.env.ST_CLIENT_ID;
const CLIENT_SECRET = process.env.ST_CLIENT_SECRET;
const TENANT_ID = process.env.ST_TENANT_ID;
const APP_KEY = process.env.ST_APP_KEY;

async function getAccessToken() {
    const authUrl = 'https://auth.servicetitan.io/connect/token';
    const params = new URLSearchParams({
        grant_type: 'client_credentials',
        client_id: CLIENT_ID,
        client_secret: CLIENT_SECRET
    });

    const response = await fetch(authUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: params.toString()
    });

    if (!response.ok) {
        throw new Error(`Auth failed: ${response.statusText}`);
    }

    const data = await response.json();
    return data.access_token;
}

async function testAPPayments() {
    try {
        console.log('Getting access token...');
        const token = await getAccessToken();

        // Test AP Payments endpoint
        const apPaymentsUrl = `https://api.servicetitan.io/accounting/v2/tenant/${TENANT_ID}/ap-payments?pageSize=10`;

        console.log('\nTesting AP Payments endpoint...');
        console.log(`URL: ${apPaymentsUrl}`);

        const response = await fetch(apPaymentsUrl, {
            headers: {
                'Authorization': `Bearer ${token}`,
                'ST-App-Key': APP_KEY,
                'Content-Type': 'application/json'
            }
        });

        console.log(`Status: ${response.status} ${response.statusText}`);

        if (response.ok) {
            const data = await response.json();
            console.log(`\nAP Payments found: ${data.totalCount || data.data?.length || 0}`);

            if (data.data && data.data.length > 0) {
                console.log('\nFirst AP Payment sample:');
                const first = data.data[0];
                console.log(JSON.stringify(first, null, 2));

                // Check for job-related fields
                const jobFields = Object.keys(first).filter(key =>
                    key.toLowerCase().includes('job') ||
                    key.toLowerCase().includes('project')
                );

                if (jobFields.length > 0) {
                    console.log('\n✅ Found job-related fields:', jobFields);
                } else {
                    console.log('\n⚠️  No direct job fields found in AP Payments');
                }

                // Show summary
                console.log('\n=== AP Payments Summary ===');
                data.data.slice(0, 5).forEach((payment, i) => {
                    console.log(`${i+1}. ID: ${payment.id}`);
                    console.log(`   Vendor: ${payment.vendorId || 'N/A'}`);
                    console.log(`   Amount: $${payment.total || payment.amount || 0}`);
                    console.log(`   Date: ${payment.referenceDate || payment.date || 'N/A'}`);
                    if (payment.memo) console.log(`   Memo: ${payment.memo}`);
                    console.log('');
                });
            }
        } else {
            const errorText = await response.text();
            console.log('Error response:', errorText);
        }

    } catch (error) {
        console.error('Error:', error);
    }
}

testAPPayments();