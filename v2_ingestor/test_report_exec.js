#!/usr/bin/env node

import fetch from 'node-fetch';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';

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

async function testReportExecution() {
    try {
        console.log('Getting access token...');
        const token = await getAccessToken();

        const reportCategory = 'operations';
        const reportId = '389438975';

        // Try different endpoint patterns
        const attempts = [
            {
                name: 'GET with query params',
                method: 'GET',
                url: `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${reportCategory}/reports/${reportId}?from=2025-10-20&to=2025-10-26&DateType=1&pageSize=5`
            },
            {
                name: 'GET /data endpoint',
                method: 'GET',
                url: `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${reportCategory}/reports/${reportId}/data?from=2025-10-20&to=2025-10-26&pageSize=5`
            },
            {
                name: 'POST /execute endpoint',
                method: 'POST',
                url: `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${reportCategory}/reports/${reportId}/execute`,
                body: {
                    from: '2025-10-20',
                    to: '2025-10-26',
                    DateType: 1,
                    pageSize: 5
                }
            },
            {
                name: 'POST with parameters object',
                method: 'POST',
                url: `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${reportCategory}/reports/${reportId}`,
                body: {
                    parameters: {
                        From: '2025-10-20',
                        To: '2025-10-26',
                        DateType: 1
                    },
                    pageSize: 5
                }
            }
        ];

        for (const attempt of attempts) {
            console.log(`\n${'='.repeat(60)}`);
            console.log(`ATTEMPT: ${attempt.name}`);
            console.log(`Method: ${attempt.method}`);
            console.log(`URL: ${attempt.url}`);

            const options = {
                method: attempt.method,
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'ST-App-Key': APP_KEY,
                    'Content-Type': 'application/json'
                },
                timeout: 60000
            };

            if (attempt.body) {
                options.body = JSON.stringify(attempt.body);
                console.log(`Body:`, JSON.stringify(attempt.body, null, 2));
            }

            console.log('\nSending request...');
            const startTime = Date.now();

            try {
                const response = await fetch(attempt.url, options);
                const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

                console.log(`Response in ${elapsed}s: ${response.status} ${response.statusText}`);

                const contentType = response.headers.get('content-type');
                console.log(`Content-Type: ${contentType}`);

                if (response.ok) {
                    if (contentType?.includes('application/json')) {
                        const data = await response.json();
                        console.log('✅ SUCCESS!');
                        console.log('Response keys:', Object.keys(data));

                        if (data.data && Array.isArray(data.data)) {
                            console.log(`Records: ${data.data.length}`);
                            if (data.data.length > 0) {
                                console.log('\nFirst record columns:', Object.keys(data.data[0]));
                                console.log('\nFirst record:', JSON.stringify(data.data[0], null, 2));
                            }
                        }

                        // Save successful response
                        fs.writeFileSync('successful_report_response.json', JSON.stringify(data, null, 2));
                        console.log('\n✅ Saved to successful_report_response.json');
                        return; // Stop after first success
                    } else {
                        const text = await response.text();
                        console.log('Response (non-JSON):', text.substring(0, 500));
                    }
                } else {
                    const errorText = await response.text();
                    console.log('❌ Error:', errorText.substring(0, 500));
                }
            } catch (error) {
                console.log('❌ Request failed:', error.message);
            }
        }

        console.log('\n' + '='.repeat(60));
        console.log('All attempts failed. The report might require a different API pattern.');
        console.log('You may need to contact ServiceTitan support for Reporting API documentation.');

    } catch (error) {
        console.error('Fatal error:', error.message);
    }
}

testReportExecution();