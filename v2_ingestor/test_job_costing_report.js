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

function formatReportParameters(params) {
    // Convert parameters to ST Reporting API format
    return Object.entries(params).map(([key, value]) => ({
        name: key,
        value: value
    }));
}

async function fetchJobCostingReport() {
    try {
        console.log('Getting access token...');
        const token = await getAccessToken();

        const reportCategory = 'operations';
        const reportId = '389438975';  // *FOREMAN Job Cost - THIS WEEK ONLY* - API

        // Correct endpoint with /data
        const url = `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${reportCategory}/reports/${reportId}/data`;

        const parameters = {
            From: '2025-10-20',
            To: '2025-10-26',
            DateType: 1  // Scheduled Date
        };

        const body = {
            request: {
                page: 1,
                pageSize: 10  // Small page for testing
            },
            parameters: formatReportParameters(parameters)
        };

        console.log(`\n=== FETCHING JOB COSTING REPORT ===`);
        console.log(`URL: ${url}`);
        console.log(`Body:`, JSON.stringify(body, null, 2));
        console.log('\nThis may take 30-60 seconds...\n');

        const startTime = Date.now();

        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${token}`,
                'ST-App-Key': APP_KEY,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(body),
            timeout: 120000  // 2 minute timeout
        });

        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`Response received in ${elapsed}s`);
        console.log(`Status: ${response.status} ${response.statusText}`);

        if (response.ok) {
            const data = await response.json();

            console.log('\n=== SUCCESS! ===');
            console.log('Response keys:', Object.keys(data));

            // Check response structure
            if (data.fields && Array.isArray(data.fields)) {
                console.log(`\nFields (${data.fields.length} columns):`);
                data.fields.forEach((field, i) => {
                    console.log(`  ${i}: ${field.name} (${field.dataType})`);
                });
            }

            if (data.data && Array.isArray(data.data)) {
                console.log(`\nData: ${data.data.length} records`);

                if (data.data.length > 0) {
                    console.log('\n=== FIRST 3 RECORDS ===');
                    data.data.slice(0, 3).forEach((row, i) => {
                        console.log(`\nRecord ${i + 1} (array of ${row.length} values):`);
                        console.log(`  [0] Scheduled Date: ${row[0]}`);
                        console.log(`  [1] Business Unit: ${row[1]}`);
                        console.log(`  [6] Job Number: ${row[6]}`);
                        console.log(`  [7] Subtotal: $${row[7]}`);
                        console.log(`  [8] Labor Pay: $${row[8]}`);
                        console.log(`  [9] Payroll Adjustments: $${row[9]}`);
                        console.log(`  [10] Materials: $${row[10]}`);
                        console.log(`  [11] Returns: $${row[11]}`);
                        console.log(`  [12] Total Costs: $${row[12]}`);
                        console.log(`  [13] GPM%: ${row[13]}%`);
                        console.log(`  [14] Status: ${row[14]}`);
                    });
                }
            }

            // Pagination info
            if (data.hasMore !== undefined) {
                console.log(`\nHas More Pages: ${data.hasMore}`);
                console.log(`Page: ${data.page || 1}`);
                console.log(`Page Size: ${data.pageSize || body.request.pageSize}`);
                console.log(`Total Count: ${data.totalCount || 'unknown'}`);
            }

            // Save full response
            fs.writeFileSync('job_costing_report_data.json', JSON.stringify(data, null, 2));
            console.log(`\n✅ Full response saved to: job_costing_report_data.json`);

            return data;

        } else {
            const errorText = await response.text();
            console.error('\n❌ Error:', errorText);
        }

    } catch (error) {
        console.error('\n❌ Error:', error.message);
        if (error.code === 'ETIMEDOUT') {
            console.error('Request timed out - Reporting API may be slow');
        }
    }
}

fetchJobCostingReport();