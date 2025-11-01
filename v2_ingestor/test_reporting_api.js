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

async function testReportingAPI() {
    try {
        console.log('Getting access token...');
        const token = await getAccessToken();

        // Test 1: Get report metadata/schema first
        console.log('\n=== TEST 1: Fetching Report Metadata ===');
        const reportCategory = 'operations';
        const reportId = '389438975';

        const metadataUrl = `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${reportCategory}/reports/${reportId}`;

        console.log(`URL: ${metadataUrl}`);

        // Test with a small date range first (just 1 week)
        const params = new URLSearchParams({
            from: '2025-10-20',
            to: '2025-10-26',
            pageSize: '10'  // Small page size for testing
        });

        const reportUrl = `${metadataUrl}?${params.toString()}`;
        console.log(`\nFull URL: ${reportUrl}`);

        console.log('\nFetching report data (this may take 30-60 seconds)...');
        const startTime = Date.now();

        const response = await fetch(reportUrl, {
            headers: {
                'Authorization': `Bearer ${token}`,
                'ST-App-Key': APP_KEY,
                'Content-Type': 'application/json'
            },
            timeout: 120000  // 2 minute timeout
        });

        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`Response received in ${elapsed}s`);
        console.log(`Status: ${response.status} ${response.statusText}`);

        if (response.ok) {
            const data = await response.json();

            console.log('\n=== RESPONSE STRUCTURE ===');
            console.log('Keys:', Object.keys(data));

            if (data.data) {
                console.log(`\nTotal records: ${data.data.length}`);

                if (data.data.length > 0) {
                    console.log('\n=== FIRST RECORD (Full) ===');
                    console.log(JSON.stringify(data.data[0], null, 2));

                    console.log('\n=== COLUMN NAMES ===');
                    console.log(Object.keys(data.data[0]));

                    // Check for dynamicValues
                    if (data.data[0].dynamicValues) {
                        console.log('\n=== DYNAMIC VALUES ===');
                        console.log(JSON.stringify(data.data[0].dynamicValues, null, 2));
                    }

                    // Show summary of all records
                    console.log('\n=== SAMPLE DATA (first 3 records) ===');
                    data.data.slice(0, 3).forEach((record, i) => {
                        console.log(`\nRecord ${i + 1}:`);
                        console.log(`  Job ID: ${record.jobId || record.id}`);
                        console.log(`  Revenue: $${record.jobsSubtotal || record.revenue || 'N/A'}`);
                        console.log(`  Materials: $${record.materialsEquipPOBillCosts || record.materials || 'N/A'}`);
                        console.log(`  GPM%: ${record.jobsGrossMarginPercent || record.gpm || 'N/A'}%`);
                    });
                }
            }

            if (data.page || data.pageSize || data.totalCount) {
                console.log('\n=== PAGINATION INFO ===');
                console.log(`Page: ${data.page}`);
                console.log(`Page Size: ${data.pageSize}`);
                console.log(`Total Count: ${data.totalCount}`);
                console.log(`Has More: ${data.hasMore}`);
            }

            // Save full response for analysis
            const outputFile = 'reporting_api_response.json';
            fs.writeFileSync(outputFile, JSON.stringify(data, null, 2));
            console.log(`\n✅ Full response saved to: ${outputFile}`);

        } else {
            const errorText = await response.text();
            console.error('\n❌ Error response:', errorText);
        }

    } catch (error) {
        console.error('\n❌ Error:', error.message);
        if (error.code === 'ETIMEDOUT') {
            console.error('Request timed out - Reporting API may be slow or unavailable');
        }
    }
}

testReportingAPI();