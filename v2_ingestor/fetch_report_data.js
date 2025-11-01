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

async function fetchReportData() {
    try {
        console.log('Getting access token...');
        const token = await getAccessToken();

        const reportCategory = 'operations';
        const reportId = '389438975';

        // Execute the report (POST request with parameters)
        const executeUrl = `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${reportCategory}/reports/${reportId}`;

        const reportParams = {
            parameters: {
                DateType: 1,  // Scheduled Date (usually)
                From: '2025-10-20',
                To: '2025-10-26',
                BusinessUnitId: [],  // Empty = all BUs
                IncludeAdjustmentInvoices: false
            }
        };

        console.log('\n=== EXECUTING REPORT ===');
        console.log(`URL: ${executeUrl}`);
        console.log(`Parameters:`, JSON.stringify(reportParams, null, 2));
        console.log('\nThis may take 30-120 seconds...');

        const startTime = Date.now();

        const response = await fetch(executeUrl, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${token}`,
                'ST-App-Key': APP_KEY,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(reportParams),
            timeout: 180000  // 3 minute timeout
        });

        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`\nResponse received in ${elapsed}s`);
        console.log(`Status: ${response.status} ${response.statusText}`);

        if (response.ok) {
            const data = await response.json();

            console.log('\n=== RESPONSE STRUCTURE ===');
            console.log('Keys:', Object.keys(data));

            if (data.data && Array.isArray(data.data)) {
                console.log(`\nTotal records: ${data.data.length}`);

                if (data.data.length > 0) {
                    console.log('\n=== FIRST RECORD ===');
                    console.log(JSON.stringify(data.data[0], null, 2));

                    console.log('\n=== ALL COLUMN NAMES ===');
                    console.log(Object.keys(data.data[0]).join(', '));

                    // Show first 5 records summary
                    console.log('\n=== SAMPLE DATA (first 5) ===');
                    data.data.slice(0, 5).forEach((record, i) => {
                        console.log(`\n${i + 1}. Job: ${record.JobId || record.JobNumber || 'N/A'}`);
                        console.log(`   BU: ${record.JobBusinessUnit || record.BusinessUnit || 'N/A'}`);
                        console.log(`   Revenue: $${record.JobsSubtotal || record.Revenue || 'N/A'}`);
                        console.log(`   Labor: $${record.LaborPay || record.Labor || 'N/A'}`);
                        console.log(`   Materials: $${record['Materials + Equip. + PO/Bill Costs'] || record.Materials || 'N/A'}`);
                        console.log(`   GPM%: ${record.JobsGrossMarginPercent || record.GPM || 'N/A'}%`);
                    });

                    // Check pagination
                    if (data.hasMore || data.nextPage) {
                        console.log('\n⚠️  More pages available!');
                        console.log(`Has More: ${data.hasMore}`);
                        console.log(`Next Page: ${data.nextPage}`);
                    }
                }

                // Save to file
                fs.writeFileSync('report_data.json', JSON.stringify(data, null, 2));
                console.log(`\n✅ Full data saved to: report_data.json`);

            } else if (data.jobId) {
                // Async report - returns job ID to poll
                console.log('\n⏳ Report is processing asynchronously');
                console.log(`Job ID: ${data.jobId}`);
                console.log('You need to poll for results using the job ID');
            } else {
                console.log('\n❓ Unexpected response format:');
                console.log(JSON.stringify(data, null, 2));
            }

        } else {
            const errorText = await response.text();
            console.error('\n❌ Error:', errorText);
        }

    } catch (error) {
        console.error('\n❌ Error:', error.message);
        if (error.code === 'ETIMEDOUT') {
            console.error('Request timed out - Report may be too large or API is slow');
        }
    }
}

fetchReportData();