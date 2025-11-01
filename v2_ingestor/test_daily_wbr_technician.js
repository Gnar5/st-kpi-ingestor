#!/usr/bin/env node

/**
 * Test Daily WBR C/R report from TECHNICIAN category
 * This is the report that should have the baseline numbers
 */

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

async function testDailyWbrCr() {
    try {
        console.log('Getting access token...');
        const token = await getAccessToken();

        console.log('\n' + '='.repeat(80));
        console.log('DAILY WBR C/R REPORT TEST');
        console.log('='.repeat(80));

        const reportId = '130700652';
        const category = 'technician';

        // Get metadata
        console.log('\n1. Fetching report metadata...');
        const metadataUrl = `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${category}/reports/${reportId}`;

        const metaResponse = await fetch(metadataUrl, {
            headers: {
                'Authorization': `Bearer ${token}`,
                'ST-App-Key': APP_KEY,
                'Content-Type': 'application/json'
            }
        });

        if (!metaResponse.ok) {
            throw new Error(`Metadata fetch failed: ${metaResponse.status} ${metaResponse.statusText}`);
        }

        const metadata = await metaResponse.json();
        console.log(`\n✅ Report: ${metadata.name}`);
        console.log(`Description: ${metadata.description}`);

        console.log(`\nParameters (${metadata.parameters?.length || 0}):`);
        metadata.parameters?.forEach(p => {
            console.log(`  - ${p.label} (${p.name}): ${p.dataType}${p.isRequired ? ' [REQUIRED]' : ''}`);
        });

        console.log(`\nFields (${metadata.fields?.length || 0}):`);
        metadata.fields?.forEach(f => {
            console.log(`  - ${f.label} (${f.name}): ${f.dataType}`);
        });

        // Fetch data for validation week
        console.log('\n' + '='.repeat(80));
        console.log('2. Fetching data for week 10/20-10/26...');
        console.log('='.repeat(80));

        const dataUrl = `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${category}/reports/${reportId}/data`;

        const requestBody = {
            request: {
                page: 1,
                pageSize: 5000
            },
            parameters: [
                { name: 'From', value: '2025-10-20' },
                { name: 'To', value: '2025-10-26' }
            ]
        };

        console.log('\nRequest:');
        console.log(JSON.stringify(requestBody, null, 2));

        const startTime = Date.now();
        const dataResponse = await fetch(dataUrl, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${token}`,
                'ST-App-Key': APP_KEY,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestBody),
            timeout: 120000
        });

        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`\nResponse received in ${elapsed}s`);
        console.log(`Status: ${dataResponse.status} ${dataResponse.statusText}`);

        if (!dataResponse.ok) {
            const errorText = await dataResponse.text();
            console.log('\n❌ Data fetch failed:');
            console.log(errorText);
            return;
        }

        const data = await dataResponse.json();

        console.log('\n✅ Data fetch successful!');
        console.log(`Response keys: ${Object.keys(data).join(', ')}`);

        if (data.data) {
            console.log(`Total records: ${data.data.length}`);

            // Display all records (should be by business unit)
            console.log('\n' + '='.repeat(80));
            console.log('COMPLETE DATA (ALL BUSINESS UNITS)');
            console.log('='.repeat(80));

            let totalOpps = 0;
            let totalClosed = 0;
            let totalSales = 0;
            let totalCompleted = 0;

            data.data.forEach((record, i) => {
                console.log(`\nBusiness Unit ${i + 1}:`);
                console.log(JSON.stringify(record, null, 2));

                // Try to extract metrics (array format)
                if (Array.isArray(record)) {
                    // Based on field order from metadata
                    console.log(`\nParsed values:`);
                    record.forEach((val, idx) => {
                        console.log(`  [${idx}]: ${val}`);
                    });

                    // Aggregate totals (will need to identify which indices are what)
                    // Typically: opportunities, closed, sales, etc.
                }
            });

            console.log('\n' + '='.repeat(80));
            console.log('FIELD MAPPING');
            console.log('='.repeat(80));
            console.log('\nTo understand the data structure, here are the fields:');
            metadata.fields.forEach((f, idx) => {
                console.log(`[${idx}] ${f.label} (${f.name}): ${f.dataType}`);
            });

            // Save full response
            const filename = 'daily_wbr_cr_data.json';
            fs.writeFileSync(filename, JSON.stringify({
                metadata,
                data
            }, null, 2));
            console.log(`\n✅ Full response saved to: ${filename}`);

        } else if (data.items) {
            console.log(`Total items: ${data.items.length}`);
            console.log('\nFirst 5 items:');
            data.items.slice(0, 5).forEach((item, i) => {
                console.log(`\nItem ${i + 1}:`);
                console.log(JSON.stringify(item, null, 2));
            });
        }

    } catch (error) {
        console.error('\n❌ Error:', error.message);
        if (error.stack) {
            console.error(error.stack);
        }
    }
}

testDailyWbrCr();
