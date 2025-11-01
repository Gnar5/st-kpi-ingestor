#!/usr/bin/env node

/**
 * Test Leads and BA Customers & Revenue reports
 * These reports were found in the marketing category
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

async function testReport(token, reportId, reportName, category) {
    console.log(`\n${'='.repeat(80)}`);
    console.log(`Testing: ${reportName}`);
    console.log(`Report ID: ${reportId}`);
    console.log(`Category: ${category}`);
    console.log('='.repeat(80));

    // Get metadata
    const metadataUrl = `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${category}/reports/${reportId}`;

    const metaResponse = await fetch(metadataUrl, {
        headers: {
            'Authorization': `Bearer ${token}`,
            'ST-App-Key': APP_KEY,
            'Content-Type': 'application/json'
        }
    });

    if (!metaResponse.ok) {
        console.log(`❌ Cannot access metadata (${metaResponse.status})`);
        return;
    }

    const metadata = await metaResponse.json();
    console.log(`\n✅ Report: ${metadata.name}`);
    console.log(`Description: ${metadata.description || 'N/A'}`);

    console.log(`\nParameters (${metadata.parameters?.length || 0}):`);
    metadata.parameters?.forEach(p => {
        console.log(`  - ${p.label} (${p.name}): ${p.dataType}${p.isRequired ? ' [REQUIRED]' : ''}`);
    });

    console.log(`\nFields (${metadata.fields?.length || 0}):`);
    metadata.fields?.slice(0, 15).forEach(f => {
        console.log(`  - ${f.label} (${f.name}): ${f.dataType}`);
    });
    if (metadata.fields?.length > 15) {
        console.log(`  ... and ${metadata.fields.length - 15} more fields`);
    }

    // Try to fetch data
    console.log(`\n=== Fetching Data (10/20-10/26) ===`);

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

    console.log('Request body:', JSON.stringify(requestBody, null, 2));

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
        console.log(`❌ Data fetch failed:`);
        console.log(errorText);
        return;
    }

    const data = await dataResponse.json();

    console.log('\n✅ Data fetch successful!');
    console.log(`Response keys: ${Object.keys(data).join(', ')}`);

    if (data.data) {
        console.log(`Total records: ${data.data.length}`);

        if (data.data.length > 0) {
            console.log('\n=== FIRST 3 RECORDS ===');
            data.data.slice(0, 3).forEach((record, i) => {
                console.log(`\nRecord ${i + 1}:`);
                console.log(JSON.stringify(record, null, 2));
            });
        }

        // Save to file
        const filename = `${reportName.toLowerCase().replace(/[^a-z0-9]+/g, '_')}_data.json`;
        fs.writeFileSync(filename, JSON.stringify(data, null, 2));
        console.log(`\n✅ Full data saved to: ${filename}`);
    } else if (data.items) {
        console.log(`Total items: ${data.items.length}`);

        if (data.items.length > 0) {
            console.log('\n=== FIRST 3 ITEMS ===');
            data.items.slice(0, 3).forEach((item, i) => {
                console.log(`\nItem ${i + 1}:`);
                console.log(JSON.stringify(item, null, 2));
            });
        }

        // Save to file
        const filename = `${reportName.toLowerCase().replace(/[^a-z0-9]+/g, '_')}_data.json`;
        fs.writeFileSync(filename, JSON.stringify(data, null, 2));
        console.log(`\n✅ Full data saved to: ${filename}`);
    }
}

async function main() {
    try {
        console.log('Getting access token...');
        const token = await getAccessToken();

        // Test Leads report
        await testReport(token, '389357017', '*Leads* - API', 'marketing');

        // Test BA Customers & Revenue (mentions close rate!)
        await testReport(token, '132073290', '*BA Customers & Revenue*', 'marketing');

        // Test BMG - # of Sold Jobs (filters by Sold On date!)
        await testReport(token, '40761142', 'BMG - # of Sold Jobs, Amount, Lead Source', 'marketing');

    } catch (error) {
        console.error('\n❌ Error:', error.message);
        if (error.stack) {
            console.error(error.stack);
        }
    }
}

main();
