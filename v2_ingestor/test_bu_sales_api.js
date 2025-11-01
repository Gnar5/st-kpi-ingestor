#!/usr/bin/env node

/**
 * Test BU Sales - API report
 * This may be the business unit aggregated version
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

async function testReport(token, reportId, reportName) {
    console.log('\n' + '='.repeat(80));
    console.log(`${reportName.toUpperCase()}`);
    console.log('='.repeat(80));

    const category = 'technician';

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
    console.log(`Description: ${metadata.description}`);

    console.log(`\nParameters (${metadata.parameters?.length || 0}):`);
    metadata.parameters?.forEach(p => {
        console.log(`  - ${p.label} (${p.name}): ${p.dataType}${p.isRequired ? ' [REQUIRED]' : ''}`);
    });

    console.log(`\nFields (${metadata.fields?.length || 0}):`);
    metadata.fields?.forEach(f => {
        console.log(`  - ${f.label} (${f.name}): ${f.dataType}`);
    });

    // Fetch data
    console.log('\n--- Fetching data for 10/20-10/26 ---');

    const dataUrl = `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${category}/reports/${reportId}/data`;

    const requestBody = {
        request: { page: 1, pageSize: 5000 },
        parameters: [
            { name: 'From', value: '2025-10-20' },
            { name: 'To', value: '2025-10-26' }
        ]
    };

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
    console.log(`Response received in ${elapsed}s - Status: ${dataResponse.status}`);

    if (!dataResponse.ok) {
        const errorText = await dataResponse.text();
        console.log('❌ Error:', errorText);
        return;
    }

    const data = await dataResponse.json();
    console.log(`\n✅ Total records: ${data.data?.length || 0}`);

    if (data.data && data.data.length > 0) {
        console.log('\n--- ALL RECORDS ---\n');
        data.data.forEach((record, i) => {
            console.log(`Record ${i + 1}:`);
            console.log(JSON.stringify(record, null, 2));
        });

        // Aggregate if multiple records
        if (metadata.fields) {
            console.log('\n--- FIELD MAPPING ---');
            metadata.fields.forEach((f, idx) => {
                console.log(`[${idx}] ${f.label} (${f.name})`);
            });

            // Try to find totals
            const totals = {};
            metadata.fields.forEach((field, idx) => {
                const sum = data.data.reduce((acc, record) => {
                    const val = parseFloat(record[idx]);
                    return acc + (isNaN(val) ? 0 : val);
                }, 0);
                totals[field.label] = sum;
            });

            console.log('\n--- AGGREGATED TOTALS ---');
            Object.entries(totals).forEach(([label, value]) => {
                if (typeof value === 'number' && value !== 0) {
                    console.log(`${label}: ${value.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`);
                }
            });
        }

        // Save
        const filename = `${reportName.toLowerCase().replace(/[^a-z0-9]+/g, '_')}_data.json`;
        fs.writeFileSync(filename, JSON.stringify({ metadata, data }, null, 2));
        console.log(`\n✅ Saved to: ${filename}`);
    }
}

async function main() {
    try {
        console.log('Getting access token...');
        const token = await getAccessToken();

        // Test BU Sales - API (business unit aggregated version)
        await testReport(token, '397555674', 'BU Sales - API');

        // Also test one of the per-BU reports
        console.log('\n\n');
        await testReport(token, '387935289', 'Daily WBR - Phoenix-Res Sales (API)');

    } catch (error) {
        console.error('\n❌ Error:', error.message);
        if (error.stack) {
            console.error(error.stack);
        }
    }
}

main();
