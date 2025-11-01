#!/usr/bin/env node

/**
 * Test Leads - API report with creation date filter
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

async function main() {
    try {
        console.log('Getting access token...');
        const token = await getAccessToken();

        console.log('\n' + '='.repeat(80));
        console.log('LEADS - API REPORT TEST');
        console.log('='.repeat(80));

        const reportId = '389357017';
        const category = 'marketing';

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
            throw new Error(`Metadata failed: ${metaResponse.status}`);
        }

        const metadata = await metaResponse.json();
        console.log(`\n✅ Report: ${metadata.name}`);
        console.log(`Description: ${metadata.description}`);

        console.log(`\nParameters:`);
        metadata.parameters?.forEach(p => {
            console.log(`  - ${p.label} (${p.name}): ${p.dataType}${p.isRequired ? ' [REQUIRED]' : ''}`);
            if (p.acceptValues?.values) {
                console.log(`    Possible values: ${p.acceptValues.values.map(v => `${v.Name}=${v.Value}`).join(', ')}`);
            }
        });

        console.log(`\nFields:`);
        metadata.fields?.forEach(f => {
            console.log(`  - ${f.label} (${f.name})`);
        });

        // Fetch data with DateType for creation date
        // Try different DateType values to find creation date
        const dateTypes = [
            { value: 1, name: 'Creation Date (guess)' },
            { value: 2, name: 'Modified Date (guess)' },
            { value: 3, name: 'Other Date (guess)' }
        ];

        for (const dt of dateTypes) {
            console.log('\n' + '='.repeat(80));
            console.log(`Testing DateType = ${dt.value} (${dt.name})`);
            console.log('='.repeat(80));

            const dataUrl = `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${category}/reports/${reportId}/data`;

            const requestBody = {
                request: { page: 1, pageSize: 5000 },
                parameters: [
                    { name: 'From', value: '2025-10-20' },
                    { name: 'To', value: '2025-10-26' },
                    { name: 'DateType', value: dt.value.toString() }
                ]
            };

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

            console.log(`Status: ${dataResponse.status}`);

            if (!dataResponse.ok) {
                const errorText = await dataResponse.text();
                console.log(`❌ Error: ${errorText.substring(0, 200)}`);
                continue;
            }

            const data = await dataResponse.json();
            console.log(`✅ Total leads: ${data.data?.length || 0}`);

            if (data.data && data.data.length > 0) {
                console.log(`\nFirst 5 leads:`);
                data.data.slice(0, 5).forEach((lead, i) => {
                    console.log(`${i + 1}. ${JSON.stringify(lead)}`);
                });

                // Save
                const filename = `leads_api_datetype_${dt.value}_data.json`;
                fs.writeFileSync(filename, JSON.stringify({ metadata, data }, null, 2));
                console.log(`\nSaved to: ${filename}`);

                if (data.data.length === 227 || Math.abs(data.data.length - 227) <= 5) {
                    console.log(`\n🎯 MATCH! This DateType gives ${data.data.length} leads (expected 227)`);
                }
            }
        }

    } catch (error) {
        console.error('\n❌ Error:', error.message);
        if (error.stack) {
            console.error(error.stack);
        }
    }
}

main();
