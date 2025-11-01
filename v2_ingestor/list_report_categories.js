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

async function listReportCategories() {
    try {
        console.log('Getting access token...');
        const token = await getAccessToken();

        // List all report categories
        const categoriesUrl = `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-categories`;

        console.log(`\nFetching report categories...`);
        console.log(`URL: ${categoriesUrl}`);

        const response = await fetch(categoriesUrl, {
            headers: {
                'Authorization': `Bearer ${token}`,
                'ST-App-Key': APP_KEY,
                'Content-Type': 'application/json'
            }
        });

        console.log(`Status: ${response.status} ${response.statusText}`);

        if (response.ok) {
            const data = await response.json();

            console.log('\n=== REPORT CATEGORIES ===');
            if (data.data && Array.isArray(data.data)) {
                console.log(`Found ${data.data.length} categories:\n`);
                data.data.forEach((cat, i) => {
                    console.log(`${i + 1}. ${cat.name || cat.id}`);
                    console.log(`   ID: ${cat.id}`);
                    console.log(`   Display Name: ${cat.displayName || cat.name}`);
                    console.log('');
                });

                // Now try to list reports in each category
                console.log('\n=== FETCHING REPORTS PER CATEGORY ===');
                for (const category of data.data.slice(0, 3)) {  // Test first 3 categories
                    const catId = category.id || category.name;
                    console.log(`\nCategory: ${catId}`);

                    const reportsUrl = `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${catId}/reports`;

                    const reportsResp = await fetch(reportsUrl, {
                        headers: {
                            'Authorization': `Bearer ${token}`,
                            'ST-App-Key': APP_KEY,
                            'Content-Type': 'application/json'
                        }
                    });

                    if (reportsResp.ok) {
                        const reportsData = await reportsResp.json();
                        if (reportsData.data && reportsData.data.length > 0) {
                            console.log(`  Found ${reportsData.data.length} reports:`);
                            reportsData.data.forEach(report => {
                                console.log(`    - ${report.name} (ID: ${report.id})`);
                            });
                        } else {
                            console.log('  No reports found');
                        }
                    } else {
                        console.log(`  Failed to fetch reports: ${reportsResp.status}`);
                    }
                }
            } else {
                console.log('Unexpected response format');
                console.log(JSON.stringify(data, null, 2));
            }
        } else {
            const errorText = await response.text();
            console.error('Error:', errorText);
        }

    } catch (error) {
        console.error('Error:', error.message);
    }
}

listReportCategories();