#!/usr/bin/env node

/**
 * Test script to pull from ServiceTitan's "Daily WBR C/R" report
 * Report ID: 130700652 (from config.json)
 * Date range: 10/20/2025 - 10/26/2025 (validation week)
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

async function testReportAccess(token, reportId, reportName) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`Testing: ${reportName} (ID: ${reportId})`);
    console.log('='.repeat(60));

    // Try different URL patterns
    const urlPatterns = [
        // Standard category-based URLs
        { pattern: 'sales category', url: `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/sales/reports/${reportId}` },
        { pattern: 'operations category', url: `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/operations/reports/${reportId}` },
        { pattern: 'accounting category', url: `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/accounting/reports/${reportId}` },
        { pattern: 'service category', url: `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/service/reports/${reportId}` },
        // Direct URL without category
        { pattern: 'no category', url: `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/reports/${reportId}` },
    ];

    let metadata = null;
    let successPattern = null;

    for (const { pattern, url } of urlPatterns) {
        const response = await fetch(url, {
            headers: {
                'Authorization': `Bearer ${token}`,
                'ST-App-Key': APP_KEY,
                'Content-Type': 'application/json'
            }
        });

        if (response.ok) {
            metadata = await response.json();
            successPattern = pattern;
            console.log(`✅ FOUND via ${pattern}`);
            console.log(`   Name: ${metadata.name}`);
            console.log(`   Description: ${metadata.description || 'N/A'}`);
            console.log(`   Parameters: ${metadata.parameters?.length || 0}`);
            console.log(`   Fields: ${metadata.fields?.length || 0}`);
            return { reportId, reportName, metadata, successPattern, url };
        } else {
            console.log(`  ❌ ${pattern}: ${response.status}`);
        }
    }

    console.log(`❌ Report ${reportId} not accessible via any URL pattern`);
    return null;
}

async function testDailyWbrCr() {
    try {
        console.log('Getting access token...');
        const token = await getAccessToken();

        console.log('\n' + '='.repeat(80));
        console.log('TESTING ALL REPORT IDs FROM CONFIG.JSON');
        console.log('='.repeat(80));

        // Test all report IDs from config.json
        const reportsToTest = [
            { id: '389357017', name: 'Leads Report' },
            { id: '130700652', name: 'Daily WBR C/R' },
            { id: '397555674', name: 'Daily WBR Consolidated' },
            { id: '389438975', name: 'Foreman Job Cost This Week' },
            { id: '26117979', name: 'Collections (known working)' },
            { id: '387935289', name: 'Phoenix-Sales Daily WBR' },
            { id: '387936790', name: "Andy's Painting-Sales Daily WBR" },
            { id: '387930556', name: 'Commercial-AZ-Sales Daily WBR' },
            { id: '387945629', name: 'Guaranteed Painting-Sales Daily WBR' },
            { id: '387945741', name: 'Nevada-Sales Daily WBR' },
            { id: '387951872', name: 'Tucson-Sales Daily WBR' },
        ];

        const results = [];

        for (const report of reportsToTest) {
            const result = await testReportAccess(token, report.id, report.name);
            if (result) {
                results.push(result);
            }
            // Small delay to avoid rate limits
            await new Promise(resolve => setTimeout(resolve, 500));
        }

        console.log('\n' + '='.repeat(80));
        console.log('SUMMARY OF ACCESSIBLE REPORTS');
        console.log('='.repeat(80));

        if (results.length === 0) {
            console.log('❌ NO REPORTS WERE ACCESSIBLE');
            console.log('\nPossible reasons:');
            console.log('1. Reports may have been deleted or archived');
            console.log('2. Report IDs may be from a different tenant');
            console.log('3. API permissions may not include these reports');
            console.log('4. Reports may be in custom categories not tested');
            return;
        }

        console.log(`✅ Found ${results.length} accessible reports:\n`);
        results.forEach((result, i) => {
            console.log(`${i + 1}. ${result.reportName}`);
            console.log(`   ID: ${result.reportId}`);
            console.log(`   Access: ${result.successPattern}`);
            console.log(`   Name: ${result.metadata.name}`);
            console.log('');
        });

        // If we found at least one accessible report, test fetching data from it
        if (results.length > 0) {
            const testReport = results[0];
            console.log('='.repeat(80));
            console.log(`TESTING DATA FETCH: ${testReport.reportName}`);
            console.log('='.repeat(80));

            // Extract category from the successful URL if it exists
            let dataUrl;
            if (testReport.successPattern.includes('category')) {
                const category = testReport.successPattern.split(' ')[0];
                dataUrl = `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${category}/reports/${testReport.reportId}/data`;
            } else {
                dataUrl = `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/reports/${testReport.reportId}/data`;
            }

            console.log('\nFetching data for validation week (10/20-10/26)...');
            console.log('Using POST with request body (Reporting API pattern)');
            const startTime = Date.now();

            // Build request body - Reporting API uses POST with pagination and parameters
            // Parameters must be an array of { name, value } objects
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
            console.log(`Response received in ${elapsed}s`);
            console.log(`Status: ${dataResponse.status} ${dataResponse.statusText}`);

            if (!dataResponse.ok) {
                const errorText = await dataResponse.text();
                console.log(`❌ Data fetch failed: ${errorText}`);
                return;
            }

            const data = await dataResponse.json();
            console.log('\n✅ Data fetch successful!');
            console.log(`Total records: ${data.data?.length || 0}`);

            if (data.data && data.data.length > 0) {
                console.log('\nFirst record sample:');
                console.log(JSON.stringify(data.data[0], null, 2));

                // Save to file
                const filename = `${testReport.reportName.toLowerCase().replace(/\s+/g, '_')}_response.json`;
                fs.writeFileSync(filename, JSON.stringify(data, null, 2));
                console.log(`\n✅ Full response saved to: ${filename}`);
            }
        }

    } catch (error) {
        console.error('\n❌ Error:', error.message);
        if (error.stack) {
            console.error(error.stack);
        }
    }
}

testDailyWbrCr();
