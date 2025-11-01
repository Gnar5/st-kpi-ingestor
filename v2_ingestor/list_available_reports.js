#!/usr/bin/env node

/**
 * List all available reports in ServiceTitan Reporting API
 *
 * This script discovers what reports exist in the account,
 * which may help identify the correct WBR report to use.
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

async function listReportsInCategory(token, category) {
    console.log(`\n${'='.repeat(80)}`);
    console.log(`CATEGORY: ${category}`);
    console.log('='.repeat(80));

    const url = `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/${category}/reports`;

    try {
        const response = await fetch(url, {
            headers: {
                'Authorization': `Bearer ${token}`,
                'ST-App-Key': APP_KEY,
                'Content-Type': 'application/json'
            }
        });

        if (!response.ok) {
            console.log(`❌ Cannot list reports (${response.status} ${response.statusText})`);
            return [];
        }

        const data = await response.json();

        // Check different possible response formats
        let reports = [];
        if (Array.isArray(data)) {
            reports = data;
        } else if (data.reports) {
            reports = data.reports;
        } else if (data.data) {
            reports = data.data;
        } else if (data.items) {
            reports = data.items;
        }

        console.log(`✅ Found ${reports.length} reports`);

        if (reports.length > 0) {
            console.log('\nReports:');
            reports.forEach((report, i) => {
                const id = report.id || report.reportId || report.Id || 'N/A';
                const name = report.name || report.reportName || report.Name || 'N/A';
                const desc = report.description || report.Description || '';

                console.log(`\n${i + 1}. ${name}`);
                console.log(`   ID: ${id}`);
                if (desc) {
                    console.log(`   Description: ${desc.substring(0, 100)}${desc.length > 100 ? '...' : ''}`);
                }
            });
        }

        return reports;

    } catch (error) {
        console.log(`❌ Error: ${error.message}`);
        return [];
    }
}

async function discoverReports() {
    try {
        console.log('Getting access token...');
        const token = await getAccessToken();

        console.log('\n' + '='.repeat(80));
        console.log('DISCOVERING AVAILABLE REPORTS IN SERVICETITAN');
        console.log('='.repeat(80));

        const categories = ['sales', 'operations', 'accounting', 'service', 'marketing', 'customers', 'technician'];
        const allReports = {};

        for (const category of categories) {
            const reports = await listReportsInCategory(token, category);
            allReports[category] = reports;

            // Small delay to avoid rate limits
            await new Promise(resolve => setTimeout(resolve, 500));
        }

        // Summary
        console.log('\n' + '='.repeat(80));
        console.log('SUMMARY');
        console.log('='.repeat(80));

        const totalReports = Object.values(allReports).reduce((sum, reports) => sum + reports.length, 0);
        console.log(`\nTotal accessible reports: ${totalReports}`);

        Object.entries(allReports).forEach(([category, reports]) => {
            console.log(`  ${category}: ${reports.length} reports`);
        });

        // Look for WBR-related reports
        console.log('\n' + '='.repeat(80));
        console.log('SEARCHING FOR WBR-RELATED REPORTS');
        console.log('='.repeat(80));

        const wbrKeywords = ['wbr', 'weekly', 'business', 'review', 'close', 'rate', 'sold', 'estimate'];
        const wbrReports = [];

        Object.entries(allReports).forEach(([category, reports]) => {
            reports.forEach(report => {
                const name = (report.name || report.reportName || report.Name || '').toLowerCase();
                const desc = (report.description || report.Description || '').toLowerCase();
                const searchText = `${name} ${desc}`;

                if (wbrKeywords.some(keyword => searchText.includes(keyword))) {
                    wbrReports.push({
                        category,
                        id: report.id || report.reportId || report.Id,
                        name: report.name || report.reportName || report.Name,
                        description: report.description || report.Description || ''
                    });
                }
            });
        });

        if (wbrReports.length > 0) {
            console.log(`\n✅ Found ${wbrReports.length} potentially relevant reports:\n`);
            wbrReports.forEach((report, i) => {
                console.log(`${i + 1}. ${report.name}`);
                console.log(`   ID: ${report.id}`);
                console.log(`   Category: ${report.category}`);
                if (report.description) {
                    console.log(`   Description: ${report.description.substring(0, 150)}${report.description.length > 150 ? '...' : ''}`);
                }
                console.log('');
            });

            // Save WBR reports to file
            const filename = 'wbr_reports_found.json';
            fs.writeFileSync(filename, JSON.stringify(wbrReports, null, 2));
            console.log(`✅ WBR reports saved to: ${filename}\n`);
        } else {
            console.log('\n❌ No WBR-related reports found');
            console.log('This suggests either:');
            console.log('1. WBR reports are custom views not accessible via API');
            console.log('2. WBR reports require different search keywords');
            console.log('3. The Reporting API list endpoint is not available');
        }

        // Save all reports
        const allFilename = 'all_reports_found.json';
        fs.writeFileSync(allFilename, JSON.stringify(allReports, null, 2));
        console.log(`\n✅ All reports saved to: ${allFilename}`);

    } catch (error) {
        console.error('\n❌ Error:', error.message);
        if (error.stack) {
            console.error(error.stack);
        }
    }
}

discoverReports();
