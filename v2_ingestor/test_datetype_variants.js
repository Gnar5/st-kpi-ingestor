#!/usr/bin/env node

import fetch from 'node-fetch';
import dotenv from 'dotenv';
import fs from 'fs';

dotenv.config();

const CLIENT_ID = process.env.ST_CLIENT_ID;
const CLIENT_SECRET = process.env.ST_CLIENT_SECRET;
const TENANT_ID = process.env.ST_TENANT_ID;
const APP_KEY = process.env.ST_APP_KEY;

async function getAccessToken() {
  const response = await fetch('https://auth.servicetitan.io/connect/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET
    })
  });
  const data = await response.json();
  return data.access_token;
}

async function testDateType(dateType, label) {
  const token = await getAccessToken();

  const url = `https://api.servicetitan.io/reporting/v2/tenant/${TENANT_ID}/report-category/operations/reports/389438975/data`;

  const parameters = [
    { name: 'From', value: '2025-10-20' },
    { name: 'To', value: '2025-10-26' }
  ];

  if (dateType !== null) {
    parameters.push({ name: 'DateType', value: dateType });
  }

  const body = {
    request: { page: 1, pageSize: 500 },
    parameters
  };

  console.log(`\n=== Testing ${label} ===`);
  console.log(`DateType: ${dateType === null ? 'NONE' : dateType}`);

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'ST-App-Key': APP_KEY,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  });

  if (!response.ok) {
    console.log('❌ Error:', response.status, response.statusText);
    return;
  }

  const result = await response.json();
  console.log(`✅ Status: ${response.status} - ${result.data.length} records`);

  // Analyze date range
  const inRange = result.data.filter(r => {
    const d = r[0]?.split('T')[0];
    return d && d >= '2025-10-20' && d <= '2025-10-26';
  });

  const outRange = result.data.filter(r => {
    const d = r[0]?.split('T')[0];
    return !d || d < '2025-10-20' || d > '2025-10-26';
  });

  console.log(`  Jobs in date range (2025-10-20 to 2025-10-26): ${inRange.length}`);
  console.log(`  Jobs outside range: ${outRange.length}`);

  // Calculate totals for jobs with revenue
  const withRevenue = inRange.filter(r => r[7] > 0);
  const totalRevenue = withRevenue.reduce((s, r) => s + (r[7] || 0), 0);
  const totalLabor = withRevenue.reduce((s, r) => s + (r[8] || 0) + (r[9] || 0), 0);
  const totalMaterials = withRevenue.reduce((s, r) => s + (r[10] || 0), 0);

  console.log(`  Jobs with revenue: ${withRevenue.length}`);
  console.log(`  Total Revenue: $${totalRevenue.toFixed(2)}`);
  console.log(`  Total Labor: $${totalLabor.toFixed(2)}`);
  console.log(`  Total Materials: $${totalMaterials.toFixed(2)}`);

  // Compare to target
  console.log(`  Gap to ST target (162 jobs, $474,562):`);
  console.log(`    Jobs: ${withRevenue.length - 162}`);
  console.log(`    Revenue: $${(totalRevenue - 474562).toFixed(2)}`);

  return result;
}

async function main() {
  console.log('=== Testing Different DateType Parameters ===');
  console.log('Target: 162 jobs, $474,562 revenue\n');

  // Test different DateType values
  await testDateType(null, 'No DateType parameter');
  await testDateType(1, 'DateType = 1 (Scheduled?)');
  await testDateType(2, 'DateType = 2 (Payment/Completed?)');
  await testDateType(0, 'DateType = 0');

  console.log('\n=== Complete ===\n');
}

main().catch(console.error);
