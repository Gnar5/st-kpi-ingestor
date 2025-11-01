#!/usr/bin/env node

/**
 * Test script for JobCostingReportIngestor
 * Tests the full pipeline: fetch → transform → load
 */

import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import { ServiceTitanClient } from './src/api/servicetitan_client.js';
import { BigQueryClient } from './src/bq/bigquery_client.js';
import { JobCostingReportIngestor } from './src/ingestors_reports/job_costing_report.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config({ path: path.join(__dirname, '.env') });

async function main() {
  console.log('=== Testing Job Costing Report Ingestor ===\n');

  // Initialize clients
  const stClient = new ServiceTitanClient({
    clientId: process.env.ST_CLIENT_ID,
    clientSecret: process.env.ST_CLIENT_SECRET,
    tenantId: process.env.ST_TENANT_ID,
    appKey: process.env.ST_APP_KEY
  });

  const bqClient = new BigQueryClient({
    projectId: 'kpi-auto-471020',
    datasetId: 'st_raw_v2'
  });

  // Initialize ingestor
  const ingestor = new JobCostingReportIngestor(stClient, bqClient, {
    reportId: '389438975'
  });

  try {
    // Test 1: Fetch data for specific week
    console.log('Test 1: Fetching data for week 2025-10-20 to 2025-10-26...');
    const rawData = await ingestor.fetch({
      from: '2025-10-20',
      to: '2025-10-26'
    });
    console.log(`✅ Fetched ${rawData.length} records\n`);

    // Test 2: Transform data
    console.log('Test 2: Transforming data...');
    const transformedData = await ingestor.transform(rawData);
    console.log(`✅ Transformed ${transformedData.length} records\n`);

    // Test 3: Sample output
    console.log('Test 3: Sample transformed records...');
    console.log('\nFirst 3 records:');
    transformedData.slice(0, 3).forEach((record, i) => {
      console.log(`\nRecord ${i + 1}:`);
      console.log(`  Job ID: ${record.job_id}`);
      console.log(`  Business Unit: ${record.business_unit}`);
      console.log(`  Scheduled Date: ${record.scheduled_date}`);
      console.log(`  Revenue: $${record.revenue_subtotal.toFixed(2)}`);
      console.log(`  Labor Total: $${record.labor_total.toFixed(2)}`);
      console.log(`  Material Costs: $${record.material_costs.toFixed(2)}`);
      console.log(`  Total Costs: $${record.total_costs.toFixed(2)}`);
      console.log(`  Gross Profit: $${record.gross_profit.toFixed(2)}`);
      console.log(`  GPM%: ${record.gpm_percent.toFixed(2)}%`);
      console.log(`  Status: ${record.job_status}`);
    });

    // Test 4: Summary stats
    console.log('\n\nTest 4: Summary statistics...');
    const stats = {
      total_jobs: transformedData.length,
      completed_jobs: transformedData.filter(r => r.job_status === 'Completed').length,
      jobs_with_revenue: transformedData.filter(r => r.revenue_subtotal > 0).length,
      total_revenue: transformedData.reduce((sum, r) => sum + r.revenue_subtotal, 0),
      total_labor: transformedData.reduce((sum, r) => sum + r.labor_total, 0),
      total_materials: transformedData.reduce((sum, r) => sum + r.material_costs, 0),
      total_costs: transformedData.reduce((sum, r) => sum + r.total_costs, 0),
      total_gross_profit: transformedData.reduce((sum, r) => sum + r.gross_profit, 0)
    };

    stats.overall_gpm = (stats.total_gross_profit / stats.total_revenue * 100).toFixed(2);

    console.log('\nAll Jobs (including $0 revenue):');
    console.log(`  Total Jobs: ${stats.total_jobs}`);
    console.log(`  Completed: ${stats.completed_jobs}`);
    console.log(`  Jobs with Revenue: ${stats.jobs_with_revenue}`);
    console.log(`  Total Revenue: $${stats.total_revenue.toFixed(2)}`);
    console.log(`  Total Labor: $${stats.total_labor.toFixed(2)}`);
    console.log(`  Total Materials: $${stats.total_materials.toFixed(2)}`);
    console.log(`  Total Costs: $${stats.total_costs.toFixed(2)}`);
    console.log(`  Gross Profit: $${stats.total_gross_profit.toFixed(2)}`);
    console.log(`  Overall GPM%: ${stats.overall_gpm}%`);

    // Test 5: Filter to jobs with revenue (matching CSV export)
    console.log('\n\nTest 5: Jobs with revenue > $0 (CSV export filter)...');
    const revenueJobs = transformedData.filter(r => r.revenue_subtotal > 0);
    const revStats = {
      total_jobs: revenueJobs.length,
      total_revenue: revenueJobs.reduce((sum, r) => sum + r.revenue_subtotal, 0),
      total_labor: revenueJobs.reduce((sum, r) => sum + r.labor_total, 0),
      total_materials: revenueJobs.reduce((sum, r) => sum + r.material_costs, 0),
      total_costs: revenueJobs.reduce((sum, r) => sum + r.total_costs, 0),
      total_gross_profit: revenueJobs.reduce((sum, r) => sum + r.gross_profit, 0)
    };
    revStats.overall_gpm = (revStats.total_gross_profit / revStats.total_revenue * 100).toFixed(2);

    console.log(`  Total Jobs: ${revStats.total_jobs}`);
    console.log(`  Total Revenue: $${revStats.total_revenue.toFixed(2)}`);
    console.log(`  Total Labor: $${revStats.total_labor.toFixed(2)}`);
    console.log(`  Total Materials: $${revStats.total_materials.toFixed(2)}`);
    console.log(`  Total Costs: $${revStats.total_costs.toFixed(2)}`);
    console.log(`  Gross Profit: $${revStats.total_gross_profit.toFixed(2)}`);
    console.log(`  Overall GPM%: ${revStats.overall_gpm}%`);

    // Test 6: Compare to ServiceTitan CSV targets
    console.log('\n\nTest 6: Comparison to ServiceTitan CSV export...');
    const targets = {
      job_count: 162,
      revenue: 474562,
      labor: 171079,
      materials: 105292,
      gpm: 41.93
    };

    console.log('\n  Metric              | ST Target    | API Result   | Variance');
    console.log('  --------------------|--------------|--------------|----------');
    console.log(`  Job Count           | ${targets.job_count.toString().padEnd(12)} | ${revStats.total_jobs.toString().padEnd(12)} | ${(revStats.total_jobs - targets.job_count).toString()}`);
    console.log(`  Revenue             | $${targets.revenue.toFixed(2).padEnd(11)} | $${revStats.total_revenue.toFixed(2).padEnd(11)} | $${(revStats.total_revenue - targets.revenue).toFixed(2)}`);
    console.log(`  Labor               | $${targets.labor.toFixed(2).padEnd(11)} | $${revStats.total_labor.toFixed(2).padEnd(11)} | $${(revStats.total_labor - targets.labor).toFixed(2)}`);
    console.log(`  Materials           | $${targets.materials.toFixed(2).padEnd(11)} | $${revStats.total_materials.toFixed(2).padEnd(11)} | $${(revStats.total_materials - targets.materials).toFixed(2)}`);
    console.log(`  GPM%                | ${targets.gpm.toFixed(2).padEnd(11)}% | ${revStats.overall_gpm.padEnd(11)}% | ${(parseFloat(revStats.overall_gpm) - targets.gpm).toFixed(2)}pp`);

    console.log('\n✅ All tests completed successfully!\n');

    // Test 7: Schema validation
    console.log('Test 7: BigQuery schema...');
    const schema = ingestor.getSchema();
    console.log(`✅ Schema has ${schema.length} fields`);
    console.log('Key fields:', schema.filter(f => f.mode === 'REQUIRED').map(f => f.name).join(', '));

    console.log('\n=== Test Complete ===\n');

  } catch (error) {
    console.error('❌ Test failed:', error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

main();
