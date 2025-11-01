#!/usr/bin/env node

/**
 * Aggregate Daily WBR C/R data to compare with baseline
 */

import fs from 'fs';

const data = JSON.parse(fs.readFileSync('daily_wbr_cr_data.json', 'utf8'));

console.log('='.repeat(80));
console.log('DAILY WBR C/R AGGREGATION FOR 10/20-10/26');
console.log('='.repeat(80));

console.log(`\nTotal records: ${data.data.data.length}`);

// Field indices based on metadata:
// [0] Name
// [1] ClosedOpportunities
// [2] CompletedJobs
// [3] SalesOpportunity
// [4] CloseRate
// [5] TotalSales
// [6] ClosedAverageSale

let totalClosedOpps = 0;
let totalCompletedJobs = 0;
let totalSalesOpps = 0;
let totalSales = 0;

data.data.data.forEach(record => {
    totalClosedOpps += record[1] || 0;
    totalCompletedJobs += record[2] || 0;
    totalSalesOpps += record[3] || 0;
    totalSales += record[5] || 0;
});

const overallCloseRate = totalSalesOpps > 0 ? (totalClosedOpps / totalSalesOpps) : 0;

console.log('\n' + '='.repeat(80));
console.log('AGGREGATED TOTALS');
console.log('='.repeat(80));

console.log(`\nClosed Opportunities: ${totalClosedOpps}`);
console.log(`Completed Jobs: ${totalCompletedJobs}`);
console.log(`Sales Opportunities: ${totalSalesOpps}`);
console.log(`Total Sales: $${totalSales.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`);
console.log(`Close Rate: ${(overallCloseRate * 100).toFixed(2)}%`);

console.log('\n' + '='.repeat(80));
console.log('COMPARISON TO BASELINE');
console.log('='.repeat(80));

const baseline = {
    leads: 227,
    completedEstimates: 190,
    closeRate: 40.34,
    totalBooked: 434600
};

console.log('\nBaseline (Expected):');
console.log(`  Leads: ${baseline.leads}`);
console.log(`  Completed Estimates: ${baseline.completedEstimates}`);
console.log(`  Close Rate: ${baseline.closeRate}%`);
console.log(`  Total Booked: $${baseline.totalBooked.toLocaleString('en-US')}`);

console.log('\nActual (Daily WBR C/R Report):');
console.log(`  Sales Opportunities: ${totalSalesOpps} (vs ${baseline.leads} expected leads)`);
console.log(`  Closed Opportunities: ${totalClosedOpps} (vs ${baseline.completedEstimates} expected)`);
console.log(`  Close Rate: ${(overallCloseRate * 100).toFixed(2)}% (vs ${baseline.closeRate}% expected)`);
console.log(`  Total Sales: $${totalSales.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})} (vs $${baseline.totalBooked.toLocaleString('en-US')} expected)`);

console.log('\nAccuracy:');
console.log(`  Sales Opps: ${((totalSalesOpps / baseline.leads) * 100).toFixed(1)}%`);
console.log(`  Closed Opps: ${((totalClosedOpps / baseline.completedEstimates) * 100).toFixed(1)}%`);
console.log(`  Close Rate: ${((overallCloseRate * 100) / baseline.closeRate * 100).toFixed(1)}%`);
console.log(`  Total Sales: ${((totalSales / baseline.totalBooked) * 100).toFixed(1)}%`);

console.log('\n' + '='.repeat(80));
console.log('TOP 20 TECHNICIANS BY SALES');
console.log('='.repeat(80));

const techWithSales = data.data.data
    .filter(record => record[5] > 0)
    .map(record => ({
        name: record[0],
        closedOpps: record[1],
        completedJobs: record[2],
        salesOpps: record[3],
        closeRate: record[4],
        totalSales: record[5],
        avgSale: record[6]
    }))
    .sort((a, b) => b.totalSales - a.totalSales)
    .slice(0, 20);

console.log('\n');
techWithSales.forEach((tech, i) => {
    console.log(`${i + 1}. ${tech.name}`);
    console.log(`   Sales Opps: ${tech.salesOpps} | Closed: ${tech.closedOpps} | Close Rate: ${(tech.closeRate * 100).toFixed(1)}%`);
    console.log(`   Total Sales: $${tech.totalSales.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})} | Avg: $${tech.avgSale.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`);
    console.log('');
});
