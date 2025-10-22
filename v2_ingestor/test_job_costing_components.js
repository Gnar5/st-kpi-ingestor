/**
 * Test Job Costing Components
 * Based on FOREMAN report formula:
 * Jobs Total Costs = Labor Pay + Payroll Adjustments + Materials/Equipment/PO Costs - Returns
 */

import 'dotenv/config';
import { ServiceTitanClient } from './src/api/servicetitan_client.js';

async function testJobCostingComponents() {
  const client = new ServiceTitanClient();

  console.log('🔍 Testing Job Costing Component Endpoints\n');

  // Test 1: Purchase Orders (Material/Equipment/PO Costs)
  console.log('1️⃣ Testing Purchase Orders API...');
  try {
    const poParams = {
      active: 'Any',
      createdOnOrAfter: '2024-10-01T00:00:00Z',
      createdBefore: '2024-10-08T00:00:00Z',
      pageSize: 5
    };
    const pos = await client.fetchAll('inventory/v2/tenant/{tenant}/purchase-orders', poParams);
    console.log(`✅ Purchase Orders: ${pos.length} records`);
    if (pos.length > 0) {
      const samplePO = pos[0];
      console.log('Sample PO:', JSON.stringify({
        id: samplePO.id,
        jobId: samplePO.jobId,
        invoiceId: samplePO.invoiceId,
        status: samplePO.status,
        total: samplePO.total,
        tax: samplePO.tax,
        items: samplePO.items?.length || 0
      }, null, 2));
    }
  } catch (error) {
    console.log(`❌ Purchase Orders failed: ${error.message}`);
  }

  // Test 2: Returns (Material Returns to subtract)
  console.log('\n2️⃣ Testing Returns API...');
  try {
    const returnParams = {
      createdOnOrAfter: '2024-10-01T00:00:00Z',
      createdBefore: '2024-10-08T00:00:00Z',
      pageSize: 5
    };
    const returns = await client.fetchAll('inventory/v2/tenant/{tenant}/returns', returnParams);
    console.log(`✅ Returns: ${returns.length} records`);
    if (returns.length > 0) {
      const sampleReturn = returns[0];
      console.log('Sample Return:', JSON.stringify({
        id: sampleReturn.id,
        jobId: sampleReturn.jobId,
        total: sampleReturn.total,
        status: sampleReturn.status,
        items: sampleReturn.items?.length || 0
      }, null, 2));
    }
  } catch (error) {
    console.log(`❌ Returns failed: ${error.message}`);
  }

  // Test 3: Gross Pay Items (Labor Pay)
  console.log('\n3️⃣ Testing Gross Pay Items API...');
  try {
    const grossPayParams = {
      paidOnOrAfter: '2024-10-01T00:00:00Z',
      paidBefore: '2024-10-08T00:00:00Z',
      pageSize: 5
    };
    const grossPay = await client.fetchAll('payroll/v2/tenant/{tenant}/gross-pay-items', grossPayParams);
    console.log(`✅ Gross Pay Items: ${grossPay.length} records`);
    if (grossPay.length > 0) {
      const samplePay = grossPay[0];
      console.log('Sample Gross Pay:', JSON.stringify({
        id: samplePay.id,
        jobId: samplePay.jobId,
        employeeId: samplePay.employeeId,
        rate: samplePay.rate,
        hours: samplePay.hours,
        total: samplePay.total,
        paidOn: samplePay.paidOn
      }, null, 2));
    }
  } catch (error) {
    console.log(`❌ Gross Pay Items failed: ${error.message}`);
  }

  // Test 4: Payroll Adjustments
  console.log('\n4️⃣ Testing Payroll Adjustments API...');
  try {
    const adjustmentParams = {
      paidOnOrAfter: '2024-10-01T00:00:00Z',
      paidBefore: '2024-10-08T00:00:00Z',
      pageSize: 5
    };
    const adjustments = await client.fetchAll('payroll/v2/tenant/{tenant}/payroll-adjustments', adjustmentParams);
    console.log(`✅ Payroll Adjustments: ${adjustments.length} records`);
    if (adjustments.length > 0) {
      const sampleAdj = adjustments[0];
      console.log('Sample Adjustment:', JSON.stringify({
        id: sampleAdj.id,
        jobId: sampleAdj.jobId,
        employeeId: sampleAdj.employeeId,
        amount: sampleAdj.amount,
        reason: sampleAdj.reason,
        paidOn: sampleAdj.paidOn
      }, null, 2));
    }
  } catch (error) {
    console.log(`❌ Payroll Adjustments failed: ${error.message}`);
  }

  // Summary
  console.log('\n' + '='.repeat(60));
  console.log('📊 JOB COSTING FORMULA (from FOREMAN report)');
  console.log('='.repeat(60));
  console.log('Jobs Subtotal = [From Invoices linked to job]');
  console.log('Labor Pay = SUM(gross-pay-items WHERE jobId = job.id)');
  console.log('Payroll Adjustments = SUM(payroll-adjustments WHERE jobId = job.id)');
  console.log('Material/Equipment/PO Costs = SUM(purchase-orders WHERE jobId = job.id)');
  console.log('Returns = SUM(returns WHERE jobId = job.id)');
  console.log('');
  console.log('Jobs Total Costs = Labor Pay + Payroll Adjustments + PO Costs - Returns');
  console.log('Jobs Gross Margin % = (Jobs Subtotal - Jobs Total Costs) / Jobs Subtotal * 100');
  console.log('='.repeat(60));
}

testJobCostingComponents().catch(console.error);
