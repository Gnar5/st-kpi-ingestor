/**
 * Test script to fetch payments from ServiceTitan API and inspect the structure
 */

import { ServiceTitanClient } from './src/api/servicetitan_client.js';

async function testPaymentsAPI() {
  try {
    const client = new ServiceTitanClient();

    console.log('Fetching sample payments from week of 8/18/2025...\n');

    // Fetch payments from the week we're testing
    const payments = await client.getPayments({
      createdOnOrAfter: '2025-08-18T00:00:00Z',
      createdBefore: '2025-08-25T00:00:00Z',
      pageSize: 10
    });

    console.log(`\nFetched ${payments.length} payments`);

    if (payments.length > 0) {
      console.log('\n=== First Payment Structure ===');
      console.log(JSON.stringify(payments[0], null, 2));

      console.log('\n=== Key Field Values ===');
      console.log('ID:', payments[0].id);
      console.log('InvoiceId:', payments[0].invoiceId);
      console.log('Amount:', payments[0].amount);
      console.log('BusinessUnitId:', payments[0].businessUnitId);
      console.log('PaymentTypeId:', payments[0].paymentTypeId || payments[0].typeId);
      console.log('Status:', payments[0].status);
      console.log('CreatedOn:', payments[0].createdOn);
      console.log('Memo:', payments[0].memo);

      // Calculate total for the week
      const total = payments.reduce((sum, p) => sum + (p.amount || 0), 0);
      console.log('\n=== Week Total ===');
      console.log(`Total payments: ${payments.length}`);
      console.log(`Total amount: $${total.toFixed(2)}`);
    } else {
      console.log('\nNo payments found in that date range');
    }

  } catch (error) {
    console.error('Error:', error.message);
    console.error(error.stack);
  }
}

testPaymentsAPI();
