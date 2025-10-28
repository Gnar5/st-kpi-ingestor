/**
 * Test script to fetch payments from ServiceTitan API and inspect the structure
 */

import { ServiceTitanClient } from './src/api/servicetitan_client.js';

async function testPaymentsAPI() {
  try {
    const client = new ServiceTitanClient();

    console.log('Fetching sample payments...');

    // Fetch just a few payments to inspect structure
    const payments = await client.getPayments({
      createdOnOrAfter: '2025-08-18T00:00:00Z',
      createdBefore: '2025-08-19T00:00:00Z',
      pageSize: 5
    });

    console.log(`\nFetched ${payments.length} payments`);

    if (payments.length > 0) {
      console.log('\nFirst payment structure:');
      console.log(JSON.stringify(payments[0], null, 2));

      console.log('\nField values:');
      console.log('ID:', payments[0].id);
      console.log('Amount:', payments[0].amount);
      console.log('BusinessUnitId:', payments[0].businessUnitId);
      console.log('InvoiceId:', payments[0].invoiceId);
      console.log('CreatedOn:', payments[0].createdOn);
    } else {
      console.log('\nNo payments found in that date range');
    }

  } catch (error) {
    console.error('Error:', error.message);
    console.error(error.stack);
  }
}

testPaymentsAPI();
