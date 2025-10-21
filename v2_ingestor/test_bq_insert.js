import { BigQuery } from '@google-cloud/bigquery';

const bq = new BigQuery({ projectId: 'kpi-auto-471020' });
const dataset = bq.dataset('st_raw_v2');
const table = dataset.table('raw_campaigns');

const rows = [
  {
    id: 389520147,
    active: true,
    name: "Sept/Oct Arizona Newsletter (Commercial)",
    categoryId: null,
    category: null,
    createdOn: "2025-10-14T23:50:44.340Z",
    modifiedOn: "2025-10-15T07:37:58.982Z",
    _ingested_at: "2025-10-21T20:28:54.051Z",
    _ingestion_source: "servicetitan_v2"
  }
];

console.log('Attempting to insert:', JSON.stringify(rows, null, 2));

table.insert(rows).then(() => {
  console.log('✅ Insert successful!');
}).catch((err) => {
  console.log('❌ Insert failed');
  console.log('Error:', err.message);
  if (err.errors && err.errors.length > 0) {
    console.log('Detailed errors:', JSON.stringify(err.errors, null, 2));
  }
});
