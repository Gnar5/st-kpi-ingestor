import {BigQuery} from '@google-cloud/bigquery';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const cfg = JSON.parse(fs.readFileSync(path.join(__dirname,'../config/config.json'),'utf8'));

const bq = new BigQuery({ projectId: cfg.bigquery.project_id });

export async function insertRows(dataset, table, rows) {
  if (!rows?.length) return;
  try {
    await bq.dataset(dataset).table(table).insert(rows, {
      ignoreUnknownValues: true,
      skipInvalidRows: false  // Changed to false to catch schema issues
    });
    console.log(`[BQ] Successfully inserted ${rows.length} rows into ${dataset}.${table}`);
    return { inserted: rows.length, errors: [] };
  } catch (err) {
    // Check for partial errors
    if (err.name === 'PartialFailureError' && err.errors?.length) {
      const errorCount = err.errors.length;
      const successCount = rows.length - errorCount;
      console.error(`[BQ] Partial insertion failure: ${successCount}/${rows.length} rows inserted`);
      console.error(`[BQ] First error sample:`, JSON.stringify(err.errors[0], null, 2));
      return { inserted: successCount, errors: err.errors };
    }
    // Full failure
    console.error(`[BQ] Insert failed for ${dataset}.${table}:`, err.message);
    throw err;
  }
}

export async function query(query) {
  const [job] = await bq.createQueryJob({ query });
  const [rows] = await job.getQueryResults();
  return rows;
}
