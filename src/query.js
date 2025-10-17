import { query } from './bq.js';

async function runQuery() {
  const sql = `
    SELECT
      MIN(job_start) AS min_job_start,
      MAX(job_start) AS max_job_start,
      updated_on
    FROM
      \`st_kpi.raw_foreman\`
    GROUP BY
      updated_on
    ORDER BY
      updated_on DESC
    LIMIT 5;
  `;

  try {
    const rows = await query(sql);
    console.log(rows);
  } catch (e) {
    console.error(e);
  }
}

runQuery();
