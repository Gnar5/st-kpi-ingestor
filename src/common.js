import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { makeStClient } from './st_client.js';
import { insertRows } from './bq.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

console.log('Loading config from:', path.join(__dirname,'../config/config.json'));
export const cfg = JSON.parse(fs.readFileSync(path.join(__dirname,'../config/config.json'),'utf8'));
console.log('Config loaded successfully');

export let buNameIdMap = {};

export async function initBuMap() {
  console.log('Starting initBuMap...');
  try {
    const st = await makeStClient(cfg);
    console.log('ST client created');
    buNameIdMap = await st.fetchBusinessUnits();
    console.log(`BU map initialized: ${Object.keys(buNameIdMap).length} units`);
  } catch (e) {
    console.error('initBuMap failed:', e);
    throw e;
  }
}

export async function ingest(res, categoryPath, reportId, params, mapRow, destTable) {
  const started = Date.now();
  const effectiveParams = params || {};

  if (!reportId) throw new Error('ingest requires a reportId');
  if (!categoryPath) throw new Error('ingest requires a categoryPath');
  if (!destTable) throw new Error('ingest requires a destTable');

  let dataset = 'st_raw';
  let table = destTable;
  if (destTable.includes('.')) {
    const parts = destTable.split('.', 2);
    dataset = parts[0];
    table = parts[1];
  }

  const toObj = (row, columns) => {
    if (row && typeof row === 'object' && !Array.isArray(row)) return row;
    if (Array.isArray(row) && columns?.length === row.length) {
      return Object.fromEntries(columns.map((name, idx) => [name, row[idx]]));
    }
    return {};
  };

  try {
    console.log('[ingest] fetching report', { categoryPath, reportId, params: effectiveParams });
    const st = await makeStClient(cfg);
    const resp = await st.fetchReport(categoryPath, reportId, effectiveParams);

    const columns = (resp?.data?.columns || resp?.columns || []).map(c => c?.name ?? c);
    const rawItems = resp?.data?.items || resp?.items || [];
    const items = Array.isArray(rawItems) ? rawItems : [];

    console.log('[ingest] report response', { reportId, items: items.length, sample: items[0] || null });

    const rows = [];
    for (const raw of items) {
      const obj = toObj(raw, columns);
      try {
        const mapped = mapRow ? mapRow(obj, { raw, columns, params: effectiveParams }) : obj;
        if (mapped) rows.push(mapped);
      } catch (e) {
        console.error('[ingest] mapRow error', { reportId, err: e?.stack || String(e) });
      }
    }

    if (rows.length) {
      await insertRows(dataset, table, rows);
    }

    const result = {
      status: 'ok',
      reportId,
      inserted: rows.length,
      elapsed_ms: Date.now() - started
    };

    if (res) res.json(result);
    return result;
  } catch (e) {
    console.error('[ingest] error', { reportId, err: e?.stack || String(e) });
    if (res) {
      res.status(500).json({ status: 'error', message: String(e) });
      return;
    }
    throw e;
  }
}
