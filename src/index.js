process.on('unhandledRejection', e => console.error('UNHANDLED REJECTION:', e?.stack || e));
process.on('uncaughtException', e => console.error('UNCAUGHT EXCEPTION:', e?.stack || e));

import express from 'express';
import { makeStClient } from './st_client.js';
import { cfg, initBuMap, ingest, buNameIdMap } from './common.js';
import { insertRows } from './bq.js';

const app = express();
app.get('/', (_req, res) => res.send('ST KPI Ingestor up'));

function ymdInTz(date, tz) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit'
  }).format(date);
}

function toNumber(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  if (typeof value === 'string') {
    let str = value.trim();
    if (!str) return null;
    let negative = false;
    if (str.startsWith('(') && str.endsWith(')')) {
      negative = true;
      str = str.slice(1, -1);
    }
    str = str.replace(/[,$\s]/g, '');
    str = str.replace(/[^0-9.\-]/g, '');
    if (!str) return null;
    const num = Number(str);
    if (!Number.isFinite(num)) return null;
    return negative ? -num : num;
  }
  return null;
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ----------------------- DIAGNOSTICS -----------------------
app.get('/health', (_req, res) =>
  res.json({ ok: true, rev: process.env.K_REVISION || null })
);

app.get('/debug/wbr_keys', (_req, res) => {
  const repMap = (cfg?.servicetitan?.daily_wbr_report_ids_by_bu) || {};
  res.json({ wbr_keys: Object.keys(repMap) });
});

app.get('/debug/bus', (_req, res) => {
  res.json({ businessUnits: Object.keys(buNameIdMap || {}) });
});

app.get('/debug/report/:category/:id', async (req, res) => {
  try {
    const cat = req.params.category;
    const id  = req.params.id;
    const st  = await makeStClient(cfg);
    const info = await st.fetchReportInfo(`report-category/${cat}`, id);
    res.json(info);
  } catch (e) {
    console.error(e);
    res.status(500).send(String(e));
  }
});

app.get('/debug/job_types', async (_req, res) => {
  try {
    const st = await makeStClient(cfg);
    const list = await st.listJobTypes();
    res.json(list);
  } catch (e) {
    console.error('job_types debug error:', e);
    res.status(500).json({ error: String(e) });
  }
});
// ----------------------- LEADS DEBUG: DateType Comparison -----------------------
app.get('/debug/leads/datetypes', async (req, res) => {
  try {
    const reportId = cfg.servicetitan.report_ids.leads;
    const from = req.query.from || '2025-10-02';
    const to = req.query.to || '2025-10-08';
    const buName = req.query.bu || 'Phoenix-Sales'; // Test with one BU

    if (!Object.keys(buNameIdMap || {}).length) {
      await initBuMap().catch(() => null);
    }

    const buId = buNameIdMap?.[buName];
    if (!buId) {
      return res.status(400).json({ error: `Unknown BU: ${buName}` });
    }

    const st = await makeStClient(cfg);
    const results = {};

    // Test DateType 1, 2, and 3
    for (const dateType of [1, 2, 3]) {
      const params = { From: from, To: to, DateType: dateType, BusinessUnitIds: [buId], BusinessUnitId: buId };

      try {
        const resp = await st.fetchReport('report-category/marketing', reportId, params);
        const rawItems = resp?.data?.items || resp?.items || [];
        const items = Array.isArray(rawItems) ? rawItems : [];

        results[`DateType_${dateType}`] = {
          description: dateType === 1 ? 'Job Creation Date' : dateType === 2 ? 'Modified Date' : 'Job Completed Date',
          count: items.length
        };
      } catch (err) {
        results[`DateType_${dateType}`] = { error: String(err) };
      }

      await sleep(500); // Rate limiting
    }

    res.json({
      bu: buName,
      date_range: { from, to },
      results,
      note: 'Compare these counts with ServiceTitan report to determine correct DateType'
    });
  } catch (e) {
    console.error('debug/leads/datetypes error:', e);
    res.status(500).json({ error: String(e) });
  }
});

// ----------------------- LEADS DEBUG: Sample Columns -----------------------
app.get('/debug/leads/columns', async (req, res) => {
  try {
    const reportId = cfg.servicetitan.report_ids.leads;
    const from = req.query.from || '2025-10-02';
    const to = req.query.to || '2025-10-08';
    const buName = req.query.bu || 'Phoenix-Sales';

    if (!Object.keys(buNameIdMap || {}).length) {
      await initBuMap().catch(() => null);
    }

    const buId = buNameIdMap?.[buName];
    if (!buId) {
      return res.status(400).json({ error: `Unknown BU: ${buName}` });
    }

    const st = await makeStClient(cfg);
    const params = { From: from, To: to, DateType: 2, BusinessUnitIds: [buId], BusinessUnitId: buId };

    const resp = await st.fetchReport('report-category/marketing', reportId, params);
    const cols = (resp?.data?.columns || resp?.columns || []).map(c => c?.name ?? c);
    const rawItems = resp?.data?.items || resp?.items || [];
    const items = Array.isArray(rawItems) ? rawItems : [];

    // Show first row as sample
    const sample = items[0] || null;

    res.json({
      bu: buName,
      date_range: { from, to },
      columns: cols,
      total_rows: items.length,
      sample_row: sample,
      note: 'Check if Modified Date/Modified On field exists'
    });
  } catch (e) {
    console.error('debug/leads/columns error:', e);
    res.status(500).json({ error: String(e) });
  }
});

// ----------------------- LEADS DEBUG -----------------------
app.get('/debug/leads', async (req, res) => {
  try {
    const reportId = cfg.servicetitan.report_ids.leads;
    const from = req.query.from || '2025-10-02';
    const to = req.query.to || '2025-10-08';

    if (!Object.keys(buNameIdMap || {}).length) {
      await initBuMap().catch(() => null);
    }

    const salesBuNames = (() => {
      const names = new Set();
      for (const entry of cfg?.bu_mapping || []) {
        for (const n of entry?.sales_bu_names || []) names.add(n);
      }
      if (names.size) return [...names];
      return Object.keys(buNameIdMap || {}).filter(name => /-Sales$/i.test(name));
    })();

    // Job type filtering is handled by ServiceTitan report configuration
    const nameExcludes = (cfg?.filters?.leads?.customer_name_excludes_contains || [])
      .map(v => String(v || '').trim().toLowerCase())
      .filter(Boolean);

    const st = await makeStClient(cfg);
    const results = {};
    const rowToObj = (row, cols) => {
      if (row && typeof row === 'object' && !Array.isArray(row)) return row;
      if (Array.isArray(row) && cols.length === row.length) {
        return Object.fromEntries(cols.map((n,i) => [n, row[i]]));
      }
      return {};
    };

    for (const buName of salesBuNames) {
      const buId = buNameIdMap?.[buName];
      if (!buId) continue;

      // DateType: 2 = Modified Date (matches ServiceTitan UI report)
      const params = { From: from, To: to, DateType: 2, BusinessUnitIds: [buId], BusinessUnitId: buId };
      let resp;
      try {
        resp = await st.fetchReport('report-category/marketing', reportId, params);
      } catch (err) {
        results[buName] = { error: String(err) };
        continue;
      }

      const cols = (resp?.data?.columns || resp?.columns || []).map(c => c?.name ?? c);
      const rawItems = resp?.data?.items || resp?.items || [];
      const items = Array.isArray(rawItems) ? rawItems : [];

      let passedBuFilter = 0;
      let passedNameFilter = 0;
      let finalCount = 0;
      const filteredCustomers = [];

      for (const row of items) {
        const r = rowToObj(row, cols);
        const rowBu = r['Business Unit'] || r['Job Business Unit'] || (Array.isArray(row) ? row[6] : null);

        if (rowBu && rowBu !== buName) continue;
        passedBuFilter++;

        const customerName = r['Customer name'] || (Array.isArray(row) ? row[0] : null);
        if (customerName) {
          const lower = String(customerName).toLowerCase();
          if (nameExcludes.some(token => lower.includes(token))) {
            filteredCustomers.push(customerName);
            continue;
          }
        }
        passedNameFilter++;
        finalCount++;
      }

      results[buName] = {
        total_from_api: items.length,
        passed_bu_filter: passedBuFilter,
        passed_name_filter: passedNameFilter,
        final_count: finalCount,
        filtered_customers: filteredCustomers.length > 0 ? filteredCustomers : undefined
      };
    }

    res.json({
      date_range: { from, to },
      filter_config: {
        customer_name_excludes: nameExcludes,
        note: 'Job type filtering handled by ServiceTitan report configuration'
      },
      by_bu: results,
      grand_total: Object.values(results).reduce((sum, r) => sum + (r.final_count || 0), 0)
    });
  } catch (e) {
    console.error('debug/leads error:', e);
    res.status(500).json({ error: String(e) });
  }
});

// ----------------------- LEADS -----------------------
app.get('/ingest/leads', async (req,res) => {
  try {
    const reportId = cfg.servicetitan.report_ids.leads;
    const destTable = cfg.bigquery.raw_tables.leads;

    // Support explicit from/to dates OR days parameter
    // Also support offset_days to pull historical data (e.g., offset_days=365 for same week last year)
    let start, end;
    if (req.query.from && req.query.to) {
      start = new Date(`${req.query.from}T00:00:00`);
      end = new Date(`${req.query.to}T00:00:00`);
    } else {
      const days = Number(req.query.days || cfg.servicetitan.date_windows.incremental_pull_days || 3);
      const offsetDays = Number(req.query.offset_days || 0);
      end = new Date();
      end.setDate(end.getDate() - offsetDays);
      start = new Date(end);
      start.setDate(end.getDate() - (days - 1));
    }

    // DateType: 2 = Modified Date (matches ServiceTitan UI report)
    const baseParams = { From: start.toISOString().slice(0,10), To: end.toISOString().slice(0,10), DateType: 2 };

    console.log('[Leads:req]', { reportId, baseParams });

    if (!Object.keys(buNameIdMap || {}).length) {
      await initBuMap().catch(() => null);
    }

    const salesBuNames = (() => {
      const names = new Set();
      for (const entry of cfg?.bu_mapping || []) {
        for (const n of entry?.sales_bu_names || []) names.add(n);
      }
      if (names.size) return [...names];
      return Object.keys(buNameIdMap || {}).filter(name => /-Sales$/i.test(name));
    })();

    if (!salesBuNames.length) {
      throw new Error('No sales business units configured; cannot ingest leads');
    }

    // Job type filtering is handled by ServiceTitan report configuration
    // No need to duplicate it here
    const nameExcludes = (cfg?.filters?.leads?.customer_name_excludes_contains || [])
      .map(v => String(v || '').trim().toLowerCase())
      .filter(Boolean);

    const st = await makeStClient(cfg);
    const rows = [];
    const seenKeys = new Set(); // Track unique job_id + bu_key combinations
    let apiCallCount = 0;
    const startTime = Date.now();

    console.log(`[Leads] Will make ${salesBuNames.length} API calls (one per BU, range-based)`);

    const rowToObj = (row, cols) => {
      if (row && typeof row === 'object' && !Array.isArray(row)) return row;
      if (Array.isArray(row) && cols.length === row.length) {
        return Object.fromEntries(cols.map((n,i) => [n, row[i]]));
      }
      return {};
    };

    for (const buName of salesBuNames) {
      const buId = buNameIdMap?.[buName];
      if (!buId) {
        console.warn('[Leads] missing BusinessUnitId for', buName);
        continue;
      }

      const params = { ...baseParams, BusinessUnitIds: [buId], BusinessUnitId: buId };
      let resp;
      try {
        resp = await st.fetchReport('report-category/marketing', reportId, params);
        apiCallCount++;
      } catch (err) {
        console.error('[Leads] fetch error', { buName, err: String(err) });
        continue;
      }

      const cols = (resp?.data?.columns || resp?.columns || []).map(c => c?.name ?? c);
      const rawItems = resp?.data?.items || resp?.items || [];
      const items = Array.isArray(rawItems) ? rawItems : [];
      console.log('[Leads:resp]', { buName, items_len: items.length, totalApiCalls: apiCallCount });

      for (const row of items) {
        const r = rowToObj(row, cols);
        const rowBu = r['Business Unit'] || r['Job Business Unit'] || (Array.isArray(row) ? row[6] : null);
        if (rowBu && rowBu !== buName) continue;

        const jobType = r['Job Type'] || (Array.isArray(row) ? row[3] : null);
        const customerName = r['Customer name'] || (Array.isArray(row) ? row[0] : null);
        if (customerName) {
          const lower = String(customerName).toLowerCase();
          if (nameExcludes.some(token => lower.includes(token))) continue;
        }

        // ServiceTitan returns array data - creation date is at position 8
        const creationDateStr = Array.isArray(row) ? row[8] : (r['Created Date'] || r['Job Creation Date'] || r['Created On']);
        const jobId = String(r['Job #'] || r['Job Number'] || (Array.isArray(row) ? row[5] : ''));
        const buKey = rowBu || buName;

        // Parse the creation date
        const creationDate = creationDateStr ? new Date(creationDateStr) : null;

        const uniqueKey = `${jobId}|${buKey}`;
        if (seenKeys.has(uniqueKey)) {
          console.log('[Leads] Skipping duplicate:', { jobId, buKey });
          continue;
        }
        seenKeys.add(uniqueKey);

        rows.push({
          bu_key: buKey,
          job_created_on: creationDate || new Date(baseParams.From),  // Use actual creation date from position 8
          created_date: creationDate,  // Store the creation date for Looker reporting
          customer_name: customerName || null,
          job_type: jobType || null,
          job_id: jobId,
          updated_on: new Date(),
          raw: JSON.stringify(row)
        });
      }

      await sleep(500); // Increased to 500ms for safety
    }

    const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[Leads] Complete: ${apiCallCount} API calls, ${rows.length} rows in ${elapsedSec}s`);

    if (rows.length) await insertRows('st_raw', destTable, rows);
    res.json({
      status: 'ok',
      items: rows.length,
      api_calls: apiCallCount,
      elapsed_seconds: parseFloat(elapsedSec),
      duplicates_skipped: apiCallCount > 0 ? (seenKeys.size - rows.length) : 0
    });
  } catch (e) {
    console.error('leads error:', e);
    res.status(500).json({ status:'error', message:String(e) });
  }
});
// ----------------------- COLLECTIONS -----------------------
app.get('/ingest/collections', async (req,res) => {
  try {
    const reportId = cfg.servicetitan.report_ids.collections;
    const destTable = cfg.bigquery.raw_tables.collections;

    // Support explicit from/to dates OR days parameter
    // Also support offset_days to pull historical data (e.g., offset_days=365 for same week last year)
    let start, end;
    if (req.query.from && req.query.to) {
      start = new Date(`${req.query.from}T00:00:00`);
      end = new Date(`${req.query.to}T00:00:00`);
    } else {
      const days = Number(req.query.days || cfg.servicetitan.date_windows.incremental_pull_days || 3);
      const offsetDays = Number(req.query.offset_days || 0);
      end = new Date();
      end.setDate(end.getDate() - offsetDays);
      start = new Date(end);
      start.setDate(end.getDate() - (days - 1));
    }

    // DateType: 2 = Paid On (matches ServiceTitan UI filter)
    const params = { From: start.toISOString().slice(0,10), To: end.toISOString().slice(0,10), DateType: 2 };

    console.log('[Collections:req]', { reportId, params });

    const st = await makeStClient(cfg);
    const resp = await st.fetchReport('report-category/accounting', reportId, params);

    const cols = (resp?.data?.columns || resp?.columns || []).map(c => c?.name ?? c);
    const rawItems = resp?.data?.items || resp?.items || [];
    const items = Array.isArray(rawItems) ? rawItems : [];

    console.log('[Collections:resp]', { items_len: items.length, columns: cols, sample: items[0] });

    const rowToObj = (row, cols) => {
      if (row && typeof row === 'object' && !Array.isArray(row)) return row;
      if (Array.isArray(row) && cols.length === row.length) {
        return Object.fromEntries(cols.map((n,i)=>[n,row[i]]));
      }
      return {};
    };

    const rows = [];
    let remainingBalanceCount = 0;

    for (const row of items) {
      const r = rowToObj(row, cols);

      const buRaw =
        (r && typeof r === 'object' && !Array.isArray(r) ? (
          r['Invoice Business Unit'] ??
          r['Job Business Unit'] ??
          r['Business Unit']
        ) : null) ?? (Array.isArray(row) ? row[7] : null);
      const buKey = typeof buRaw === 'string' ? buRaw.trim() : buRaw || 'unknown';

      const paidOnStr = (r && typeof r === 'object' && !Array.isArray(r) ? r['Paid On'] : null) ?? (Array.isArray(row) ? row[0] : null);
      const paidOn = paidOnStr ? new Date(paidOnStr) : null;

      const amountRaw = (r && typeof r === 'object' && !Array.isArray(r) ? (r['Amount'] ?? r['Payment Amount']) : null) ?? (Array.isArray(row) ? row[2] : null);
      const amount = toNumber(amountRaw);

      const invoiceNumber = (r && typeof r === 'object' && !Array.isArray(r) ? r['Invoice Number'] : null) ?? (Array.isArray(row) ? row[3] : null);
      const invoiceBalanceRaw = (r && typeof r === 'object' && !Array.isArray(r) ? r['Invoice Balance'] : null) ?? (Array.isArray(row) ? row[8] : null);
      const invoiceBalance = toNumber(invoiceBalanceRaw);

      if (invoiceBalance !== null && Math.abs(invoiceBalance) > 0.005) {
        remainingBalanceCount++;
      }

      rows.push({
        bu_key: buKey || 'unknown',
        payment_date: paidOn && !Number.isNaN(paidOn.valueOf()) ? paidOn : null,
        amount: amount ?? 0,
        job_id: invoiceNumber ? String(invoiceNumber) : '',
        updated_on: new Date(),
        raw: JSON.stringify(row)
      });
    }

    if (remainingBalanceCount) {
      console.log('[Collections] Rows with remaining balance detected', { remainingBalanceCount });
    }

    // Truncate existing data for this date range before inserting to avoid duplicates
    if (rows.length) {
      const { query } = await import('./bq.js');
      const fromDate = params.From;
      const toDate = params.To;
      const truncateSQL = `
        DELETE FROM \`kpi-auto-471020.st_raw.${destTable}\`
        WHERE DATE(payment_date) >= '${fromDate}'
          AND DATE(payment_date) <= '${toDate}'
      `;
      console.log('[Collections] Truncating date range:', { fromDate, toDate });
      await query(truncateSQL);

      await insertRows('st_raw', destTable, rows);
    }
    res.json({ status:'ok', items: rows.length });
  } catch (e) {
    console.error('collections error:', e);
    res.status(500).json({ status:'error', message:String(e) });
  }
});

// ----------------------- COLLECTIONS DEBUG -----------------------
app.get('/debug/collections', async (req, res) => {
  try {
    const reportId = cfg.servicetitan.report_ids.collections;
    const from = req.query.from || '2025-10-06';
    const to = req.query.to || '2025-10-12';

    const params = { From: from, To: to, DateType: 2 };

    console.log('[Collections:debug:req]', { reportId, params });

    const st = await makeStClient(cfg);
    const resp = await st.fetchReport('report-category/accounting', reportId, params);

    const cols = (resp?.data?.columns || resp?.columns || []).map(c => c?.name ?? c);
    const rawItems = resp?.data?.items || resp?.items || [];
    const items = Array.isArray(rawItems) ? rawItems : [];

    const rowToObj = (row, cols) => {
      if (row && typeof row === 'object' && !Array.isArray(row)) return row;
      if (Array.isArray(row) && cols.length === row.length) {
        return Object.fromEntries(cols.map((n,i)=>[n,row[i]]));
      }
      return {};
    };

    // Analyze by BU
    const byBu = {};
    const issues = [];

    for (const row of items) {
      const r = rowToObj(row, cols);

      const buRaw =
        (r && typeof r === 'object' && !Array.isArray(r) ? (
          r['Invoice Business Unit'] ??
          r['Job Business Unit'] ??
          r['Business Unit']
        ) : null) ?? (Array.isArray(row) ? row[7] : null);
      const buKey = typeof buRaw === 'string' ? buRaw.trim() : buRaw || 'unknown';

      const amountRaw = (r && typeof r === 'object' && !Array.isArray(r) ? (r['Amount'] ?? r['Payment Amount']) : null) ?? (Array.isArray(row) ? row[2] : null);
      const amount = toNumber(amountRaw);

      const invoiceNumber = (r && typeof r === 'object' && !Array.isArray(r) ? r['Invoice Number'] : null) ?? (Array.isArray(row) ? row[3] : null);
      const paidOnStr = (r && typeof r === 'object' && !Array.isArray(r) ? r['Paid On'] : null) ?? (Array.isArray(row) ? row[0] : null);

      if (!byBu[buKey]) {
        byBu[buKey] = {
          total_with_negatives: 0,
          total_positives_only: 0,
          count: 0,
          positive_count: 0,
          negative_count: 0,
          negative_total: 0,
          zero_count: 0,
          negative_samples: [],
          positive_samples: []
        };
      }

      byBu[buKey].count++;
      byBu[buKey].total_with_negatives += (amount ?? 0);

      if (amount === null || amount === 0) {
        byBu[buKey].zero_count++;
      } else if (amount < 0) {
        byBu[buKey].negative_count++;
        byBu[buKey].negative_total += amount;
        if (byBu[buKey].negative_samples.length < 5) {
          byBu[buKey].negative_samples.push({ invoice: invoiceNumber, amount: amount, paid_on: paidOnStr });
        }
      } else {
        byBu[buKey].positive_count++;
        byBu[buKey].total_positives_only += amount;
        if (byBu[buKey].positive_samples.length < 3) {
          byBu[buKey].positive_samples.push({ invoice: invoiceNumber, amount: amount, paid_on: paidOnStr });
        }
      }

      // Flag potential issues
      if (amount === null) {
        issues.push({ issue: 'null_amount', invoice: invoiceNumber, bu: buKey, raw_amount: amountRaw });
      }
    }

    // Round totals for readability
    for (const bu in byBu) {
      byBu[bu].total_with_negatives = Math.round(byBu[bu].total_with_negatives * 100) / 100;
      byBu[bu].total_positives_only = Math.round(byBu[bu].total_positives_only * 100) / 100;
      byBu[bu].negative_total = Math.round(byBu[bu].negative_total * 100) / 100;
    }

    res.json({
      date_range: { from, to },
      total_rows: items.length,
      columns: cols,
      by_bu: byBu,
      issues: issues.length > 0 ? issues.slice(0, 20) : undefined,
      sample_row: items[0] || null
    });
  } catch (e) {
    console.error('debug/collections error:', e);
    res.status(500).json({ error: String(e) });
  }
});

// --------FOREMAN------------

app.get('/ingest/foreman', async (req, res) => {
  try {
    const reportId = cfg.servicetitan.report_ids.foreman_job_cost_this_week;
    const destTable = cfg.bigquery.raw_tables.foreman;
    const days = Number(req.query.days || cfg.servicetitan.date_windows.incremental_pull_days || 7);
    const offsetDays = Number(req.query.offset_days || 0);

    const qFrom = req.query.from;
    const qTo = req.query.to;
    const dateType = Number(req.query.dateType || 3);

    let params;
    if (qFrom && qTo) {
      params = { From: String(qFrom), To: String(qTo), DateType: dateType };
    } else {
      const end = new Date();
      end.setDate(end.getDate() - offsetDays);
      const start = new Date(end);
      start.setDate(end.getDate() - (days - 1));
      params = {
        From: start.toISOString().slice(0, 10),
        To: end.toISOString().slice(0, 10),
        DateType: dateType
      };
    }

    // Filter for Production BUs only
    if (!Object.keys(buNameIdMap || {}).length) {
      await initBuMap().catch(() => null);
    }

    const productionBuNames = (() => {
      const names = new Set();
      for (const entry of cfg?.bu_mapping || []) {
        for (const n of entry?.production_bu_names || []) names.add(n);
      }
      if (names.size) return [...names];
      return Object.keys(buNameIdMap || {}).filter(name => /-Production$/i.test(name));
    })();

    const buIds = productionBuNames
      .map(name => buNameIdMap[name])
      .filter(id => id);

    if (buIds.length > 0) {
      params.BusinessUnitIds = buIds;
    }

    console.log('[Foreman:req]', { reportId, params, productionBUs: productionBuNames.length });

    const st = await makeStClient(cfg);
    const resp = await st.fetchReport('report-category/operations', reportId, params);

    const cols = (resp?.data?.columns || resp?.columns || []).map(c => c?.name ?? c);
    const rawItems = resp?.data?.items || resp?.items || [];
    const items = Array.isArray(rawItems) ? rawItems : [];

    console.log('[Foreman:resp]', { items_len: items.length, columns: cols, sample: items[0] });

    const rowToObj = (row, cols) => {
      if (row && typeof row === 'object' && !Array.isArray(row)) return row;
      if (Array.isArray(row) && cols.length === row.length) {
        return Object.fromEntries(cols.map((n, i) => [n, row[i]]));
      }
      return {};
    };

    const rows = items.map(row => {
      const r = rowToObj(row, cols);

      // New report column positions:
      // 0: Scheduled Date, 1: Business Unit, 2: Sold By, 3: Primary Technician
      // 4: Job Type, 5: Customer Name, 6: Job #, 7: Jobs Subtotal
      // 8: Labor Pay, 9: Payroll Adjustments, 10: Material+Equip+PO/Bills Cost, 11: Returns
      // 12: Jobs Total Cost, 13: Jobs Gross Margin %, 14: Status
      const bu_key = r['Business Unit'] || (Array.isArray(row) ? row[1] : 'unknown');
      const job_id = String(r['Job #'] || r['Job Number'] || (Array.isArray(row) ? row[6] : ''));
      const job_start = r['Scheduled Date'] || r.ScheduledDate || (Array.isArray(row) ? row[0] : new Date());
      const job_type = r['Job Type'] || (Array.isArray(row) ? row[4] : null);
      const job_subtotal = toNumber(r['Jobs Subtotal'] ?? r.JobsSubtotal ?? (Array.isArray(row) ? row[7] : null)) ?? 0;
      const job_total_costs = toNumber(r['Jobs Total Cost'] ?? r.JobsTotalCost ?? (Array.isArray(row) ? row[12] : null)) ?? 0;
      const job_gm_pct = toNumber(r['Jobs Gross Margin %'] ?? r.JobsGrossMarginPct ?? (Array.isArray(row) ? row[13] : null)) ?? 0;

      return {
        bu_key,
        job_id,
        job_start: new Date(job_start),
        job_type,
        job_subtotal: Math.round(job_subtotal * 100) / 100,
        job_total_costs: Math.round(job_total_costs * 100) / 100,
        job_gm_pct: Math.round(job_gm_pct * 10000) / 10000,
        updated_on: new Date(),
        raw: JSON.stringify(row)
      };
    });

    if (rows.length) await insertRows('st_raw', destTable, rows);
    res.json({ status: 'ok', items: rows.length });
  } catch (e) {
    console.error('foreman error:', e);
    res.status(500).json({ status: 'error', message: String(e) });
  }
});


// ----------------------- FUTURE BOOKINGS -----------------------
app.get('/ingest/future_bookings', async (req, res) => {
  try {
    const reportId = cfg.servicetitan.report_ids.foreman_job_cost_this_week;
    const destTable = 'raw_future_bookings';

    // Support explicit from/to dates OR days parameter (forward-looking)
    let start, end;
    if (req.query.from && req.query.to) {
      start = new Date(`${req.query.from}T00:00:00`);
      end = new Date(`${req.query.to}T00:00:00`);
    } else {
      const days = Number(req.query.days || cfg.servicetitan.date_windows.future_bookings_horizon_days || 365);
      start = new Date();
      end = new Date(start);
      end.setDate(start.getDate() + (days - 1));
    }

    const params = {
      From: start.toISOString().slice(0, 10),
      To: end.toISOString().slice(0, 10),
      DateType: 3  // DateType 3 = Scheduled Start Date
    };

    // Filter for Production BUs only
    if (!Object.keys(buNameIdMap || {}).length) {
      await initBuMap().catch(() => null);
    }

    const productionBuNames = (() => {
      const names = new Set();
      for (const entry of cfg?.bu_mapping || []) {
        for (const n of entry?.production_bu_names || []) names.add(n);
      }
      if (names.size) return [...names];
      return Object.keys(buNameIdMap || {}).filter(name => /-Production$/i.test(name));
    })();

    const buIds = productionBuNames
      .map(name => buNameIdMap[name])
      .filter(id => id);

    if (buIds.length > 0) {
      params.BusinessUnitIds = buIds;
    }

    console.log('[FutureBookings:req]', { reportId, params, productionBUs: productionBuNames.length });

    const st = await makeStClient(cfg);
    const resp = await st.fetchReport('report-category/operations', reportId, params);

    const cols = (resp?.data?.columns || resp?.columns || []).map(c => c?.name ?? c);
    const rawItems = resp?.data?.items || resp?.items || [];
    const items = Array.isArray(rawItems) ? rawItems : [];

    console.log('[FutureBookings:resp]', { items_len: items.length, columns: cols, sample: items[0] });

    const rowToObj = (row, cols) => {
      if (row && typeof row === 'object' && !Array.isArray(row)) return row;
      if (Array.isArray(row) && cols.length === row.length) {
        return Object.fromEntries(cols.map((n, i) => [n, row[i]]));
      }
      return {};
    };

    const rows = items.map(row => {
      const r = rowToObj(row, cols);

      // Using same column positions as Foreman report:
      // 0: Scheduled Date, 1: Business Unit, 4: Job Type, 6: Job #, 7: Jobs Subtotal
      const bu_key = r['Business Unit'] || (Array.isArray(row) ? row[1] : 'unknown');
      const job_id = String(r['Job #'] || r['Job Number'] || (Array.isArray(row) ? row[6] : ''));
      const scheduled_date = r['Scheduled Date'] || r.ScheduledDate || (Array.isArray(row) ? row[0] : new Date());
      const job_type = r['Job Type'] || (Array.isArray(row) ? row[4] : null);
      const job_subtotal = toNumber(r['Jobs Subtotal'] ?? r.JobsSubtotal ?? (Array.isArray(row) ? row[7] : null)) ?? 0;

      return {
        bu_key,
        job_id,
        scheduled_date: new Date(scheduled_date),
        job_type,
        job_subtotal: Math.round(job_subtotal * 100) / 100,
        as_of_date: new Date(),
        raw: JSON.stringify(row)
      };
    });

    if (rows.length) await insertRows('st_raw', destTable, rows);
    res.json({ status: 'ok', items: rows.length, total_bookings: rows.reduce((sum, r) => sum + r.job_subtotal, 0) });
  } catch (e) {
    console.error('future_bookings error:', e);
    res.status(500).json({ status: 'error', message: String(e) });
  }
});

// ----------------------- WBR WORKER -----------------------
async function ingestDailyWbrForBu(buName, buId, { days = 3, toStr = null, fromStr = null, dateType = 3, offsetDays = 0 } = {}) {
  const reportId = (cfg?.servicetitan?.daily_wbr_report_ids_by_bu || {})[buName];
  if (!reportId) throw new Error(`No Daily WBR report id configured for BU: ${buName}`);

  const tz = cfg.timezone || 'America/Phoenix';

  let endStr, startStr;
  if (toStr && fromStr) {
    startStr = fromStr;
    endStr = toStr;
  } else if (toStr) {
    const toDate = new Date(`${toStr}T12:00:00`);
    toDate.setDate(toDate.getDate() - offsetDays);
    const startDate = new Date(toDate.getTime() - (days - 1) * 86400000);
    startStr = ymdInTz(startDate, tz);
    endStr = ymdInTz(toDate, tz);
  } else {
    const now = new Date();
    now.setDate(now.getDate() - offsetDays);
    endStr = ymdInTz(now, tz);
    const fromDate = new Date(now.getTime() - (days - 1) * 86400000);
    startStr = ymdInTz(fromDate, tz);
  }

  const st = await makeStClient(cfg);
  let totalInserted = 0;

  const safeNum = (v) => toNumber(v);
  const safeInt = (v) => { const n = toNumber(v); return n == null ? null : Math.trunc(n); };
  const roundScale = (v, s) => { const n = toNumber(v); return n == null ? null : Number(n.toFixed(s)); };
  const toRate = (v) => {
    if (v == null || v === '') return null;
    const n = Number(String(v).replace('%', '').trim());
    if (!Number.isFinite(n)) return null;
    return n > 1 ? n / 100 : n;
  };
  const rowToObj = (row, cols) => {
    if (row && typeof row === 'object' && !Array.isArray(row)) return row;
    if (Array.isArray(row) && cols.length === row.length) {
      return Object.fromEntries(cols.map((n, i) => [n, row[i]]));
    }
    return { _row: row };
  };

  // Make a single API call for the entire date range instead of one per day
  const params = {
    From: startStr,
    To: endStr,
    DateType: Number.isFinite(dateType) ? dateType : 3,
    IncludeInactiveTechnicians: true,
  };

  if (Number.isFinite(buId)) {
    params.BusinessUnitIds = [buId];
  }
  if (Array.isArray(cfg?.servicetitan?.job_type_ids_sales) && cfg.servicetitan.job_type_ids_sales.length) {
    params.JobTypeIds = cfg.servicetitan.job_type_ids_sales;
  }

  console.log('[WBR:req]', { buName, dateRange: `${startStr} to ${endStr}`, buId, reportId, params });

  let resp;
  try {
    resp = await st.fetchReport('report-category/technician', reportId, params);
  } catch (e) {
    console.error('[WBR:fetchError]', { buName, dateRange: `${startStr} to ${endStr}`, buId, reportId, params, err: String(e) });
    throw e;
  }

  const cols = (resp?.data?.columns || resp?.columns || []).map(c => c?.name ?? c);
  const rawItems = resp?.data?.items || resp?.items || [];
  const items = Array.isArray(rawItems) ? rawItems : [];

  console.log('[WBR:resp]', { buName, dateRange: `${startStr} to ${endStr}`, items_len: items.length, sample: items[0] || null });

  const nowTs = new Date();
  const rows = items.map(row => {
    const r = rowToObj(row, cols);
    const eventDateStr = (r['Date'] || r['Day'] || r['Sold On'] || r['Job Creation Date'] || startStr).toString().slice(0, 10);

    const estimates = safeInt(r['Sales Opportunities'] ?? r.SalesOpportunities);
    const booked = safeInt(r['Closed Opportunities'] ?? r.CompletedJob ?? r.Completed);
    const total = safeNum(r['Total Sales'] ?? r.TotalSales ?? r.TotalRevenue ?? r.SoldAmount ?? r.BookedRevenue);
    const avgClosed = safeNum((booked ?? 0) > 0 && total != null ? (total / booked) : (r['Average Closed Sale'] ?? r.AverageClosedSale));
    let cr = toRate(r['Close Rate'] ?? r.CloseRate);
    cr = (cr == null || !Number.isFinite(cr)) ? null : cr;

    return {
      event_date: eventDateStr,
      bu_name: buName,
      estimator: r.Estimator ?? r.Technician ?? (Array.isArray(row) ? String(row[0] ?? '') : null),
      sales_opportunities: estimates ?? (Array.isArray(row) ? safeInt(row[3]) : null),
      completed_est: booked ?? (Array.isArray(row) ? safeInt(row[2]) : null),
      closed_opportunities: safeInt(r['Closed Opportunities'] ?? r.ClosedOpportunities ?? booked ?? (Array.isArray(row) ? row[1] : null)),
      close_rate: roundScale(cr, 6),
      total_sales: roundScale(total ?? (Array.isArray(row) ? safeNum(row[5]) : null), 2),
      avg_closed_sale: roundScale(avgClosed ?? (Array.isArray(row) ? safeNum(row[6]) : null), 2),
      updated_on: nowTs
    };
  });

  if (rows.length) {
    await insertRows('st_raw', 'raw_daily_wbr_v2', rows);
    totalInserted = rows.length;
  }

  return { status: 'ok', items: totalInserted };
}

// ----------------------- DAILY WBR ENDPOINT -----------------------
app.get('/ingest/daily_wbr', async (req, res) => {
  try {
    const bu = req.query.bu;
    if (!bu) {
      return res.status(400).json({ status: 'error', message: 'Missing required parameter: bu' });
    }

    const buId = buNameIdMap[bu];
    if (!buId) {
      return res.status(400).json({ status: 'error', message: `Unknown BU: ${bu}` });
    }

    const days = Number(req.query.days || 3);
    const toStr = req.query.to || null;
    const fromStr = req.query.from || null;
    const dateType = Number(req.query.dateType || 3);
    const offsetDays = Number(req.query.offset_days || 0);

    const result = await ingestDailyWbrForBu(bu, buId, { days, toStr, fromStr, dateType, offsetDays });
    res.json(result);
  } catch (e) {
    console.error('daily_wbr error:', e);
    res.status(500).json({ status: 'error', message: String(e) });
  }
});

// ----------------------- DAILY WBR CONSOLIDATED ENDPOINT -----------------------
app.get('/ingest/daily_wbr_consolidated', async (req, res) => {
  try {
    const reportId = cfg.servicetitan.report_ids.daily_wbr_consolidated;
    const destTable = 'raw_daily_wbr_consolidated';

    // Support explicit from/to dates OR days parameter
    let start, end;
    if (req.query.from && req.query.to) {
      start = new Date(`${req.query.from}T00:00:00`);
      end = new Date(`${req.query.to}T00:00:00`);
    } else {
      const days = Number(req.query.days || cfg.servicetitan.date_windows.incremental_pull_days || 7);
      const offsetDays = Number(req.query.offset_days || 0);
      end = new Date();
      end.setDate(end.getDate() - offsetDays);
      start = new Date(end);
      start.setDate(end.getDate() - (days - 1));
    }

    const dateType = Number(req.query.dateType || 3);

    const params = {
      From: start.toISOString().slice(0, 10),
      To: end.toISOString().slice(0, 10),
      DateType: dateType
    };

    console.log('[WBR-Consolidated:req]', { reportId, params });

    const st = await makeStClient(cfg);
    const resp = await st.fetchReport('report-category/technician', reportId, params);

    const cols = (resp?.data?.columns || resp?.columns || []).map(c => c?.name ?? c);
    const rawItems = resp?.data?.items || resp?.items || [];
    const items = Array.isArray(rawItems) ? rawItems : [];

    console.log('[WBR-Consolidated:resp]', { items_len: items.length, columns: cols, sample: items[0] });

    const rowToObj = (row, cols) => {
      if (row && typeof row === 'object' && !Array.isArray(row)) return row;
      if (Array.isArray(row) && cols.length === row.length) {
        return Object.fromEntries(cols.map((n, i) => [n, row[i]]));
      }
      return {};
    };

    const nowTs = new Date();
    const rows = items.map(row => {
      const r = rowToObj(row, cols);

      // Schema: Name, Sales Opportunity, Closed Opportunities, Close rate, Completed Jobs, Total Sales, Closed Average Sale
      const buName = r['Name'] || (Array.isArray(row) ? row[0] : 'unknown');
      const salesOpportunities = toNumber(r['Sales Opportunity'] || (Array.isArray(row) ? row[1] : null));
      const closedOpportunities = toNumber(r['Closed Opportunities'] || (Array.isArray(row) ? row[2] : null));
      const closeRateRaw = r['Close rate'] || (Array.isArray(row) ? row[3] : null);
      const completedJobs = toNumber(r['Completed Jobs'] || (Array.isArray(row) ? row[4] : null));
      const totalSales = toNumber(r['Total Sales'] || (Array.isArray(row) ? row[5] : null));
      const closedAvgSale = toNumber(r['Closed Average Sale'] || (Array.isArray(row) ? row[6] : null));

      // Parse close rate (might be a percentage string like "45%" or decimal like 0.45)
      let closeRate = null;
      if (closeRateRaw !== null && closeRateRaw !== undefined) {
        const rateStr = String(closeRateRaw).replace('%', '').trim();
        const rateNum = Number(rateStr);
        if (!isNaN(rateNum)) {
          // If > 1, assume it's a percentage, convert to decimal
          closeRate = rateNum > 1 ? rateNum / 100 : rateNum;
        }
      }

      return {
        event_date: params.From,
        bu_name: buName,
        sales_opportunities: Math.round(salesOpportunities || 0),
        closed_opportunities: Math.round(closedOpportunities || 0),
        close_rate: closeRate,
        completed_est: Math.round(completedJobs || 0),
        total_sales: Math.round((totalSales || 0) * 100) / 100,
        avg_closed_sale: Math.round((closedAvgSale || 0) * 100) / 100,
        updated_on: nowTs,
        raw: JSON.stringify(row)
      };
    });

    if (rows.length) await insertRows('st_raw', destTable, rows);
    res.json({ status: 'ok', items: rows.length, date_range: params });
  } catch (e) {
    console.error('daily_wbr_consolidated error:', e);
    res.status(500).json({ status: 'error', message: String(e) });
  }
});

// ----------------------- DAILY WBR CONSOLIDATED DEBUG -----------------------
app.get('/debug/daily_wbr_consolidated', async (req, res) => {
  try {
    const reportId = cfg.servicetitan.report_ids.daily_wbr_consolidated;
    const from = req.query.from || '2024-10-06';
    const to = req.query.to || '2024-10-12';
    const dateType = Number(req.query.dateType || 3);

    const params = { From: from, To: to, DateType: dateType };

    console.log('[WBR-Consolidated-Debug:req]', { reportId, params });

    const st = await makeStClient(cfg);
    const resp = await st.fetchReport('report-category/technician', reportId, params);

    const cols = (resp?.data?.columns || resp?.columns || []).map(c => c?.name ?? c);
    const rawItems = resp?.data?.items || resp?.items || [];
    const items = Array.isArray(rawItems) ? rawItems : [];

    console.log('[WBR-Consolidated-Debug:resp]', { items_len: items.length, columns: cols });

    const rowToObj = (row, cols) => {
      if (row && typeof row === 'object' && !Array.isArray(row)) return row;
      if (Array.isArray(row) && cols.length === row.length) {
        return Object.fromEntries(cols.map((n, i) => [n, row[i]]));
      }
      return {};
    };

    // Parse and summarize by BU
    const byBu = {};
    let totalSalesSum = 0;

    for (const row of items) {
      const r = rowToObj(row, cols);
      const buName = r['Name'] || (Array.isArray(row) ? row[0] : 'unknown');
      const totalSales = toNumber(r['Total Sales'] || (Array.isArray(row) ? row[5] : null)) || 0;
      const salesOpportunities = toNumber(r['Sales Opportunity'] || (Array.isArray(row) ? row[1] : null)) || 0;
      const closedOpportunities = toNumber(r['Closed Opportunities'] || (Array.isArray(row) ? row[2] : null)) || 0;
      const closeRateRaw = r['Close rate'] || (Array.isArray(row) ? row[3] : null);
      const completedJobs = toNumber(r['Completed Jobs'] || (Array.isArray(row) ? row[4] : null)) || 0;
      const closedAvgSale = toNumber(r['Closed Average Sale'] || (Array.isArray(row) ? row[6] : null)) || 0;

      byBu[buName] = {
        sales_opportunities: salesOpportunities,
        closed_opportunities: closedOpportunities,
        close_rate: closeRateRaw,
        completed_jobs: completedJobs,
        total_sales: Math.round(totalSales * 100) / 100,
        avg_closed_sale: Math.round(closedAvgSale * 100) / 100
      };

      totalSalesSum += totalSales;
    }

    res.json({
      date_range: { from, to },
      total_rows: items.length,
      columns: cols,
      by_bu: byBu,
      total_sales_sum: Math.round(totalSalesSum * 100) / 100,
      sample_row: items[0] || null,
      note: 'Consolidated WBR returns ~10 rows (one per BU) regardless of date range. No daily loops needed.'
    });
  } catch (e) {
    console.error('debug/daily_wbr_consolidated error:', e);
    res.status(500).json({ error: String(e) });
  }
});

// ----------------------- DEDUPE ENDPOINTS -----------------------
app.post('/dedupe/daily_wbr', async (_req, res) => {
  try {
    const { query } = await import('./bq.js');

    console.log('[Dedupe] Starting daily_wbr dedupe...');
    const startTime = Date.now();

    const dedupeSQL = `
      CREATE OR REPLACE TABLE \`kpi-auto-471020.st_raw.raw_daily_wbr_v2\` AS
      WITH ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (
                 PARTITION BY event_date, bu_name, estimator
                 ORDER BY total_sales DESC, avg_closed_sale DESC
               ) AS rn
        FROM \`kpi-auto-471020.st_raw.raw_daily_wbr_v2\`
      )
      SELECT * EXCEPT(rn)
      FROM ranked
      WHERE rn = 1
    `;

    await query(dedupeSQL);

    const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[Dedupe] Complete in ${elapsedSec}s`);

    res.json({
      status: 'ok',
      message: 'Daily WBR dedupe completed successfully',
      elapsed_seconds: parseFloat(elapsedSec)
    });
  } catch (e) {
    console.error('[Dedupe] Error:', e);
    res.status(500).json({
      status: 'error',
      message: String(e)
    });
  }
});

app.post('/dedupe/leads', async (_req, res) => {
  try {
    const { query } = await import('./bq.js');

    console.log('[Dedupe] Starting leads dedupe...');
    const startTime = Date.now();

    const dedupeSQL = `
      CREATE OR REPLACE TABLE \`kpi-auto-471020.st_raw.raw_leads\` AS
      WITH ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (
                 PARTITION BY job_id, bu_key
                 ORDER BY job_created_on DESC, updated_on DESC
               ) AS rn
        FROM \`kpi-auto-471020.st_raw.raw_leads\`
      )
      SELECT * EXCEPT(rn)
      FROM ranked
      WHERE rn = 1
    `;

    await query(dedupeSQL);

    const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[Dedupe] Complete in ${elapsedSec}s`);

    res.json({
      status: 'ok',
      message: 'Leads dedupe completed successfully',
      elapsed_seconds: parseFloat(elapsedSec)
    });
  } catch (e) {
    console.error('[Dedupe] Error:', e);
    res.status(500).json({
      status: 'error',
      message: String(e)
    });
  }
});

app.post('/dedupe/foreman', async (_req, res) => {
  try {
    const { query } = await import('./bq.js');

    console.log('[Dedupe] Starting foreman dedupe...');
    const startTime = Date.now();

    const dedupeSQL = `
      CREATE OR REPLACE TABLE \`kpi-auto-471020.st_raw.raw_foreman\` AS
      WITH ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (
                 PARTITION BY job_id, bu_key
                 ORDER BY job_start DESC, updated_on DESC
               ) AS rn
        FROM \`kpi-auto-471020.st_raw.raw_foreman\`
      )
      SELECT * EXCEPT(rn)
      FROM ranked
      WHERE rn = 1
    `;

    await query(dedupeSQL);

    const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[Dedupe] Complete in ${elapsedSec}s`);

    res.json({
      status: 'ok',
      message: 'Foreman dedupe completed successfully',
      elapsed_seconds: parseFloat(elapsedSec)
    });
  } catch (e) {
    console.error('[Dedupe] Error:', e);
    res.status(500).json({
      status: 'error',
      message: String(e)
    });
  }
});

app.post('/dedupe/future_bookings', async (_req, res) => {
  try {
    const { query } = await import('./bq.js');

    console.log('[Dedupe] Starting future_bookings dedupe...');
    const startTime = Date.now();

    const dedupeSQL = `
      CREATE OR REPLACE TABLE \`kpi-auto-471020.st_raw.raw_future_bookings\` AS
      WITH ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (
                 PARTITION BY job_id, bu_key
                 ORDER BY scheduled_date DESC, as_of_date DESC
               ) AS rn
        FROM \`kpi-auto-471020.st_raw.raw_future_bookings\`
      )
      SELECT * EXCEPT(rn)
      FROM ranked
      WHERE rn = 1
    `;

    await query(dedupeSQL);

    const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[Dedupe] Complete in ${elapsedSec}s`);

    res.json({
      status: 'ok',
      message: 'Future Bookings dedupe completed successfully',
      elapsed_seconds: parseFloat(elapsedSec)
    });
  } catch (e) {
    console.error('[Dedupe] Error:', e);
    res.status(500).json({
      status: 'error',
      message: String(e)
    });
  }
});

app.post('/dedupe/collections', async (_req, res) => {
  try {
    const { query } = await import('./bq.js');

    console.log('[Dedupe] Starting collections dedupe...');
    const startTime = Date.now();

    // Collections dedupe: Keep ALL records from API since duplicate-looking rows may be legitimate
    // separate payments. Only dedupe by adding a row number to each unique combination.
    const dedupeSQL = `
      CREATE OR REPLACE TABLE \`kpi-auto-471020.st_raw.raw_collections\` AS
      SELECT *
      FROM \`kpi-auto-471020.st_raw.raw_collections\`
    `;

    await query(dedupeSQL);

    const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[Dedupe] Complete in ${elapsedSec}s`);

    res.json({
      status: 'ok',
      message: 'Collections dedupe completed successfully',
      elapsed_seconds: parseFloat(elapsedSec)
    });
  } catch (e) {
    console.error('[Dedupe] Error:', e);
    res.status(500).json({
      status: 'error',
      message: String(e)
    });
  }
});

// ----------------------- MART UPDATE ENDPOINTS -----------------------
app.post('/mart/update/leads', async (_req, res) => {
  try {
    const { query } = await import('./bq.js');
    console.log('[Mart] Starting leads_daily_fact update...');
    const startTime = Date.now();

    const updateSQL = `
      CREATE OR REPLACE TABLE \`kpi-auto-471020.st_kpi_mart.leads_daily_fact\` AS
      SELECT
        DATE(job_created_on) as kpi_date,
        bu_key,
        COUNT(*) as leads
      FROM \`kpi-auto-471020.st_raw.raw_leads\`
      WHERE job_created_on IS NOT NULL
        AND bu_key IS NOT NULL
      GROUP BY kpi_date, bu_key
    `;

    await query(updateSQL);
    const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[Mart] Complete in ${elapsedSec}s`);

    res.json({
      status: 'ok',
      message: 'Leads mart updated successfully',
      elapsed_seconds: parseFloat(elapsedSec)
    });
  } catch (e) {
    console.error('[Mart] Error:', e);
    res.status(500).json({
      status: 'error',
      message: String(e)
    });
  }
});

app.post('/mart/update/collections', async (_req, res) => {
  try {
    const { query } = await import('./bq.js');
    console.log('[Mart] Starting collections_daily_fact update...');
    const startTime = Date.now();

    const updateSQL = `
      CREATE OR REPLACE TABLE \`kpi-auto-471020.st_kpi_mart.collections_daily_fact\` AS
      SELECT
        DATE(payment_date) as kpi_date,
        bu_key,
        ROUND(SUM(amount), 2) as collected_amount
      FROM \`kpi-auto-471020.st_raw.raw_collections\`
      WHERE payment_date IS NOT NULL
        AND bu_key IS NOT NULL
      GROUP BY kpi_date, bu_key
    `;

    await query(updateSQL);
    const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[Mart] Complete in ${elapsedSec}s`);

    res.json({
      status: 'ok',
      message: 'Collections mart updated successfully',
      elapsed_seconds: parseFloat(elapsedSec)
    });
  } catch (e) {
    console.error('[Mart] Error:', e);
    res.status(500).json({
      status: 'error',
      message: String(e)
    });
  }
});

app.post('/mart/update/wbr', async (_req, res) => {
  try {
    const { query } = await import('./bq.js');
    console.log('[Mart] Starting wbr_daily_fact update...');
    const startTime = Date.now();

    const updateSQL = `
      CREATE OR REPLACE TABLE \`kpi-auto-471020.st_kpi_mart.wbr_daily_fact\` AS
      SELECT
        event_date as kpi_date,
        bu_name as bu_key,
        SUM(sales_opportunities) as estimates,
        SUM(closed_opportunities) as booked,
        SUM(completed_est) as completed_est,
        ROUND(SUM(closed_opportunities) / NULLIF(SUM(sales_opportunities), 0), 4) as close_rate_decimal,
        ROUND(SUM(total_sales), 2) as total_sales,
        ROUND(SUM(total_sales) / NULLIF(SUM(closed_opportunities), 0), 2) as avg_closed_sale
      FROM \`kpi-auto-471020.st_raw.raw_daily_wbr_v2\`
      WHERE event_date IS NOT NULL
        AND bu_name IS NOT NULL
      GROUP BY kpi_date, bu_key
    `;

    await query(updateSQL);
    const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[Mart] Complete in ${elapsedSec}s`);

    res.json({
      status: 'ok',
      message: 'WBR mart updated successfully',
      elapsed_seconds: parseFloat(elapsedSec)
    });
  } catch (e) {
    console.error('[Mart] Error:', e);
    res.status(500).json({
      status: 'error',
      message: String(e)
    });
  }
});

app.post('/mart/update/foreman', async (_req, res) => {
  try {
    const { query } = await import('./bq.js');
    console.log('[Mart] Starting foreman_daily_fact update...');
    const startTime = Date.now();

    // Create table if it doesn't exist
    const createSQL = `
      CREATE TABLE IF NOT EXISTS \`kpi-auto-471020.st_kpi_mart.foreman_daily_fact\` (
        kpi_date DATE,
        bu_key STRING,
        total_jobs INT64,
        total_subtotal NUMERIC,
        total_costs NUMERIC,
        gm_pct NUMERIC
      )
    `;
    await query(createSQL);

    const updateSQL = `
      CREATE OR REPLACE TABLE \`kpi-auto-471020.st_kpi_mart.foreman_daily_fact\` AS
      SELECT
        DATE(job_start) as kpi_date,
        bu_key,
        COUNT(*) as total_jobs,
        ROUND(SUM(job_subtotal), 2) as total_subtotal,
        ROUND(SUM(job_total_costs), 2) as total_costs,
        ROUND((SUM(job_subtotal) - SUM(job_total_costs)) * 100.0 / NULLIF(SUM(job_subtotal), 0), 2) as gm_pct
      FROM \`kpi-auto-471020.st_raw.raw_foreman\`
      WHERE job_start IS NOT NULL
        AND bu_key IS NOT NULL
      GROUP BY kpi_date, bu_key
    `;

    await query(updateSQL);
    const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[Mart] Complete in ${elapsedSec}s`);

    res.json({
      status: 'ok',
      message: 'Foreman mart updated successfully',
      elapsed_seconds: parseFloat(elapsedSec)
    });
  } catch (e) {
    console.error('[Mart] Error:', e);
    res.status(500).json({
      status: 'error',
      message: String(e)
    });
  }
});

app.post('/mart/update/consolidated', async (_req, res) => {
  try {
    const { query } = await import('./bq.js');
    console.log('[Mart] Starting consolidated KPI mart update...');
    const startTime = Date.now();

    const consolidatedSQL = `
      CREATE OR REPLACE TABLE \`kpi-auto-471020.st_kpi_mart.kpi_daily_consolidated\` AS
      WITH date_bu_spine AS (
        SELECT DISTINCT kpi_date, bu_key FROM \`kpi-auto-471020.st_kpi_mart.leads_daily_fact\`
        UNION DISTINCT
        SELECT DISTINCT kpi_date, bu_key FROM \`kpi-auto-471020.st_kpi_mart.collections_daily_fact\`
        UNION DISTINCT
        SELECT DISTINCT kpi_date, bu_key FROM \`kpi-auto-471020.st_kpi_mart.wbr_daily_fact\`
        UNION DISTINCT
        SELECT DISTINCT kpi_date, bu_key FROM \`kpi-auto-471020.st_kpi_mart.foreman_daily_fact\`
      )
      SELECT
        s.kpi_date,
        s.bu_key,
        l.leads,
        c.collected_amount,
        w.estimates as sales_opportunities,
        w.booked as closed_opportunities,
        w.close_rate_decimal,
        w.completed_est,
        w.total_sales as wbr_total_sales,
        w.avg_closed_sale,
        f.total_jobs as completed_jobs,
        f.total_subtotal as job_subtotal,
        f.total_costs as job_costs,
        f.gm_pct as gross_margin_pct
      FROM date_bu_spine s
      LEFT JOIN \`kpi-auto-471020.st_kpi_mart.leads_daily_fact\` l
        ON s.kpi_date = l.kpi_date AND s.bu_key = l.bu_key
      LEFT JOIN \`kpi-auto-471020.st_kpi_mart.collections_daily_fact\` c
        ON s.kpi_date = c.kpi_date AND s.bu_key = c.bu_key
      LEFT JOIN \`kpi-auto-471020.st_kpi_mart.wbr_daily_fact\` w
        ON s.kpi_date = w.kpi_date AND s.bu_key = w.bu_key
      LEFT JOIN \`kpi-auto-471020.st_kpi_mart.foreman_daily_fact\` f
        ON s.kpi_date = f.kpi_date AND s.bu_key = f.bu_key
    `;

    await query(consolidatedSQL);
    const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[Mart] Consolidated KPI mart updated in ${elapsedSec}s`);

    res.json({
      status: 'ok',
      message: 'Consolidated KPI mart updated successfully',
      elapsed_seconds: parseFloat(elapsedSec)
    });
  } catch (e) {
    console.error('[Mart] Error:', e);
    res.status(500).json({
      status: 'error',
      message: String(e)
    });
  }
});

app.post('/mart/update/all', async (_req, res) => {
  try {
    const { query } = await import('./bq.js');
    console.log('[Mart] Starting full mart update...');
    const startTime = Date.now();
    const results = [];

    // Define all mart updates
    const marts = [
      {
        name: 'leads',
        sql: `
          CREATE OR REPLACE TABLE \`kpi-auto-471020.st_kpi_mart.leads_daily_fact\` AS
          SELECT
            DATE(job_created_on) as kpi_date,
            bu_key,
            COUNT(*) as leads
          FROM \`kpi-auto-471020.st_raw.raw_leads\`
          WHERE job_created_on IS NOT NULL AND bu_key IS NOT NULL
          GROUP BY kpi_date, bu_key
        `
      },
      {
        name: 'collections',
        sql: `
          CREATE OR REPLACE TABLE \`kpi-auto-471020.st_kpi_mart.collections_daily_fact\` AS
          SELECT
            DATE(payment_date) as kpi_date,
            bu_key,
            ROUND(SUM(amount), 2) as collected_amount
          FROM \`kpi-auto-471020.st_raw.raw_collections\`
          WHERE payment_date IS NOT NULL AND bu_key IS NOT NULL
          GROUP BY kpi_date, bu_key
        `
      },
      {
        name: 'wbr',
        sql: `
          CREATE OR REPLACE TABLE \`kpi-auto-471020.st_kpi_mart.wbr_daily_fact\` AS
          SELECT
            event_date as kpi_date,
            bu_name as bu_key,
            SUM(sales_opportunities) as estimates,
            SUM(closed_opportunities) as booked,
            SUM(completed_est) as completed_est,
            ROUND(SUM(closed_opportunities) / NULLIF(SUM(sales_opportunities), 0), 4) as close_rate_decimal,
            ROUND(SUM(total_sales), 2) as total_sales,
            ROUND(SUM(total_sales) / NULLIF(SUM(closed_opportunities), 0), 2) as avg_closed_sale
          FROM \`kpi-auto-471020.st_raw.raw_daily_wbr_consolidated\`
          WHERE event_date IS NOT NULL AND bu_name IS NOT NULL
          GROUP BY kpi_date, bu_key
        `
      },
      {
        name: 'foreman',
        sql: `
          CREATE TABLE IF NOT EXISTS \`kpi-auto-471020.st_kpi_mart.foreman_daily_fact\` (
            kpi_date DATE, bu_key STRING, total_jobs INT64,
            total_subtotal NUMERIC, total_costs NUMERIC, gm_pct NUMERIC
          );
          CREATE OR REPLACE TABLE \`kpi-auto-471020.st_kpi_mart.foreman_daily_fact\` AS
          SELECT
            DATE(job_start) as kpi_date,
            bu_key,
            COUNT(*) as total_jobs,
            ROUND(SUM(job_subtotal), 2) as total_subtotal,
            ROUND(SUM(job_total_costs), 2) as total_costs,
            ROUND((SUM(job_subtotal) - SUM(job_total_costs)) * 100.0 / NULLIF(SUM(job_subtotal), 0), 2) as gm_pct
          FROM \`kpi-auto-471020.st_raw.raw_foreman\`
          WHERE job_start IS NOT NULL AND bu_key IS NOT NULL
          GROUP BY kpi_date, bu_key
        `
      },
      {
        name: 'consolidated',
        sql: `
          CREATE OR REPLACE TABLE \`kpi-auto-471020.st_kpi_mart.kpi_daily_consolidated\` AS
          WITH date_bu_spine AS (
            SELECT DISTINCT kpi_date, bu_key FROM \`kpi-auto-471020.st_kpi_mart.leads_daily_fact\`
            UNION DISTINCT
            SELECT DISTINCT kpi_date, bu_key FROM \`kpi-auto-471020.st_kpi_mart.collections_daily_fact\`
            UNION DISTINCT
            SELECT DISTINCT kpi_date, bu_key FROM \`kpi-auto-471020.st_kpi_mart.wbr_daily_fact\`
            UNION DISTINCT
            SELECT DISTINCT kpi_date, bu_key FROM \`kpi-auto-471020.st_kpi_mart.foreman_daily_fact\`
          )
          SELECT
            s.kpi_date,
            s.bu_key,
            l.leads,
            c.collected_amount,
            w.estimates as sales_opportunities,
            w.booked as closed_opportunities,
            w.close_rate_decimal,
            w.completed_est,
            w.total_sales as wbr_total_sales,
            w.avg_closed_sale,
            f.total_jobs as completed_jobs,
            f.total_subtotal as job_subtotal,
            f.total_costs as job_costs,
            f.gm_pct as gross_margin_pct
          FROM date_bu_spine s
          LEFT JOIN \`kpi-auto-471020.st_kpi_mart.leads_daily_fact\` l
            ON s.kpi_date = l.kpi_date AND s.bu_key = l.bu_key
          LEFT JOIN \`kpi-auto-471020.st_kpi_mart.collections_daily_fact\` c
            ON s.kpi_date = c.kpi_date AND s.bu_key = c.bu_key
          LEFT JOIN \`kpi-auto-471020.st_kpi_mart.wbr_daily_fact\` w
            ON s.kpi_date = w.kpi_date AND s.bu_key = w.bu_key
          LEFT JOIN \`kpi-auto-471020.st_kpi_mart.foreman_daily_fact\` f
            ON s.kpi_date = f.kpi_date AND s.bu_key = f.bu_key
        `
      }
    ];

    // Update all marts sequentially
    for (const mart of marts) {
      try {
        const martStart = Date.now();
        await query(mart.sql);
        const martElapsed = ((Date.now() - martStart) / 1000).toFixed(1);
        results.push({ mart: mart.name, status: 'ok', elapsed_seconds: parseFloat(martElapsed) });
        console.log(`[Mart] ${mart.name} updated in ${martElapsed}s`);
      } catch (e) {
        results.push({ mart: mart.name, status: 'error', message: String(e) });
        console.error(`[Mart] ${mart.name} error:`, e);
      }
    }

    const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[Mart] All marts updated in ${elapsedSec}s`);

    res.json({
      status: 'ok',
      message: 'All marts updated',
      elapsed_seconds: parseFloat(elapsedSec),
      results
    });
  } catch (e) {
    console.error('[Mart] Error:', e);
    res.status(500).json({
      status: 'error',
      message: String(e)
    });
  }
});

// ----------------------- START SERVER -----------------------
async function main() {
  console.log('=== SERVER STARTING ===');
  try {
    console.log('Calling initBuMap...');
    await initBuMap();
    console.log('initBuMap completed successfully');
  } catch (e) {
    console.error('initBuMap failed, continuing anyway:', e);
  }
  
  const PORT = process.env.PORT || 8080;
  app.listen(PORT, () => console.log(`=== SERVER LISTENING ON ${PORT} ===`));
}

main().catch(e => {
  console.error('=== FATAL STARTUP ERROR ===', e);
  process.exit(1);
});
