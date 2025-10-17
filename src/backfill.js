import fs from 'fs';
import { cfg, initBuMap, ingest, buNameIdMap } from './common.js';

// ---- simple arg parsing ----
const args = process.argv.slice(2);
const days = Number(args.find(a => /^\d+$/.test(a))) || (cfg?.servicetitan?.date_windows?.backfill_days ?? 730);
const windowDays = Number((args.find(a => a.startsWith('--window=')) || '').split('=')[1]) || 7;
const onlyArg = args.find(a => a.startsWith('--only='));
const only = onlyArg ? onlyArg.split('=').pop().split(',').filter(Boolean) : null;
const dryRun = args.includes('--dry-run');

// Force Daily WBR to 1-day windows (heavy report)
const perReportWindow = { daily_wbr: 1 };
// Optional: only run for one BU via --bu="BU Name"
const buOnlyArgArg = args.find(a => a.startsWith('--bu='));
const buOnlyArg = buOnlyArgArg ? buOnlyArgArg.slice(5) : null;

// ---- checkpoint (per report + BU + window) ----
const STATE_FILE = '.backfill-state.json';
function loadState(){ try { return JSON.parse(fs.readFileSync(STATE_FILE,'utf8')); } catch { return {}; } }
function saveState(s){ fs.writeFileSync(STATE_FILE, JSON.stringify(s, null, 2)); }

function* dateWindows(totalDays, stepDays) {
  const end = new Date(new Date().toISOString().slice(0,10));
  const start = new Date(end); start.setDate(start.getDate() - totalDays);
  let cursor = new Date(start);
  while (cursor < end) {
    const from = new Date(cursor);
    const to = new Date(cursor); to.setDate(to.getDate() + stepDays);
    if (to > end) to.setTime(end.getTime());
    yield { from: from.toISOString().slice(0,10), to: to.toISOString().slice(0,10) };
    cursor = to;
  }
}

// ---- reports: mapRow uses ctx.bu_key instead of 'default' ----
const reports = [
  {
    key: 'leads',
    name: 'leads',
    categoryPath: 'report-category/marketing',
    reportId: cfg.servicetitan.report_ids.leads,
    destTable: cfg.bigquery.raw_tables.leads || 'raw_leads',
    mapRow: (r, ctx) => {
      // ServiceTitan returns array data - creation date is at position 8
      const creationDateStr = Array.isArray(r) ? r[8] : (r.CreatedDate || r.CreatedOn || r.JobCreationDate);
      const creationDate = creationDateStr ? new Date(creationDateStr) : null;

      return {
        bu_key: ctx?.bu_key ?? 'unknown',
        job_created_on: creationDate || new Date(ctx?.from ?? Date.now()),
        created_date: creationDate,
        customer_name: Array.isArray(r) ? r[0] : (r.CustomerName || r.LocationName || null),
        job_type: Array.isArray(r) ? r[3] : (r.JobType || null),
        job_id: String(Array.isArray(r) ? r[5] : (r.JobId ?? r.TicketId ?? r.JobNumber ?? '')),
        updated_on: new Date(),
        raw: JSON.stringify(r)
      };
    },
  },
  {
    key: 'daily_wbr',
    name: 'daily_wbr',
    categoryPath: 'report-category/technician',
    reportId: cfg.servicetitan.report_ids.daily_wbr_cr,
    destTable: 'raw_daily_wbr_v2',
    mapRow: (r, ctx) => ({
      bu_key: ctx?.bu_key ?? 'unknown',
      event_time: r.Date ? new Date(r.Date) : (r.EventTime ? new Date(r.EventTime) : new Date()),
      job_type: r.JobType || null,
      total_sales: Number(r.TotalSales ?? r.Total ?? 0),
      completed_job: Number(r.CompletedJob ?? r.Completed ?? 0),
      close_rate: Number(r.CloseRate ?? r.SuccessRate ?? 0),
      job_id: String(r.JobId ?? ''),
      updated_on: new Date(),
      raw: JSON.stringify(r)
    }),
  },
  {
    key: 'foreman',
    name: 'foreman',
    categoryPath: 'report-category/operations',
    reportId: cfg.servicetitan.report_ids.foreman_job_cost_this_week,
    destTable: cfg.bigquery.raw_tables.foreman || 'raw_foreman',
    mapRow: (r, ctx) => {
      // New report column positions:
      // 0: Scheduled Date, 1: Business Unit, 4: Job Type, 6: Job #,
      // 7: Jobs Subtotal, 12: Jobs Total Cost, 13: Jobs Gross Margin %
      const toNumber = (v) => {
        if (v === null || v === undefined) return null;
        if (typeof v === 'number') return Number.isFinite(v) ? v : null;
        if (typeof v === 'string') {
          let str = v.trim();
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
      };

      return {
        bu_key: ctx?.bu_key ?? 'unknown',
        job_id: String(Array.isArray(r) ? r[6] : (r['Job #'] || r.JobId || '')),
        job_start: Array.isArray(r) ? new Date(r[0]) : (r['Scheduled Date'] ? new Date(r['Scheduled Date']) : new Date()),
        job_type: Array.isArray(r) ? r[4] : (r['Job Type'] || null),
        job_subtotal: toNumber(Array.isArray(r) ? r[7] : (r['Jobs Subtotal'] ?? r.JobSubtotal ?? 0)),
        job_total_costs: toNumber(Array.isArray(r) ? r[12] : (r['Jobs Total Cost'] ?? r.JobTotalCosts ?? 0)),
        job_gm_pct: toNumber(Array.isArray(r) ? r[13] : (r['Jobs Gross Margin %'] ?? r.JobGrossMarginPct ?? 0)),
        updated_on: new Date(),
        raw: JSON.stringify(r)
      };
    },
  },
  {
    key: 'collections',
    name: 'collections',
    categoryPath: 'report-category/accounting',
    reportId: cfg.servicetitan.report_ids.collections,
    destTable: cfg.bigquery.raw_tables.collections || 'raw_collections',
    mapRow: (r, ctx) => {
      const toNumber = (v) => {
        if (v === null || v === undefined) return null;
        if (typeof v === 'number') return Number.isFinite(v) ? v : null;
        if (typeof v === 'string') {
          let str = v.trim();
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
      };

      const paidOnStr = Array.isArray(r) ? r[0] : (r['Paid On'] || r.PaymentDate || r.Date);
      const amountRaw = Array.isArray(r) ? r[2] : (r.Amount || r['Payment Amount']);
      const invoiceNumber = Array.isArray(r) ? r[3] : (r['Invoice Number'] || r.JobId);
      const buRaw = Array.isArray(r) ? r[7] : (r['Invoice Business Unit'] || r['Job Business Unit'] || r['Business Unit']);

      return {
        bu_key: typeof buRaw === 'string' ? buRaw.trim() : (ctx?.bu_key ?? 'unknown'),
        payment_date: paidOnStr ? new Date(paidOnStr) : new Date(),
        amount: toNumber(amountRaw) ?? 0,
        job_id: String(invoiceNumber ?? ''),
        updated_on: new Date(),
        raw: JSON.stringify(r)
      };
    },
  },
  // AR optional: include when you’re ready
  // {
  //   key: 'ar',
  //   name: 'ar',
  //   categoryPath: 'report-category/accounting',
  //   reportId: cfg.servicetitan.report_ids.ar_report,
  //   destTable: cfg.bigquery.raw_tables.ar || 'raw_ar',
  //   mapRow: (r, ctx) => ({
  //     bu_key: ctx?.bu_key ?? 'unknown',
  //     as_of: new Date(ctx.to),
  //     location_name: r.LocationName || null,
  //     net_amount: Number(r.NetAmount ?? 0),
  //     updated_on: new Date(),
  //     raw: JSON.stringify(r)
  //   }),
  // },
];

function getDiscoveredBuNames() {
  return Object.keys(buNameIdMap || {});
}

// Prefer explicit config if present; otherwise fall back to name heuristics.
function getSalesBuNames() {
  const names = new Set();
  const cfgMap = cfg.bu_mapping || [];
  for (const m of cfgMap) for (const n of (m.sales_bu_names || [])) names.add(n);
  if (names.size === 0) {
    for (const n of getDiscoveredBuNames()) if (/-Sales$/i.test(n)) names.add(n);
  }
  return [...names].filter(n => buNameIdMap[n]);
}
function getProductionBuNames() {
  const names = new Set();
  const cfgMap = cfg.bu_mapping || [];
  for (const m of cfgMap) for (const n of (m.production_bu_names || [])) names.add(n);
  if (names.size === 0) {
    for (const n of getDiscoveredBuNames()) if (/-Production$/i.test(n)) names.add(n);
  }
  return [...names].filter(n => buNameIdMap[n]);
}

async function main() {
  console.log(`Per-BU backfill for last ${days} days in ${windowDays}d windows${dryRun ? ' (dry-run)' : ''}.`);
  await initBuMap(); // populates buNameIdMap
  const state = loadState();
  const selected = only ? reports.filter(r => only.includes(r.key) || only.includes(r.name)) : reports;
  // Assign BU lists per report
  const buListByReport = {
    leads: getSalesBuNames(),
    daily_wbr: getSalesBuNames(),
    foreman: getProductionBuNames(),
    collections: getDiscoveredBuNames(), // Collections uses ALL BUs (company-wide)
  };
  // Sanity if nothing discovered
  const anyBu = Object.values(buListByReport).some(arr => (arr || []).length > 0);
  if (!anyBu) {
    console.error('No BU names discovered from ServiceTitan. Check ST auth or permissions.');
    process.exit(2);
  }

  for (const rep of selected) {
    const buNames = buListByReport[rep.key] || [];
    if (buNames.length === 0) {
      console.warn(`No BU names for report ${rep.key}; skipping.`);
      continue;
    }
    // If --bu= provided, restrict to that BU
    const buList = buOnlyArg ? buNames.filter(n => n === buOnlyArg) : buNames;
    state[rep.key] ??= {};
    const w = perReportWindow[rep.key] || windowDays;
    for (const win of dateWindows(days, w)) {
      for (const buName of buList) {
        state[rep.key][buName] ??= [];
        const key = `${win.from}:${win.to}`;
        if (state[rep.key][buName].includes(key)) {
          console.log(`[skip] ${rep.key} ${buName} ${key}`);
          continue;
        }
        const buId = buNameIdMap[buName];
        // Choose reportId: BU-specific for daily_wbr if configured; otherwise fallback
        let reportId = rep.reportId;
        if (rep.key === 'daily_wbr') {
          const byBu = cfg?.servicetitan?.daily_wbr_report_ids_by_bu;
          if (byBu && byBu[buName]) reportId = byBu[buName];
        }
        // DateType varies by report:
        // Leads: 2 = Modified Date, Collections: 2 = Paid On, Foreman: 3 = Scheduled Start Date
        const dateTypeByReport = {
          leads: 2,          // Modified Date
          daily_wbr: 3,      // Job Completed Date
          foreman: 3,        // Scheduled Start Date
          collections: 2,    // Paid On
        };
        const dateType = dateTypeByReport[rep.key] || 2;
        const params = { From: win.from, To: win.to, DateType: dateType, BusinessUnitIds: [buId] };
        const ctx = { bu_key: buName, to: win.to };
        console.log(`[run]  ${rep.key} ${buName} ${key} (BU ID ${buId}, report ${reportId})`);
        if (!dryRun) {
          await ingest(
            null,
            rep.categoryPath,
            reportId,
            params,
            (row) => rep.mapRow(row, ctx),
            rep.destTable
          );
        }
        state[rep.key][buName].push(key);
        saveState(state);
      }
    }
  }
  console.log('Per-BU backfill complete.');
}

main().catch(e => { console.error(e); process.exit(1); });
