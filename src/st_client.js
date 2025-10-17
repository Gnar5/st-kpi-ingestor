import fetch from 'node-fetch';
import {SecretManagerServiceClient} from '@google-cloud/secret-manager';
const sm = new SecretManagerServiceClient();

// Simple sleep helper
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function resolveSecret(maybePath) {
  if (typeof maybePath !== 'string') return maybePath;
  if (!maybePath.startsWith('projects/')) return maybePath; // treat as literal if not a SM path
  const [v] = await sm.accessSecretVersion({ name: maybePath });
  return v.payload.data.toString('utf8');
}

// simple in-memory token cache
let cachedToken = null;
let tokenExpTs = 0;
let cachedBuMap = null;

function normalizeToken(tok) {
  // Accept {}, JSON string, or falsy; always return an object
  if (!tok) return {};
  if (typeof tok === 'string') {
    const s = tok.trim();
    if (!s) return {};
    try { return JSON.parse(s); } catch { return {}; }
  }
  if (typeof tok === 'object') return tok;
  return {};
}

function coalesce(val) {
  // Treat empty strings as undefined
  return (val === '' ? undefined : val);
}

export async function makeStClient(cfg) {
  // Build auth inputs
  const rawToken =
    coalesce(process.env.ST_OAUTH_TOKEN) ??
    cfg?.servicetitan?.oauth_token ??
    {};

  const token = normalizeToken(rawToken);
  const refresh = coalesce(process.env.ST_REFRESH_TOKEN) ?? cfg?.servicetitan?.refresh_token;
  if (refresh) token.refresh_token = refresh;

  const clientId =
    coalesce(process.env.ST_CLIENT_ID) ?? await resolveSecret(cfg.servicetitan.client_id_secret).catch(()=>null);
  const clientSecret =
    coalesce(process.env.ST_CLIENT_SECRET) ?? await resolveSecret(cfg.servicetitan.client_secret_secret).catch(()=>null);
  const tenantId =
    coalesce(process.env.ST_TENANT_ID) ?? await resolveSecret(cfg.servicetitan.tenant_id_secret).catch(() => cfg.servicetitan.tenant_id);
  const base =
    coalesce(process.env.ST_BASE_URL) ?? cfg?.servicetitan?.base_url ?? 'https://api.servicetitan.io';
  const appKey   = await resolveSecret(cfg.servicetitan.api_key_secret);

  // Basic validation with helpful errors
  const missing = [];
  if (!clientId)    missing.push('ST_CLIENT_ID / servicetitan.client_id');
  if (!clientSecret)missing.push('ST_CLIENT_SECRET / servicetitan.client_secret');
  if (!tenantId)    missing.push('ST_TENANT_ID / servicetitan.tenant_id');
  if (missing.length) {
    throw new Error('Missing required ServiceTitan auth config: ' + missing.join(', '));
  }

  async function getToken() {
    const now = Date.now()/1000;
    if (cachedToken && now < tokenExpTs - 60) return cachedToken; // reuse if not near expiry

    // OAuth client-credentials
    const tokenUrl = 'https://auth.servicetitan.io/connect/token';
    const body = new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: clientId ?? '',
      client_secret: clientSecret ?? '',
    });
    const res = await fetch(tokenUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body
    });
    if (res.ok) {
      const t = await res.json(); // {access_token, token_type, expires_in}
      cachedToken = t.access_token;
      tokenExpTs = Math.floor(Date.now()/1000) + (t.expires_in ?? 300);
      console.log('OAuth token acquired (no scope)');
      return cachedToken;
    }
    throw new Error('Failed to obtain OAuth token (check client_id/secret).');
  }

  async function authHeaders() {
    const token = await getToken();
    return {
      'Authorization': `Bearer ${token}`,
      'ST-App-Key': appKey
    };
  }

  // ---- NEW: list all Job Types (id, name, active) ----
  async function listJobTypes() {
    // match the same base/tenant path used by business-units below
    const url = `${base}/v2/tenant/${tenantId}/job-types`;
    const headers = await authHeaders();

    const resp = await fetch(url, { method: 'GET', headers });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`JobTypes GET ${resp.status}: ${text}`);
    }
    const json = await resp.json();
    const items = json?.data?.items || json?.items || json || [];
    return items.map(({ id, name, active }) => ({ id, name, active }));
  }
  // ---- END NEW HELPER ----

  async function fetchBusinessUnits() {
    if (cachedBuMap) return cachedBuMap;

    const url = `${base}/settings/v2/tenant/${tenantId}/business-units`;
    const headers = await authHeaders();
    const res = await fetch(url, { headers });

    if (!res.ok) {
      throw new Error(`Failed to fetch business units: ${res.status} ${await res.text()}`);
    }

    const bus = await res.json();
    cachedBuMap = {};
    for (const bu of bus.data) {
      cachedBuMap[bu.name] = bu.id;
    }
    const buCount = Array.isArray(bus?.data) ? bus.data.length : 0;
    console.log(`Fetched and cached ${buCount} business units.`);
    return cachedBuMap;
  }

  // Convert parameters object to array format expected by ServiceTitan
  function formatParameters(params) {
    return Object.entries(params).map(([name, value]) => ({
      name,
      value
    }));
  }

  // POST rows
  // POST rows (return shape matches index.js expectations)
async function fetchReport(categoryPath, reportId, parameters, page = 1, pageSize = 5000) {
  const url = `${base}/reporting/v2/tenant/${tenantId}/${categoryPath}/reports/${reportId}/data`;

  let hasMore = true;
  const allItems = [];
  let columns = null;

  while (hasMore) {
    const body = {
      request: { page, pageSize },
      parameters: formatParameters(parameters)
    };

    // Safe debug logging
    console.log('ST report call', JSON.stringify({
      reportId,
      categoryPath,
      page,
      paramNames: body.parameters.map(p => p.name)
    }));

    let lastError = null;
    let success = false;

    for (let i = 0; i < 5; i++) {
      const headers = await authHeaders();
      headers['Content-Type'] = 'application/json';
      const res = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body) });

      if (res.ok) {
        const pageData = await res.json();
        // ST usually returns { columns: [...], items: [...], hasMore: bool }
        if (!columns && pageData?.columns) columns = pageData.columns;
        const pageItems = pageData?.items || pageData?.data || [];
        if (Array.isArray(pageItems) && pageItems.length) {
          allItems.push(...pageItems);
        }
        hasMore = !!pageData?.hasMore;
        page++;
        success = true;
        break;
      }

      lastError = new Error(`ST ${reportId} page ${page} ${res.status}: ${await res.text()}`);

      if (res.status === 429) {
        const retryAfter = parseInt(res.headers.get('retry-after'), 10);
        const waitSecs = isNaN(retryAfter) ? 60 : retryAfter;
        console.warn(`ST report ${reportId} got 429. Retrying after ${waitSecs}s...`);
        await sleep(waitSecs * 1000);
        continue;
      }

      // For other errors, fail fast
      break;
    }

    if (!success) throw lastError;
  }

  // Return shape that index.js expects:
  return { data: { columns: columns || [], items: allItems }, hasMore: false };
}


  // GET report definition (optional)
  async function fetchReportInfo(categoryPath, reportId) {
    const url = `${base}/reporting/${cfg.servicetitan.api_version}/tenant/${tenantId}/${categoryPath}/reports/${reportId}`;
    const headers = await authHeaders();
    const res = await fetch(url, { headers });
    if (!res.ok) throw new Error(`ST meta ${reportId} ${res.status}: ${await res.text()}`);
    return res.json();
  }

  // Return the client with all helpers
  return { fetchReport, fetchReportInfo, fetchBusinessUnits, listJobTypes };
}