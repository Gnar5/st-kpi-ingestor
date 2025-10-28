/**
 * ServiceTitan API Client
 * Handles authentication, pagination, rate limiting, and error handling
 * for all ServiceTitan entity API endpoints
 */

import axios from 'axios';
import { logger } from '../utils/logger.js';
import { retryWithBackoff, RateLimiter, CircuitBreaker } from '../utils/backoff.js';

export class ServiceTitanClient {
  constructor(config = {}) {
    this.clientId = config.clientId || process.env.ST_CLIENT_ID;
    this.clientSecret = config.clientSecret || process.env.ST_CLIENT_SECRET;
    this.tenantId = config.tenantId || process.env.ST_TENANT_ID;
    this.appKey = config.appKey || process.env.ST_APP_KEY;

    this.baseUrl = 'https://api.servicetitan.io';
    this.authUrl = 'https://auth.servicetitan.io/connect/token';

    this.accessToken = null;
    this.tokenExpiry = null;

    // Rate limiting: ServiceTitan allows ~10 req/sec per tenant
    this.rateLimiter = new RateLimiter(
      parseInt(process.env.RATE_LIMIT_PER_SECOND) || 10,
      20 // bucket size
    );

    // Circuit breaker for fault tolerance
    this.circuitBreaker = new CircuitBreaker({
      failureThreshold: 5,
      resetTimeout: 60000
    });

    this.log = logger.child('st-client');
  }

  /**
   * Authenticate with ServiceTitan OAuth2
   */
  async authenticate() {
    if (this.accessToken && this.tokenExpiry && Date.now() < this.tokenExpiry) {
      return this.accessToken;
    }

    this.log.info('Authenticating with ServiceTitan');

    try {
      const response = await axios.post(
        this.authUrl,
        new URLSearchParams({
          grant_type: 'client_credentials',
          client_id: this.clientId,
          client_secret: this.clientSecret
        }),
        {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
        }
      );

      this.accessToken = response.data.access_token;
      // Set expiry 5 minutes before actual expiry for safety
      this.tokenExpiry = Date.now() + (response.data.expires_in - 300) * 1000;

      this.log.info('Authentication successful', {
        expiresIn: response.data.expires_in
      });

      return this.accessToken;
    } catch (error) {
      this.log.error('Authentication failed', {
        error: error.message,
        status: error.response?.status
      });
      throw new Error(`ServiceTitan authentication failed: ${error.message}`);
    }
  }

  /**
   * Build request headers with auth token
   */
  async getHeaders() {
    const token = await this.authenticate();

    return {
      'Authorization': `Bearer ${token}`,
      'ST-App-Key': this.appKey,
      'Content-Type': 'application/json',
      'Accept-Encoding': 'gzip, deflate'  // Enable compression for faster transfers
    };
  }

  /**
   * Make paginated API request
   */
  async request(endpoint, params = {}, options = {}) {
    await this.rateLimiter.acquire();

    const headers = await this.getHeaders();
    // Replace {tenant} placeholder in endpoint with actual tenant ID
    const resolvedEndpoint = endpoint.replace('{tenant}', this.tenantId);
    const url = `${this.baseUrl}/${resolvedEndpoint}`;

    const requestConfig = {
      method: options.method || 'GET',
      url,
      headers,
      params: {
        ...params
      },
      timeout: options.timeout || 30000
    };

    if (options.data) {
      requestConfig.data = options.data;
    }

    try {
      const response = await this.circuitBreaker.execute(
        () => retryWithBackoff(
          () => axios(requestConfig),
          {
            context: `ST API ${endpoint}`,
            maxRetries: options.maxRetries || 3,
            baseDelay: 2000
          }
        ),
        endpoint
      );

      return response.data;
    } catch (error) {
      this.log.error('API request failed', {
        endpoint,
        params,
        error: error.message,
        status: error.response?.status,
        statusText: error.response?.statusText
      });
      throw error;
    }
  }

  /**
   * Fetch all pages for a paginated endpoint
   */
  async *fetchAllPages(endpoint, params = {}, options = {}) {
    const pageSize = options.pageSize || 500;
    let page = 1;
    let hasMore = true;
    let totalFetched = 0;

    this.log.info('Starting paginated fetch', { endpoint, params });

    while (hasMore) {
      const pageParams = {
        ...params,
        page,
        pageSize
      };

      try {
        const response = await this.request(endpoint, pageParams, options);

        // ServiceTitan pagination format: { data: [], hasMore: bool, page: int, totalCount: int }
        const items = response.data || [];
        hasMore = response.hasMore || false;
        totalFetched += items.length;

        this.log.debug('Page fetched', {
          endpoint,
          page,
          itemsInPage: items.length,
          totalFetched,
          hasMore
        });

        yield items;

        if (items.length === 0) {
          hasMore = false;
        }

        page++;

        // Safety limit
        if (page > 10000) {
          this.log.warn('Pagination safety limit reached', { endpoint, page });
          break;
        }
      } catch (error) {
        this.log.error('Pagination failed', {
          endpoint,
          page,
          error: error.message
        });
        throw error;
      }
    }

    this.log.info('Pagination complete', {
      endpoint,
      totalPages: page - 1,
      totalItems: totalFetched
    });
  }

  /**
   * Fetch all items from a paginated endpoint (convenience method)
   */
  async fetchAll(endpoint, params = {}, options = {}) {
    const allItems = [];

    for await (const pageItems of this.fetchAllPages(endpoint, params, options)) {
      allItems.push(...pageItems);
    }

    return allItems;
  }

  /**
   * Fetch with incremental sync using modifiedOn/createdOn
   */
  async fetchIncremental(endpoint, params = {}, options = {}) {
    const modifiedSince = options.modifiedSince;

    if (!modifiedSince) {
      this.log.warn('No modifiedSince provided, fetching all data', { endpoint });
      return this.fetchAll(endpoint, params, options);
    }

    this.log.info('Incremental fetch starting', {
      endpoint,
      modifiedSince
    });

    // Build filter for modified/created dates
    const incrementalParams = {
      ...params,
      modifiedOnOrAfter: modifiedSince,
      // Some endpoints use different parameter names
      modifiedAfter: modifiedSince,
      createdOnOrAfter: modifiedSince
    };

    const items = await this.fetchAll(endpoint, incrementalParams, options);

    this.log.info('Incremental fetch complete', {
      endpoint,
      itemsFetched: items.length,
      modifiedSince
    });

    return items;
  }

  // ==========================================
  // ENTITY-SPECIFIC METHODS
  // ==========================================

  /**
   * Jobs API
   */
  async getJobs(params = {}) {
    return this.fetchAll('jpm/v2/tenant/{tenant}/jobs', params);
  }

  async getJobsIncremental(modifiedSince) {
    return this.fetchIncremental(
      'jpm/v2/tenant/{tenant}/jobs',
      {},
      { modifiedSince }
    );
  }

  /**
   * Invoices API
   */
  async getInvoices(params = {}) {
    return this.fetchAll('accounting/v2/tenant/{tenant}/invoices', params);
  }

  async getInvoicesIncremental(modifiedSince) {
    return this.fetchIncremental(
      'accounting/v2/tenant/{tenant}/invoices',
      {},
      { modifiedSince }
    );
  }

  /**
   * Estimates API
   */
  async getEstimates(params = {}) {
    return this.fetchAll('sales/v2/tenant/{tenant}/estimates', params);
  }

  async getEstimatesIncremental(modifiedSince) {
    return this.fetchIncremental(
      'sales/v2/tenant/{tenant}/estimates',
      {},
      { modifiedSince }
    );
  }

  /**
   * Payments API
   */
  async getPayments(params = {}) {
    return this.fetchAll('accounting/v2/tenant/{tenant}/payments', params);
  }

  async getPaymentsIncremental(modifiedSince) {
    return this.fetchIncremental(
      'accounting/v2/tenant/{tenant}/payments',
      {},
      { modifiedSince }
    );
  }

  /**
   * Payroll API (Gross Pay Items)
   */
  async getPayroll(params = {}) {
    return this.fetchAll('payroll/v2/tenant/{tenant}/gross-pay-items', params);
  }

  async getPayrollIncremental(modifiedSince) {
    return this.fetchIncremental(
      'payroll/v2/tenant/{tenant}/gross-pay-items',
      {},
      { modifiedSince }
    );
  }

  /**
   * Payroll Adjustments API
   * Returns payroll adjustments, bonuses, and corrections
   * These link to invoiceId rather than jobId
   */
  async getPayrollAdjustments(params = {}) {
    return this.fetchAll('payroll/v2/tenant/{tenant}/payroll-adjustments', params);
  }

  async getPayrollAdjustmentsIncremental(modifiedSince) {
    return this.fetchIncremental(
      'payroll/v2/tenant/{tenant}/payroll-adjustments',
      {},
      { modifiedSince }
    );
  }

  /**
   * Customers API
   */
  async getCustomers(params = {}) {
    return this.fetchAll('crm/v2/tenant/{tenant}/customers', params);
  }

  async getCustomersIncremental(modifiedSince) {
    return this.fetchIncremental(
      'crm/v2/tenant/{tenant}/customers',
      {},
      { modifiedSince }
    );
  }

  /**
   * Locations API
   */
  async getLocations(params = {}) {
    return this.fetchAll('crm/v2/tenant/{tenant}/locations', params);
  }

  async getLocationsIncremental(modifiedSince) {
    return this.fetchIncremental(
      'crm/v2/tenant/{tenant}/locations',
      {},
      { modifiedSince }
    );
  }

  /**
   * Campaigns API
   */
  async getCampaigns(params = {}) {
    return this.fetchAll('marketing/v2/tenant/{tenant}/campaigns', params);
  }

  async getCampaignsIncremental(modifiedSince) {
    return this.fetchIncremental(
      'marketing/v2/tenant/{tenant}/campaigns',
      {},
      { modifiedSince }
    );
  }

  /**
   * Job Types API
   */
  async getJobTypes(params = {}) {
    return this.fetchAll('settings/v2/tenant/{tenant}/job-types', params);
  }

  /**
   * Business Units API
   */
  async getBusinessUnits(params = {}) {
    return this.fetchAll('settings/v2/tenant/{tenant}/business-units', params);
  }

  /**
   * Technicians API
   */
  async getTechnicians(params = {}) {
    return this.fetchAll('settings/v2/tenant/{tenant}/technicians', params);
  }

  /**
   * Employees API
   */
  async getEmployees(params = {}) {
    return this.fetchAll('settings/v2/tenant/{tenant}/employees', params);
  }

  /**
   * Projects API
   */
  async getProjects(params = {}) {
    return this.fetchAll('jpm/v2/tenant/{tenant}/projects', params);
  }

  async getProjectsIncremental(modifiedSince) {
    return this.fetchIncremental(
      'jpm/v2/tenant/{tenant}/projects',
      {},
      { modifiedSince }
    );
  }

  /**
   * Purchase Orders API
   */
  async getPurchaseOrders(params = {}) {
    return this.fetchAll('inventory/v2/tenant/{tenant}/purchase-orders', params);
  }

  async getPurchaseOrdersIncremental(modifiedSince) {
    return this.fetchIncremental(
      'inventory/v2/tenant/{tenant}/purchase-orders',
      {},
      { modifiedSince }
    );
  }

  /**
   * Returns API
   */
  async getReturns(params = {}) {
    return this.fetchAll('inventory/v2/tenant/{tenant}/returns', params);
  }

  async getReturnsIncremental(modifiedSince) {
    return this.fetchIncremental(
      'inventory/v2/tenant/{tenant}/returns',
      {},
      { modifiedSince }
    );
  }

  /**
   * Appointments API
   */
  async getAppointments(params = {}) {
    return this.fetchAll('jpm/v2/tenant/{tenant}/appointments', params);
  }

  async getAppointmentsIncremental(modifiedSince) {
    return this.fetchIncremental(
      'jpm/v2/tenant/{tenant}/appointments',
      {},
      { modifiedSince }
    );
  }

  /**
   * Calls API (Booking/Lead source)
   */
  async getCalls(params = {}) {
    return this.fetchAll('telecom/v2/tenant/{tenant}/calls', params);
  }

  async getCallsIncremental(modifiedSince) {
    return this.fetchIncremental(
      'telecom/v2/tenant/{tenant}/calls',
      {},
      { modifiedSince }
    );
  }

  /**
   * =======================================================================
   * REPORTING API
   * ServiceTitan Reporting API for fetching report data
   * =======================================================================
   */

  /**
   * Fetch report data using the Reporting API
   * @param {string} categoryPath - Report category (e.g., 'report-category/accounting')
   * @param {string} reportId - Report ID
   * @param {object} parameters - Report parameters (e.g., { From: '2025-08-18', To: '2025-08-24', DateType: 2 })
   * @param {object} options - Optional settings (pageSize, etc.)
   * @returns {Promise<{columns: Array, items: Array}>} Report data
   */
  async fetchReport(categoryPath, reportId, parameters = {}, options = {}) {
    const url = `${this.baseUrl}/reporting/v2/tenant/${this.tenantId}/${categoryPath}/reports/${reportId}/data`;
    const pageSize = options.pageSize || 5000;

    let page = 1;
    let hasMore = true;
    const allItems = [];
    let columns = null;

    this.log.info('Starting report fetch', { reportId, categoryPath, parameters });

    while (hasMore) {
      const body = {
        request: { page, pageSize },
        parameters: this.formatReportParameters(parameters)
      };

      try {
        await this.rateLimiter.acquire();

        const headers = await this.getHeaders();
        headers['Content-Type'] = 'application/json';

        const response = await this.circuitBreaker.execute(
          () => retryWithBackoff(
            () => axios({
              method: 'POST',
              url,
              headers,
              data: body,
              timeout: options.timeout || 60000
            }),
            {
              context: `ST Report ${reportId} page ${page}`,
              maxRetries: 3,
              baseDelay: 2000
            }
          ),
          `report-${reportId}`
        );

        const pageData = response.data;

        // Capture columns from first page
        if (!columns && pageData?.columns) {
          columns = pageData.columns;
        }

        // Get items from response
        const pageItems = pageData?.items || pageData?.data || [];
        if (Array.isArray(pageItems) && pageItems.length) {
          allItems.push(...pageItems);
        }

        hasMore = !!pageData?.hasMore;

        this.log.debug('Report page fetched', {
          reportId,
          page,
          itemsInPage: pageItems.length,
          totalFetched: allItems.length,
          hasMore
        });

        page++;

        // Safety limit
        if (page > 1000) {
          this.log.warn('Report pagination safety limit reached', { reportId, page });
          break;
        }
      } catch (error) {
        this.log.error('Report fetch failed', {
          reportId,
          categoryPath,
          page,
          error: error.message
        });
        throw error;
      }
    }

    this.log.info('Report fetch complete', {
      reportId,
      totalPages: page - 1,
      totalItems: allItems.length
    });

    return {
      columns: columns || [],
      items: allItems
    };
  }

  /**
   * Format report parameters for ServiceTitan Reporting API
   * Converts { From: '2025-08-18', To: '2025-08-24' } to [{ name: 'From', value: '2025-08-18' }, ...]
   */
  formatReportParameters(params) {
    return Object.entries(params).map(([name, value]) => ({
      name,
      value
    }));
  }

  /**
   * =======================================================================
   * REFERENCE / DIMENSION APIs
   * These endpoints return metadata/lookup tables for ID resolution
   * =======================================================================
   */

  /**
   * Activity Codes API (for payroll activity lookups)
   * Returns activity codes used in payroll/timesheets
   */
  async getActivityCodes(params = {}) {
    return this.fetchAll('settings/v2/tenant/{tenant}/activity-codes', params);
  }

  /**
   * Job Types API (for job classification)
   */
  async getJobTypes(params = {}) {
    return this.fetchAll('settings/v2/tenant/{tenant}/job-types', params);
  }

  /**
   * Campaign Categories API (for marketing campaign classification)
   */
  async getCampaignCategories(params = {}) {
    return this.fetchAll('marketing/v2/tenant/{tenant}/categories', params);
  }

  /**
   * Zones API (for geographic/service area lookups)
   */
  async getZones(params = {}) {
    return this.fetchAll('settings/v2/tenant/{tenant}/zones', params);
  }

  /**
   * Tag Types API (for custom tagging)
   */
  async getTagTypes(params = {}) {
    return this.fetchAll('settings/v2/tenant/{tenant}/tag-types', params);
  }
}

export default ServiceTitanClient;
