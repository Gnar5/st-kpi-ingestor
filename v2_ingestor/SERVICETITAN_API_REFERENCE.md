# ServiceTitan Entity API Reference

Complete mapping of ServiceTitan v2 API endpoints used by this ingestor.

**Base URL**: `https://api.servicetitan.io`
**Auth URL**: `https://auth.servicetitan.io/connect/token`
**API Version**: v2
**Documentation**: https://developer.servicetitan.io

---

## Authentication

### OAuth 2.0 Client Credentials Flow

**Endpoint**: `POST https://auth.servicetitan.io/connect/token`

**Request**:
```
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials
&client_id={YOUR_CLIENT_ID}
&client_secret={YOUR_CLIENT_SECRET}
```

**Response**:
```json
{
  "access_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6IjEyMzQ1...",
  "expires_in": 3600,
  "token_type": "Bearer"
}
```

**Headers for API Calls**:
```
Authorization: Bearer {access_token}
ST-App-Key: {YOUR_APP_KEY}
Content-Type: application/json
```

---

## Core Entity Endpoints

### 1. Jobs (JPM - Job & Project Management)

**Endpoint**: `GET /jpm/v2/tenant/{tenant}/jobs`

**Purpose**: Job lifecycle data - from booking to completion

**Query Parameters**:
- `tenant` (required): Tenant ID
- `page` (default: 1): Page number
- `pageSize` (default: 500, max: 500): Items per page
- `modifiedOnOrAfter`: Filter by modification date (ISO 8601)
- `createdOnOrAfter`: Filter by creation date
- `active`: Filter active/inactive jobs
- `ids`: Comma-separated job IDs

**Response Format**:
```json
{
  "data": [
    {
      "id": 123456789,
      "jobNumber": "JOB-2025-0001",
      "projectId": 987654321,
      "customerId": 111222333,
      "locationId": 444555666,
      "jobStatus": "Completed",
      "completedOn": "2025-10-21T15:30:00Z",
      "businessUnitId": 1,
      "jobTypeId": 10,
      "priority": "High",
      "campaignId": 50,
      "summary": "Annual HVAC maintenance",
      "customFields": [
        { "name": "LeadSource", "value": "Referral" }
      ],
      "createdOn": "2025-10-15T08:00:00Z",
      "createdById": 777,
      "modifiedOn": "2025-10-21T15:30:00Z",
      "tagTypeIds": [1, 5, 12],
      "leadCallId": 999888,
      "bookingId": 111000,
      "soldById": 555
    }
  ],
  "page": 1,
  "pageSize": 500,
  "hasMore": true,
  "totalCount": 12847
}
```

**Field Descriptions**:
- `id`: Unique job identifier
- `jobNumber`: Human-readable job number
- `jobStatus`: "Scheduled", "InProgress", "Completed", "Canceled"
- `businessUnitId`: Links to business unit (BU1-BU6 for you)
- `campaignId`: Marketing campaign attribution

**Incremental Sync**: Use `modifiedOnOrAfter` parameter

---

### 2. Invoices (Accounting)

**Endpoint**: `GET /accounting/v2/tenant/{tenant}/invoices`

**Purpose**: Financial transactions, billing, revenue

**Query Parameters**:
- `tenant` (required): Tenant ID
- `page`, `pageSize`: Pagination
- `modifiedOnOrAfter`: Incremental sync filter
- `invoiceDateFrom`, `invoiceDateTo`: Filter by invoice date
- `jobIds`: Filter by job IDs
- `businessUnitIds`: Filter by business units
- `statuses`: Filter by status (e.g., "Posted,Exported")

**Response Format**:
```json
{
  "data": [
    {
      "id": 987654321,
      "syncStatus": "Exported",
      "summary": "HVAC Service Invoice",
      "referenceNumber": "INV-2025-1001",
      "invoiceDate": "2025-10-21T00:00:00Z",
      "dueDate": "2025-11-05T00:00:00Z",
      "subTotal": 1250.00,
      "salesTax": 106.25,
      "total": 1356.25,
      "balance": 0.00,
      "invoiceTypeId": 1,
      "jobId": 123456789,
      "projectId": 987654321,
      "businessUnitId": 1,
      "locationId": 444555666,
      "customerId": 111222333,
      "depositedOn": "2025-10-22T10:00:00Z",
      "createdOn": "2025-10-21T16:00:00Z",
      "modifiedOn": "2025-10-22T10:00:00Z",
      "adjustmentToId": null,
      "status": "Posted",
      "employeeId": 555,
      "commissionEligibilityDate": "2025-10-21T00:00:00Z",
      "items": [
        {
          "id": 11111,
          "description": "Labor - 2 hours",
          "quantity": 2,
          "unitPrice": 125.00,
          "total": 250.00,
          "itemType": "Service"
        },
        {
          "id": 22222,
          "description": "Air filter replacement",
          "quantity": 1,
          "unitPrice": 1000.00,
          "total": 1000.00,
          "itemType": "Material"
        }
      ],
      "customFields": []
    }
  ],
  "page": 1,
  "pageSize": 500,
  "hasMore": false,
  "totalCount": 3821
}
```

**Field Descriptions**:
- `syncStatus`: QuickBooks/accounting sync status
- `balance`: Outstanding amount (total - payments)
- `items`: Line-item details (stored as JSON in BQ)
- `status`: "Draft", "Posted", "Exported", "Void"

**Incremental Sync**: Use `modifiedOnOrAfter`

---

### 3. Estimates (Sales)

**Endpoint**: `GET /sales/v2/tenant/{tenant}/estimates`

**Purpose**: Sales estimates, quotes, proposals

**Query Parameters**:
- `tenant`, `page`, `pageSize`: Standard
- `modifiedOnOrAfter`: Incremental filter
- `soldOnFrom`, `soldOnTo`: Filter by sold date
- `jobIds`: Filter by jobs
- `statuses`: "Draft", "Presented", "Sold", "Dismissed"

**Response Format**:
```json
{
  "data": [
    {
      "id": 555444333,
      "jobId": 123456789,
      "projectId": 987654321,
      "locationId": 444555666,
      "customerId": 111222333,
      "name": "HVAC System Upgrade - Option A",
      "jobNumber": "JOB-2025-0001",
      "status": "Sold",
      "summary": "High-efficiency system with 10-year warranty",
      "createdOn": "2025-10-15T10:00:00Z",
      "modifiedOn": "2025-10-20T14:30:00Z",
      "soldOn": "2025-10-20T14:30:00Z",
      "soldById": 555,
      "estimateNumber": "EST-2025-0042",
      "businessUnitId": 1,
      "items": [
        {
          "id": 33333,
          "description": "Carrier 16 SEER Heat Pump",
          "quantity": 1,
          "price": 8500.00,
          "total": 8500.00
        },
        {
          "id": 44444,
          "description": "Installation labor",
          "quantity": 1,
          "price": 2000.00,
          "total": 2000.00
        }
      ],
      "subtotal": 10500.00,
      "totalTax": 892.50,
      "total": 11392.50
    }
  ],
  "page": 1,
  "pageSize": 500,
  "hasMore": false
}
```

**Field Descriptions**:
- `status`: "Draft", "Presented", "Sold", "Dismissed"
- `soldOn`: When estimate was accepted (null if not sold)
- `soldById`: Employee who sold the estimate

**Incremental Sync**: Use `modifiedOnOrAfter`

---

### 4. Payments (Accounting)

**Endpoint**: `GET /accounting/v2/tenant/{tenant}/payments`

**Purpose**: Payment transactions, cash flow

**Query Parameters**:
- `tenant`, `page`, `pageSize`: Standard
- `modifiedOnOrAfter`: Incremental filter
- `createdOnFrom`, `createdOnTo`: Filter by payment date
- `invoiceIds`: Filter by invoices
- `paymentTypeIds`: Filter by payment method

**Response Format**:
```json
{
  "data": [
    {
      "id": 777888999,
      "invoiceId": 987654321,
      "amount": 1356.25,
      "paymentTypeId": 1,
      "status": "Applied",
      "memo": "Credit card payment - Visa ending 4242",
      "referenceNumber": "CH-20251022-001",
      "unappliedAmount": 0.00,
      "createdOn": "2025-10-22T10:00:00Z",
      "modifiedOn": "2025-10-22T10:00:00Z",
      "businessUnitId": 1,
      "batchId": 12345
    }
  ],
  "page": 1,
  "pageSize": 500,
  "hasMore": false
}
```

**Field Descriptions**:
- `paymentTypeId`: Payment method (1=Credit Card, 2=Check, 3=Cash, etc.)
- `unappliedAmount`: Amount not yet applied to invoices
- `batchId`: Batch deposit ID for reconciliation

**Incremental Sync**: Use `modifiedOnOrAfter`

---

### 5. Payroll (Payroll)

**Endpoint**: `GET /payroll/v2/tenant/{tenant}/gross-pay-items`

**Purpose**: Technician/employee compensation

**Query Parameters**:
- `tenant`, `page`, `pageSize`: Standard
- `modifiedOnOrAfter`: Incremental filter
- `paidDateFrom`, `paidDateTo`: Filter by pay period
- `employeeIds`: Filter by employees

**Response Format**:
```json
{
  "data": [
    {
      "id": 333222111,
      "employeeId": 555,
      "jobId": 123456789,
      "invoiceId": 987654321,
      "rate": 35.00,
      "hours": 4.5,
      "amount": 157.50,
      "paidDate": "2025-10-21T00:00:00Z",
      "description": "Job labor - HVAC maintenance",
      "payrollTypeId": 1,
      "createdOn": "2025-10-21T17:00:00Z",
      "modifiedOn": "2025-10-21T17:00:00Z",
      "businessUnitId": 1
    }
  ],
  "page": 1,
  "pageSize": 500,
  "hasMore": false
}
```

**Field Descriptions**:
- `payrollTypeId`: Type of pay (1=Regular, 2=Overtime, 3=Commission, etc.)
- `hours`: Hours worked (if applicable)
- `rate`: Hourly rate (if applicable)
- `amount`: Total compensation

**Incremental Sync**: Use `modifiedOnOrAfter`

---

### 6. Customers (CRM)

**Endpoint**: `GET /crm/v2/tenant/{tenant}/customers`

**Purpose**: Customer master data

**Query Parameters**:
- `tenant`, `page`, `pageSize`: Standard
- `modifiedOnOrAfter`: Incremental filter
- `active`: Filter active/inactive
- `type`: "Residential" or "Commercial"
- `createdOnFrom`, `createdOnTo`: Filter by creation date

**Response Format**:
```json
{
  "data": [
    {
      "id": 111222333,
      "active": true,
      "name": "John Smith",
      "type": "Residential",
      "address": {
        "street": "123 Main St",
        "city": "Los Angeles",
        "state": "CA",
        "zip": "90001",
        "country": "USA"
      },
      "email": "john.smith@example.com",
      "phoneNumber": "+1-555-123-4567",
      "balance": 0.00,
      "customFields": [
        { "name": "PreferredContact", "value": "Email" }
      ],
      "createdOn": "2020-05-10T12:00:00Z",
      "createdById": 100,
      "modifiedOn": "2025-10-22T09:00:00Z",
      "mergedToId": null
    }
  ],
  "page": 1,
  "pageSize": 500,
  "hasMore": true
}
```

**Field Descriptions**:
- `type`: "Residential" or "Commercial"
- `balance`: Outstanding account balance
- `mergedToId`: If customer was merged, references new customer ID

**Incremental Sync**: Use `modifiedOnOrAfter`

---

### 7. Locations (CRM)

**Endpoint**: `GET /crm/v2/tenant/{tenant}/locations`

**Purpose**: Service locations (customer properties)

**Query Parameters**:
- `tenant`, `page`, `pageSize`: Standard
- `modifiedOnOrAfter`: Incremental filter
- `customerIds`: Filter by customers
- `active`: Filter active/inactive

**Response Format**:
```json
{
  "data": [
    {
      "id": 444555666,
      "customerId": 111222333,
      "active": true,
      "name": "Main Residence",
      "address": {
        "street": "123 Main St",
        "city": "Los Angeles",
        "state": "CA",
        "zip": "90001",
        "country": "USA",
        "latitude": 34.0522,
        "longitude": -118.2437
      },
      "taxZoneId": 10,
      "zoneId": 5,
      "createdOn": "2020-05-10T12:00:00Z",
      "modifiedOn": "2025-01-15T08:30:00Z",
      "customFields": []
    }
  ],
  "page": 1,
  "pageSize": 500,
  "hasMore": false
}
```

**Field Descriptions**:
- `taxZoneId`: Tax jurisdiction for billing
- `zoneId`: Service zone (for dispatch optimization)

**Incremental Sync**: Use `modifiedOnOrAfter`

---

### 8. Campaigns (Marketing)

**Endpoint**: `GET /marketing/v2/tenant/{tenant}/campaigns`

**Purpose**: Marketing campaign tracking

**Query Parameters**:
- `tenant`, `page`, `pageSize`: Standard
- `modifiedOnOrAfter`: Incremental filter
- `active`: Filter active/inactive
- `categoryIds`: Filter by campaign category

**Response Format**:
```json
{
  "data": [
    {
      "id": 50,
      "active": true,
      "name": "Fall HVAC Tune-Up Promotion",
      "categoryId": 3,
      "category": {
        "id": 3,
        "name": "Seasonal Maintenance"
      },
      "createdOn": "2025-09-01T00:00:00Z",
      "modifiedOn": "2025-10-15T10:00:00Z"
    }
  ],
  "page": 1,
  "pageSize": 500,
  "hasMore": false
}
```

**Field Descriptions**:
- `categoryId`: Campaign category (for grouping)
- `category`: Nested object with category details

**Incremental Sync**: Use `modifiedOnOrAfter`

---

## Additional Useful Endpoints (Not Yet Implemented)

### 9. Appointments

**Endpoint**: `GET /jpm/v2/tenant/{tenant}/appointments`

**Purpose**: Scheduled appointments, technician dispatch

**Key Fields**: `start`, `end`, `status`, `jobId`, `technicianIds`, `arrivalWindowStart`, `arrivalWindowEnd`

---

### 10. Projects

**Endpoint**: `GET /jpm/v2/tenant/{tenant}/projects`

**Purpose**: Multi-job projects (commercial work)

**Key Fields**: `id`, `name`, `customerId`, `startDate`, `endDate`, `status`, `totalValue`

---

### 11. Purchase Orders

**Endpoint**: `GET /inventory/v2/tenant/{tenant}/purchase-orders`

**Purpose**: Material procurement, vendor management

**Key Fields**: `id`, `vendorId`, `orderDate`, `receivedDate`, `total`, `items`

---

### 12. Employees

**Endpoint**: `GET /settings/v2/tenant/{tenant}/employees`

**Purpose**: Employee master data

**Key Fields**: `id`, `name`, `email`, `role`, `businessUnitId`, `active`

---

### 13. Technicians

**Endpoint**: `GET /settings/v2/tenant/{tenant}/technicians`

**Purpose**: Field technicians (subset of employees)

**Key Fields**: `id`, `name`, `skills`, `zoneIds`, `active`

---

### 14. Business Units

**Endpoint**: `GET /settings/v2/tenant/{tenant}/business-units`

**Purpose**: Organizational structure (your 6 BUs)

**Key Fields**: `id`, `name`, `active`, `address`

---

### 15. Job Types

**Endpoint**: `GET /settings/v2/tenant/{tenant}/job-types`

**Purpose**: Job classification (Maintenance, Repair, Install, etc.)

**Key Fields**: `id`, `name`, `businessUnitId`, `active`

---

### 16. Calls (Telecom)

**Endpoint**: `GET /telecom/v2/tenant/{tenant}/calls`

**Purpose**: Inbound call tracking, lead source attribution

**Key Fields**: `id`, `from`, `to`, `duration`, `recordingUrl`, `campaignId`, `createdOn`

---

### 17. Returns (Inventory)

**Endpoint**: `GET /inventory/v2/tenant/{tenant}/returns`

**Purpose**: Material returns, vendor credits

**Key Fields**: `id`, `vendorId`, `returnDate`, `total`, `items`

---

## Pagination Best Practices

All endpoints support pagination with:
- `page`: Page number (1-indexed)
- `pageSize`: Items per page (max 500)

**Response includes**:
- `data`: Array of entities
- `page`: Current page number
- `pageSize`: Items per page
- `hasMore`: Boolean indicating more pages exist
- `totalCount`: Total items (not always included)

**Iteration pattern** (implemented in `ServiceTitanClient.fetchAllPages()`):
```javascript
let page = 1;
let hasMore = true;

while (hasMore) {
  const response = await fetch(`/endpoint?page=${page}&pageSize=500`);
  processData(response.data);

  hasMore = response.hasMore;
  page++;
}
```

---

## Rate Limiting

**ServiceTitan Limits**:
- **~10 requests per second** per tenant
- **Burst allowance**: Short bursts up to 20 req/sec
- **429 response**: Rate limit exceeded, includes `Retry-After` header

**Client Implementation**:
- Token bucket algorithm (10 tokens/sec, 20 bucket size)
- Automatic retry with exponential backoff on 429
- Jittered delays to prevent thundering herd

---

## Error Responses

### 400 Bad Request
```json
{
  "message": "Invalid request",
  "errors": [
    { "field": "pageSize", "message": "Must be <= 500" }
  ]
}
```

### 401 Unauthorized
```json
{
  "error": "invalid_client",
  "error_description": "Client authentication failed"
}
```

### 404 Not Found
```json
{
  "message": "Tenant not found",
  "tenantId": 12345
}
```

### 429 Too Many Requests
```json
{
  "message": "Rate limit exceeded",
  "retryAfter": 30
}
```

### 500 Internal Server Error
```json
{
  "message": "An unexpected error occurred",
  "requestId": "abc123"
}
```

---

## Date/Time Formats

All timestamps use **ISO 8601** format:
- `2025-10-21T15:30:00Z` (UTC)
- `2025-10-21T15:30:00-07:00` (with timezone)

**Query parameters**:
- `modifiedOnOrAfter=2025-10-21T00:00:00Z`
- `createdOnFrom=2025-10-01T00:00:00Z`
- `createdOnTo=2025-10-31T23:59:59Z`

---

## Field Naming Conventions

ServiceTitan uses **PascalCase** in JSON responses, but the ingestor transforms to **camelCase** for consistency:

| API Response | BigQuery Column |
|--------------|-----------------|
| `JobNumber` | `jobNumber` |
| `ModifiedOn` | `modifiedOn` |
| `BusinessUnitId` | `businessUnitId` |

---

## Common Query Patterns

### Get all jobs modified in last 24 hours
```
GET /jpm/v2/tenant/{tenant}/jobs?modifiedOnOrAfter=2025-10-21T00:00:00Z&pageSize=500
```

### Get all invoices for a specific job
```
GET /accounting/v2/tenant/{tenant}/invoices?jobIds=123456789&pageSize=500
```

### Get all payments for a business unit
```
GET /accounting/v2/tenant/{tenant}/payments?businessUnitIds=1&pageSize=500
```

### Get sold estimates in a date range
```
GET /sales/v2/tenant/{tenant}/estimates?soldOnFrom=2025-10-01&soldOnTo=2025-10-31&statuses=Sold&pageSize=500
```

---

## Testing Endpoints

### Using cURL

```bash
# Get access token
TOKEN=$(curl -X POST https://auth.servicetitan.io/connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=$ST_CLIENT_ID&client_secret=$ST_CLIENT_SECRET" \
  | jq -r '.access_token')

# Fetch jobs
curl -X GET "https://api.servicetitan.io/jpm/v2/tenant/$ST_TENANT_ID/jobs?page=1&pageSize=10" \
  -H "Authorization: Bearer $TOKEN" \
  -H "ST-App-Key: $ST_APP_KEY" \
  | jq .
```

### Using Postman

1. **Collection**: Create "ServiceTitan v2 API"
2. **Authorization**: OAuth 2.0, Client Credentials
3. **Variables**: `tenant_id`, `app_key`
4. **Requests**: Import from ServiceTitan developer portal

---

## API Versioning

Current version: **v2**

Endpoint format: `/{module}/v2/tenant/{tenant}/{resource}`

Modules:
- `jpm`: Job & Project Management
- `accounting`: Financial transactions
- `sales`: Estimates, proposals
- `payroll`: Compensation
- `crm`: Customer & location data
- `marketing`: Campaigns
- `settings`: Configuration, employees
- `inventory`: Materials, vendors
- `telecom`: Call tracking

---

## Developer Resources

- **Developer Portal**: https://developer.servicetitan.io
- **API Explorer**: Interactive docs at https://developer.servicetitan.io/api-explorer
- **Support**: Contact ServiceTitan support for API access
- **Rate Limits**: Monitor usage at https://developer.servicetitan.io/dashboard

---

## Change Log (API Changes)

### 2025-10-21
- Initial v2 implementation
- 8 entity endpoints integrated

### Future Additions
- Appointments (scheduled)
- Projects (scheduled)
- Employees (planned)
- Technicians (planned)
- Call tracking (planned)

---

**For implementation details, see the corresponding ingestor classes in `src/ingestors/`**
