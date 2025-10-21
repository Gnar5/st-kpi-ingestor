# ServiceTitan API Scopes Setup Guide

## Required API Scopes for V2 Ingestor

When creating or editing your app in the **ServiceTitan Developer Portal**, you need to enable the following API scopes:

### 📋 Scopes to Enable

Go to: https://developer.servicetitan.io/applications (or your apps dashboard)

Select your app → **API Scopes** section → Enable these:

#### 1. **Jobs & Project Management (JPM)**
- ✅ `jpm.jobs:read` - Read job data
- ✅ `jpm.projects:read` - Read project data
- ✅ `jpm.appointments:read` - Read appointment data (optional, for future)

#### 2. **Accounting**
- ✅ `accounting.invoices:read` - Read invoice data
- ✅ `accounting.payments:read` - Read payment data

#### 3. **Sales**
- ✅ `sales.estimates:read` - Read estimate data

#### 4. **Payroll**
- ✅ `payroll.gross-pay-items:read` - Read payroll data

#### 5. **CRM**
- ✅ `crm.customers:read` - Read customer data
- ✅ `crm.locations:read` - Read location data

#### 6. **Marketing**
- ✅ `marketing.campaigns:read` - Read campaign data

#### 7. **Settings** (Optional but useful)
- ✅ `settings.business-units:read` - Read business unit data
- ✅ `settings.employees:read` - Read employee data
- ✅ `settings.technicians:read` - Read technician data
- ✅ `settings.job-types:read` - Read job type data

---

## How to Enable Scopes

### Step 1: Log into Developer Portal
1. Go to https://developer.servicetitan.io
2. Sign in with your ServiceTitan credentials
3. Navigate to **"My Apps"** or **"Applications"**

### Step 2: Select Your App
- Click on the app you just created (or the one with your new Client ID/Secret)

### Step 3: Enable API Scopes
1. Find the **"API Scopes"** or **"Permissions"** section
2. Check the boxes next to all the scopes listed above
3. Click **"Save"** or **"Update"**

### Step 4: Regenerate Credentials (if needed)
- Some changes may require regenerating your Client Secret
- If so, copy the new secret to your `.env` file

---

## Testing After Enabling Scopes

Once you've enabled the scopes, test each entity:

```bash
# Test campaigns (marketing scope)
curl http://localhost:8081/ingest/campaigns

# Test customers (CRM scope)
curl http://localhost:8081/ingest/customers

# Test jobs (JPM scope)
curl http://localhost:8081/ingest/jobs

# Test invoices (accounting scope)
curl http://localhost:8081/ingest/invoices
```

---

## Troubleshooting

### Still Getting 403 Errors?

1. **Wait 5 minutes** - Scope changes can take a few minutes to propagate
2. **Restart the service**:
   ```bash
   # Kill and restart
   npm start
   ```
3. **Verify scopes in portal** - Double-check all boxes are checked
4. **Check app status** - Make sure the app is "Active" or "Published"

### Which Scopes Are Absolutely Required?

**Minimum for basic functionality:**
- `jpm.jobs:read`
- `accounting.invoices:read`
- `crm.customers:read`
- `crm.locations:read`

**Recommended for full functionality:**
- All scopes listed in the main section above

### Getting "Invalid Scope" Errors?

Some scopes might not be available for your ServiceTitan account tier. Contact ServiceTitan support to enable enterprise/advanced API access.

---

## Quick Reference: Entity → Scope Mapping

| Entity | Required Scope | Module |
|--------|---------------|--------|
| **jobs** | `jpm.jobs:read` | JPM |
| **invoices** | `accounting.invoices:read` | Accounting |
| **estimates** | `sales.estimates:read` | Sales |
| **payments** | `accounting.payments:read` | Accounting |
| **payroll** | `payroll.gross-pay-items:read` | Payroll |
| **customers** | `crm.customers:read` | CRM |
| **locations** | `crm.locations:read` | CRM |
| **campaigns** | `marketing.campaigns:read` | Marketing |

---

## After Enabling Scopes

Once all scopes are enabled, your v2 ingestor will be able to:

1. ✅ Authenticate successfully (already working)
2. ✅ Fetch data from all 8 entity endpoints
3. ✅ Write to BigQuery
4. ✅ Track sync state
5. ✅ Run scheduled syncs

**Then you can proceed with:**
- Full testing of all entities
- Deployment to Cloud Run
- Setting up Cloud Scheduler
- Running parallel with v1

---

## Need Help?

If you're unable to enable certain scopes:
- Contact ServiceTitan Developer Support: developer@servicetitan.com
- Reference your Tenant ID: `636913317`
- Mention you need "Entity API access" for data integration

---

**Once scopes are enabled, the v2 ingestor will work immediately - no code changes needed!**
