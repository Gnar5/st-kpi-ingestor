# Quick Fix: Cloud Run 400 Authentication Errors

**Issue:** Cloud Run deployment is failing with 400 errors during authentication
**Cause:** Environment variables (ST_CLIENT_ID, ST_CLIENT_SECRET, etc.) are not configured on Cloud Run
**Solution:** Configure secrets in Cloud Run

---

## Option 1: Use Google Secret Manager (Recommended for Production)

### Step 1: Create Secrets in Secret Manager

```bash
# Fetch your current .env values
cd v2_ingestor
source .env

# Create secrets (one-time setup)
echo -n "$ST_CLIENT_ID" | gcloud secrets create ST_CLIENT_ID \
  --data-file=- \
  --replication-policy=automatic

echo -n "$ST_CLIENT_SECRET" | gcloud secrets create ST_CLIENT_SECRET \
  --data-file=- \
  --replication-policy=automatic

echo -n "$ST_APP_KEY" | gcloud secrets create ST_APP_KEY \
  --data-file=- \
  --replication-policy=automatic

echo -n "$ST_TENANT_ID" | gcloud secrets create ST_TENANT_ID \
  --data-file=- \
  --replication-policy=automatic

echo -n "kpi-auto-471020" | gcloud secrets create BQ_PROJECT_ID \
  --data-file=- \
  --replication-policy=automatic
```

### Step 2: Grant Cloud Run Access to Secrets

```bash
# Get your Cloud Run service account email
SERVICE_ACCOUNT=$(gcloud run services describe st-v2-ingestor \
  --region=us-central1 \
  --format='value(spec.template.spec.serviceAccountName)')

# If empty, it uses the default compute service account
if [ -z "$SERVICE_ACCOUNT" ]; then
  PROJECT_NUMBER=$(gcloud projects describe kpi-auto-471020 --format='value(projectNumber)')
  SERVICE_ACCOUNT="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
fi

echo "Service Account: $SERVICE_ACCOUNT"

# Grant access to each secret
for SECRET in ST_CLIENT_ID ST_CLIENT_SECRET ST_APP_KEY ST_TENANT_ID BQ_PROJECT_ID; do
  gcloud secrets add-iam-policy-binding $SECRET \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/secretmanager.secretAccessor"
done
```

### Step 3: Update Cloud Run to Use Secrets

```bash
gcloud run services update st-v2-ingestor \
  --region=us-central1 \
  --update-secrets=ST_CLIENT_ID=ST_CLIENT_ID:latest \
  --update-secrets=ST_CLIENT_SECRET=ST_CLIENT_SECRET:latest \
  --update-secrets=ST_APP_KEY=ST_APP_KEY:latest \
  --update-secrets=ST_TENANT_ID=ST_TENANT_ID:latest \
  --update-secrets=BQ_PROJECT_ID=BQ_PROJECT_ID:latest
```

---

## Option 2: Use Environment Variables (Faster but Less Secure)

### Update Cloud Run with Environment Variables Directly

```bash
cd v2_ingestor
source .env

gcloud run services update st-v2-ingestor \
  --region=us-central1 \
  --set-env-vars=ST_CLIENT_ID=$ST_CLIENT_ID \
  --set-env-vars=ST_CLIENT_SECRET=$ST_CLIENT_SECRET \
  --set-env-vars=ST_APP_KEY=$ST_APP_KEY \
  --set-env-vars=ST_TENANT_ID=$ST_TENANT_ID \
  --set-env-vars=BQ_PROJECT_ID=kpi-auto-471020
```

**Warning:** This exposes credentials in Cloud Run console. Use Option 1 for production.

---

## Option 3: Redeploy with Environment Variables

Instead of updating, redeploy with env vars from the start:

```bash
cd v2_ingestor
source .env

gcloud run deploy st-v2-ingestor \
  --source . \
  --region us-central1 \
  --platform managed \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 3600 \
  --set-env-vars ST_CLIENT_ID=$ST_CLIENT_ID,ST_CLIENT_SECRET=$ST_CLIENT_SECRET,ST_APP_KEY=$ST_APP_KEY,ST_TENANT_ID=$ST_TENANT_ID,BQ_PROJECT_ID=kpi-auto-471020
```

---

## Verify It Worked

### Test a Simple Endpoint

```bash
# Get your Cloud Run URL
SERVICE_URL=$(gcloud run services describe st-v2-ingestor \
  --region=us-central1 \
  --format='value(status.url)')

echo "Service URL: $SERVICE_URL"

# Test health check
curl "$SERVICE_URL/health"

# Should return:
# {"status":"healthy","service":"st-v2-ingestor","version":"2.0.0",...}
```

### Test a Small Entity

```bash
# Try campaigns (smallest dataset)
curl "$SERVICE_URL/ingest/campaigns?mode=incremental"

# Should return:
# {"success":true,"entity":"campaigns","recordsProcessed":3,...}
```

### Check Logs

```bash
# View recent logs
gcloud run services logs read st-v2-ingestor \
  --region=us-central1 \
  --limit=50

# Look for:
# ✅ "Authentication successful"
# ❌ "Request failed with status code 400"
```

---

## Once Fixed, Retry Backfill

```bash
# Get your Cloud Run URL
SERVICE_URL=$(gcloud run services describe st-v2-ingestor \
  --region=us-central1 \
  --format='value(status.url)')

# Run full backfill (all entities in parallel)
curl "$SERVICE_URL/ingest-all?mode=full&parallel=true"

# Or one at a time for safer execution
curl "$SERVICE_URL/ingest/jobs?mode=full"
curl "$SERVICE_URL/ingest/invoices?mode=full"
curl "$SERVICE_URL/ingest/estimates?mode=full"
curl "$SERVICE_URL/ingest/payments?mode=full"
curl "$SERVICE_URL/ingest/payroll?mode=full"
curl "$SERVICE_URL/ingest/customers?mode=full"
curl "$SERVICE_URL/ingest/locations?mode=full"
curl "$SERVICE_URL/ingest/campaigns?mode=full"
```

---

## Recommended Approach

**For quick testing:** Use **Option 2** (environment variables)
**For production:** Use **Option 1** (Secret Manager)

**Why Option 1 is better:**
- Credentials not visible in Cloud Run console
- Automatic rotation support
- Audit logging
- Fine-grained access control

---

## Troubleshooting

### Still getting 400 errors?

**Check 1: Verify env vars are set**
```bash
gcloud run services describe st-v2-ingestor \
  --region=us-central1 \
  --format='value(spec.template.spec.containers[0].env)'
```

Should show:
```
name: ST_CLIENT_ID
value: <your-client-id>
...
```

**Check 2: Verify credentials are valid**
```bash
# Test locally first
cd v2_ingestor
source .env
npm start

# In another terminal
curl "http://localhost:8081/ingest/campaigns?mode=incremental"

# If local works but Cloud Run doesn't, it's an env var issue
```

**Check 3: Check Cloud Run logs for actual error**
```bash
gcloud run services logs read st-v2-ingestor \
  --region=us-central1 \
  --limit=100 | grep -A 5 "400"
```

### Getting 403 errors instead?

That's actually progress! 403 means authentication worked but API permissions are wrong.

**Fix:** Check ServiceTitan developer portal and ensure all API scopes are enabled (see SERVICETITAN_SCOPES_GUIDE.md)

---

**Next:** Once credentials are configured, run the full backfill!
