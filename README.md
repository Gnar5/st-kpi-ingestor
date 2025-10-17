# ServiceTitan → BigQuery → Looker (Starter)

## 0) Prereqs
- Project: kpi-auto-471020
- APIs enabled: Secret Manager, Cloud Run, Cloud Build, Artifact Registry, Cloud Scheduler, BigQuery
- Secrets created: ST_TENANT_ID, ST_CLIENT_ID, ST_CLIENT_SECRET, ST_APP_KEY
- Roles for your Cloud Run service account: secretmanager.secretAccessor, bigquery.dataEditor

## 1) Create datasets & tables
bq --location=US mk -d st_kpi || true
bq --location=US mk -d st_kpi_mart || true
bq query --use_legacy_sql=false < sql/ddl.sql

## 2) Deploy to Cloud Run
gcloud run deploy st-kpi-ingestor --source . --region us-central1 --allow-unauthenticated --project kpi-auto-471020

## 3) Test endpoints
curl https://<run-url>/ingest/leads
curl https://<run-url>/ingest/daily_wbr
curl https://<run-url>/ingest/foreman
curl https://<run-url>/ingest/collections
curl https://<run-url>/ingest/ar

## 4) Schedule hourly in Cloud Scheduler (stagger minutes)
gcloud scheduler jobs create http st-ingest-leads       --schedule="0 * * * *"  --uri="https://<run-url>/ingest/leads"       --http-method=GET --region=us-central1
gcloud scheduler jobs create http st-ingest-dailywbr    --schedule="5 * * * *"  --uri="https://<run-url>/ingest/daily_wbr"    --http-method=GET --region=us-central1
gcloud scheduler jobs create http st-ingest-foreman     --schedule="10 * * * *" --uri="https://<run-url>/ingest/foreman"     --http-method=GET --region=us-central1
gcloud scheduler jobs create http st-ingest-collections --schedule="15 * * * *" --uri="https://<run-url>/ingest/collections" --http-method=GET --region=us-central1
gcloud scheduler jobs create http st-ingest-ar          --schedule="20 * * * *" --uri="https://<run-url>/ingest/ar"          --http-method=GET --region=us-central1
