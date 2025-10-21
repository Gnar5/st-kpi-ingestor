#!/bin/bash

source .env

# Get access token
TOKEN_RESPONSE=$(curl -s -X POST https://auth.servicetitan.io/connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=$ST_CLIENT_ID" \
  -d "client_secret=$ST_CLIENT_SECRET")

ACCESS_TOKEN=$(echo $TOKEN_RESPONSE | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4)

echo "Testing Estimates API..."
curl -s -w "\nHTTP_CODE:%{http_code}\n" \
  "https://api.servicetitan.io/sales/v2/tenant/$ST_TENANT_ID/estimates?page=1&pageSize=10" \
  -H "ST-App-Key: $ST_APP_KEY" \
  -H "Authorization: Bearer $ACCESS_TOKEN"

echo ""
echo "---"
echo "Testing Payroll API..."
curl -s -w "\nHTTP_CODE:%{http_code}\n" \
  "https://api.servicetitan.io/payroll/v2/tenant/$ST_TENANT_ID/gross-pay-items?page=1&pageSize=10" \
  -H "ST-App-Key: $ST_APP_KEY" \
  -H "Authorization: Bearer $ACCESS_TOKEN"
