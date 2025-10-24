#!/bin/bash
# Deploy Standard KPI Views to BigQuery
# Creates views for Opportunities, Leads, and Completed Estimates

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VIEWS_DIR="$SCRIPT_DIR/views"

echo "========================================="
echo "Deploying Standard KPI Views to BigQuery"
echo "========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

deploy_view() {
  local sql_file=$1
  local view_name=$2

  echo -n "Deploying $view_name... "

  if bq query --use_legacy_sql=false < "$VIEWS_DIR/$sql_file" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ SUCCESS${NC}"
    return 0
  else
    echo -e "${RED}✗ FAILED${NC}"
    return 1
  fi
}

# Track deployment status
TOTAL=8
SUCCESS=0
FAILED=0

echo "Step 1: Deploying Stage Views"
echo "------------------------------"

# Stage views must be deployed first (mart views depend on them)
deploy_view "st_stage_opportunity_jobs.sql" "st_stage.opportunity_jobs" && ((SUCCESS++)) || ((FAILED++))
deploy_view "st_stage_estimate_with_opportunity.sql" "st_stage.estimate_with_opportunity" && ((SUCCESS++)) || ((FAILED++))
deploy_view "st_stage_leads_jobs.sql" "st_stage.leads_jobs" && ((SUCCESS++)) || ((FAILED++))
deploy_view "st_stage_completed_estimates_jobs.sql" "st_stage.completed_estimates_jobs" && ((SUCCESS++)) || ((FAILED++))

echo ""
echo "Step 2: Deploying Mart Views"
echo "-----------------------------"

# Mart views depend on stage views
deploy_view "st_mart_v2_opportunity_daily.sql" "st_mart_v2.opportunity_daily" && ((SUCCESS++)) || ((FAILED++))
deploy_view "st_mart_v2_leads_daily.sql" "st_mart_v2.leads_daily" && ((SUCCESS++)) || ((FAILED++))
deploy_view "st_mart_v2_leads_daily_bu.sql" "st_mart_v2.leads_daily_bu" && ((SUCCESS++)) || ((FAILED++))
deploy_view "st_mart_v2_completed_estimates_daily.sql" "st_mart_v2.completed_estimates_daily" && ((SUCCESS++)) || ((FAILED++))

echo ""
echo "========================================="
echo "Deployment Summary"
echo "========================================="
echo "Total Views: $TOTAL"
echo -e "Successful: ${GREEN}$SUCCESS${NC}"
echo -e "Failed: ${RED}$FAILED${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
  echo -e "${GREEN}All views deployed successfully!${NC}"
  echo ""
  echo "Next steps:"
  echo "  1. Run validation queries: bq query --use_legacy_sql=false < validation/validate_kpi_views.sql"
  echo "  2. Check Looker dashboards to ensure data is flowing correctly"
  exit 0
else
  echo -e "${RED}Some views failed to deploy. Please check the errors above.${NC}"
  exit 1
fi
