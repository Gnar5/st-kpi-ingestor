#!/bin/bash
# rebuild_dim_jobs.sh
# Rebuilds the dim_jobs dimension table from raw_jobs + reference tables
# Should be run daily after data ingestion completes

set -e

echo "Starting dim_jobs rebuild at $(date)"

bq query --use_legacy_sql=false "
CREATE OR REPLACE TABLE \`kpi-auto-471020.st_dim_v2.dim_jobs\` AS
SELECT
  j.*,
  bu.name as businessUnitName,
  bu.active as businessUnitActive,
  COALESCE(bu.name, 'Unknown') as businessUnitNormalized,
  jt.name as jobTypeName,
  jt.active as jobTypeActive
FROM \`kpi-auto-471020.st_raw_v2.raw_jobs\` j
LEFT JOIN \`kpi-auto-471020.st_ref_v2.dim_business_units\` bu ON j.businessUnitId = bu.id
LEFT JOIN \`kpi-auto-471020.st_ref_v2.dim_job_types\` jt ON j.jobTypeId = jt.id
"

echo "dim_jobs rebuild completed successfully at $(date)"

# Get row count for verification
ROW_COUNT=$(bq query --use_legacy_sql=false --format=csv "SELECT COUNT(*) as count FROM \`kpi-auto-471020.st_dim_v2.dim_jobs\`" | tail -1)
echo "dim_jobs now has $ROW_COUNT rows"
