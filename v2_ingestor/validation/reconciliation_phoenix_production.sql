-- Phoenix Production Reconciliation - Find the $25k gap
-- ServiceTitan FOREMAN shows $232,891.98, BigQuery shows $207,980.78
-- Missing: $24,911.20

-- Step 1: Get ALL completed Phoenix jobs with scheduled dates 8/18-8/24
WITH bigquery_jobs AS (
  SELECT
    jc.job_id,
    jc.jobNumber,
    jc.jobTypeName,
    jc.jobStatus,
    jc.job_start_date,
    jc.completed_date,
    jc.revenue_subtotal,
    jc.total_cost,
    jc.gpm_percent
  FROM `kpi-auto-471020.st_mart_v2.job_costing` jc
  WHERE jc.businessUnitNormalized = 'Phoenix-Production'
    AND DATE(jc.job_start_date) BETWEEN '2025-08-18' AND '2025-08-24'
    AND jc.jobStatus = 'Completed'
),

-- Step 2: Create a reference table from ST FOREMAN report (manual entry from PDF)
-- Note: The FOREMAN report shows 119 jobs total for Phoenix
servicetitan_jobs AS (
  SELECT job_number, revenue_subtotal FROM (
    -- Extract from FOREMAN PDF - sample of jobs with revenue
    SELECT '147344566' as job_number, 24911.20 as revenue_subtotal UNION ALL
    SELECT '367295288', 11661.74 UNION ALL
    SELECT '386128461', 10642.50 UNION ALL
    SELECT '385786771', 11450.00 UNION ALL
    SELECT '367014849', 9255.61 UNION ALL
    SELECT '365691691', 7315.18 UNION ALL
    -- Add more as needed for complete reconciliation
    SELECT 'PLACEHOLDER', 0.00 -- Replace with actual data
  )
),

-- Step 3: Anti-join to find jobs in ST but not in BQ
missing_in_bq AS (
  SELECT
    st.job_number,
    st.revenue_subtotal as st_revenue,
    bq.jobNumber as bq_job_number,
    bq.revenue_subtotal as bq_revenue,
    CASE
      WHEN bq.jobNumber IS NULL THEN 'Job not in BQ'
      WHEN bq.revenue_subtotal = 0 THEN 'Job in BQ but $0 revenue'
      WHEN bq.revenue_subtotal != st.revenue_subtotal THEN 'Revenue mismatch'
      ELSE 'Match'
    END as status
  FROM servicetitan_jobs st
  LEFT JOIN bigquery_jobs bq ON st.job_number = bq.jobNumber
  WHERE st.job_number != 'PLACEHOLDER'
),

-- Step 4: Jobs in BQ but potentially shouldn't be (extra jobs)
extra_in_bq AS (
  SELECT
    bq.jobNumber,
    bq.revenue_subtotal as bq_revenue,
    bq.jobStatus,
    bq.job_start_date
  FROM bigquery_jobs bq
  LEFT JOIN servicetitan_jobs st ON bq.jobNumber = st.job_number
  WHERE st.job_number IS NULL
    AND bq.revenue_subtotal > 0 -- Only flag jobs with revenue
)

-- Final diagnostic output
SELECT
  'Missing in BQ' as issue_type,
  COUNT(*) as job_count,
  ROUND(SUM(st_revenue), 2) as total_revenue_impact
FROM missing_in_bq
WHERE status != 'Match'

UNION ALL

SELECT
  'Extra in BQ' as issue_type,
  COUNT(*) as job_count,
  ROUND(SUM(bq_revenue), 2) as total_revenue_impact
FROM extra_in_bq

UNION ALL

SELECT
  'BQ Total' as issue_type,
  COUNT(*) as job_count,
  ROUND(SUM(revenue_subtotal), 2) as total_revenue
FROM bigquery_jobs

UNION ALL

SELECT
  'ST Total (from report)' as issue_type,
  119 as job_count,  -- From FOREMAN PDF
  232891.98 as total_revenue;
