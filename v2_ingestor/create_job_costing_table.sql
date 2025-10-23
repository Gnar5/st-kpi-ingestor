-- Create Job Costing Composite Table
-- This table combines data from multiple sources to replicate ServiceTitan's FOREMAN Job Cost report
-- Used for accurate Dollars Produced and GPM calculations

CREATE OR REPLACE TABLE `kpi-auto-471020.st_mart_v2.job_costing` AS

WITH
-- Get job appointments to determine scheduled start date (job start date)
job_appointments AS (
  SELECT
    jobId,
    MIN(scheduledStart) as job_start_date,  -- First scheduled appointment is the job start date
    MAX(scheduledEnd) as job_end_date
  FROM `kpi-auto-471020.st_raw_v2.raw_appointments`
  WHERE jobId IS NOT NULL
  GROUP BY jobId
),

-- Get invoice totals by job (revenue)
job_invoices AS (
  SELECT
    jobId,
    SUM(subTotal) as invoice_subtotal,
    SUM(total) as invoice_total,
    SUM(salesTax) as invoice_tax,
    COUNT(*) as invoice_count
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices`
  WHERE jobId IS NOT NULL
  GROUP BY jobId
),

-- Get labor costs by job from payroll
job_labor AS (
  SELECT
    jobId,
    SUM(amount) as labor_gross_pay,
    -- Assume 30% burden rate (payroll taxes, benefits, etc.) if not specified
    SUM(amount * 0.30) as labor_burden,
    SUM(amount * 1.30) as total_labor_cost,
    COUNT(DISTINCT employeeId) as tech_count
  FROM `kpi-auto-471020.st_raw_v2.raw_payroll`
  WHERE jobId IS NOT NULL
  GROUP BY jobId
),

-- Get material costs from purchase orders
job_materials AS (
  SELECT
    jobId,
    SUM(total) as material_cost,
    COUNT(*) as po_count
  FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders`
  WHERE jobId IS NOT NULL
    AND status = 'Billed'  -- Only include billed POs
  GROUP BY jobId
),

-- Get returns that reduce job costs
job_returns AS (
  SELECT
    jobId,
    SUM(total) as return_credit
  FROM `kpi-auto-471020.st_raw_v2.raw_returns`
  WHERE jobId IS NOT NULL
  GROUP BY jobId
),

-- Main job costing query
job_costing AS (
  SELECT
    j.id as job_id,
    j.jobNumber,
    j.jobTypeName as jobType,
    j.jobStatus,
    j.businessUnitId,
    j.businessUnitNormalized,

    -- Dates
    ja.job_start_date,
    ja.job_end_date,
    j.completedOn as completed_date,
    j.createdOn as created_date,

    -- Revenue (from invoices)
    COALESCE(ji.invoice_subtotal, 0) as revenue_subtotal,
    COALESCE(ji.invoice_total, 0) as revenue_total,

    -- Costs
    COALESCE(jl.total_labor_cost, 0) as labor_cost,
    COALESCE(jm.material_cost, 0) - COALESCE(jr.return_credit, 0) as material_cost_net,

    -- Total costs
    COALESCE(jl.total_labor_cost, 0) +
    COALESCE(jm.material_cost, 0) -
    COALESCE(jr.return_credit, 0) as total_cost,

    -- Gross profit and margin
    COALESCE(ji.invoice_subtotal, 0) - (
      COALESCE(jl.total_labor_cost, 0) +
      COALESCE(jm.material_cost, 0) -
      COALESCE(jr.return_credit, 0)
    ) as gross_profit,

    -- GPM %
    SAFE_DIVIDE(
      COALESCE(ji.invoice_subtotal, 0) - (
        COALESCE(jl.total_labor_cost, 0) +
        COALESCE(jm.material_cost, 0) -
        COALESCE(jr.return_credit, 0)
      ),
      NULLIF(COALESCE(ji.invoice_subtotal, 0), 0)
    ) * 100 as gpm_percent,

    -- Component details for troubleshooting
    COALESCE(jl.labor_gross_pay, 0) as labor_gross_pay,
    COALESCE(jl.labor_burden, 0) as labor_burden,
    COALESCE(jm.material_cost, 0) as material_cost_raw,
    COALESCE(jr.return_credit, 0) as return_credit,
    COALESCE(ji.invoice_count, 0) as invoice_count,
    COALESCE(jl.tech_count, 0) as tech_count,
    COALESCE(jm.po_count, 0) as po_count,

    -- Is this a warranty job?
    CASE
      WHEN j.jobTypeName IN ('Warranty', 'Touchup') THEN TRUE
      ELSE FALSE
    END as is_warranty,

    -- Ingestion metadata
    CURRENT_TIMESTAMP() as created_at

  FROM `kpi-auto-471020.st_dim_v2.dim_jobs` j
  LEFT JOIN job_appointments ja ON j.id = ja.jobId
  LEFT JOIN job_invoices ji ON j.id = ji.jobId
  LEFT JOIN job_labor jl ON j.id = jl.jobId
  LEFT JOIN job_materials jm ON j.id = jm.jobId
  LEFT JOIN job_returns jr ON j.id = jr.jobId

  -- Only include jobs with appointments (needed for job start date)
  WHERE ja.job_start_date IS NOT NULL
)

SELECT * FROM job_costing;