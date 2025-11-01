-- Updated job_costing table that includes invoice materials
-- This fixes the missing $12K in materials

CREATE OR REPLACE TABLE `kpi-auto-471020.st_mart_v2.job_costing_v2` AS

WITH
-- Get job appointment dates in Phoenix timezone
job_appointments AS (
  SELECT
    jobId,
    MIN(DATETIME(TIMESTAMP(scheduledStart), 'America/Phoenix')) as job_start_date,
    MAX(DATETIME(TIMESTAMP(scheduledEnd), 'America/Phoenix')) as job_end_date
  FROM `kpi-auto-471020.st_raw_v2.raw_appointments`
  WHERE jobId IS NOT NULL
  GROUP BY jobId
),

-- Get revenue from invoices
job_invoices AS (
  SELECT
    jobId,
    SUM(subtotal) as invoice_subtotal,
    SUM(total) as invoice_total,
    COUNT(*) as invoice_count
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices`
  WHERE jobId IS NOT NULL
  GROUP BY jobId
),

-- Get labor costs from payroll (with deduplication)
job_labor AS (
  SELECT
    jobId,
    SUM(amount) as labor_gross_pay,
    SUM(amount) as total_labor_cost,
    COUNT(DISTINCT dedup_key) as tech_count
  FROM (
    SELECT DISTINCT
      jobId,
      employeeId,
      date,
      activity,
      amount,
      paidDurationHours,
      CONCAT(
        CAST(jobId AS STRING), '-',
        CAST(employeeId AS STRING), '-',
        CAST(date AS STRING), '-',
        COALESCE(activity, 'NULL'), '-',
        CAST(amount AS STRING), '-',
        CAST(paidDurationHours AS STRING)
      ) as dedup_key
    FROM `kpi-auto-471020.st_raw_v2.raw_payroll`
    WHERE jobId IS NOT NULL
  )
  GROUP BY jobId
),

-- Get payroll adjustments via invoices
job_payroll_adjustments AS (
  SELECT
    i.jobId,
    SUM(pa.amount) as payroll_adj
  FROM `kpi-auto-471020.st_raw_v2.raw_payroll_adjustments` pa
  JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON pa.invoiceId = i.id
  WHERE i.jobId IS NOT NULL
  GROUP BY 1
),

-- Get material costs from purchase orders
job_materials_pos AS (
  SELECT
    jobId,
    SUM(total) as po_cost,
    COUNT(*) as po_count
  FROM `kpi-auto-471020.st_raw_v2.raw_purchase_orders`
  WHERE jobId IS NOT NULL
    AND status != 'Canceled'
  GROUP BY jobId
),

-- NEW: Get material costs from invoice line items
job_materials_invoice AS (
  SELECT
    i.jobId,
    SUM(CAST(JSON_VALUE(item, '$.cost') AS FLOAT64)) as invoice_material_cost,
    COUNT(*) as material_item_count
  FROM `kpi-auto-471020.st_raw_v2.raw_invoices` i,
  UNNEST(JSON_QUERY_ARRAY(i.items)) as item
  WHERE i.jobId IS NOT NULL
    AND JSON_VALUE(item, '$.type') IN ('Material', 'Equipment')
  GROUP BY 1
),

-- Combine all material sources
job_materials AS (
  SELECT
    COALESCE(po.jobId, im.jobId) as jobId,
    COALESCE(po.po_cost, 0) + COALESCE(im.invoice_material_cost, 0) as material_cost,
    COALESCE(po.po_count, 0) as po_count,
    COALESCE(im.material_item_count, 0) as invoice_material_count
  FROM job_materials_pos po
  FULL OUTER JOIN job_materials_invoice im ON po.jobId = im.jobId
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

    -- Labor costs (including payroll adjustments)
    COALESCE(jl.total_labor_cost, 0) + COALESCE(jpa.payroll_adj, 0) as labor_cost,

    -- Materials (POs + Invoice materials - returns)
    COALESCE(jm.material_cost, 0) - COALESCE(jr.return_credit, 0) as material_cost_net,

    -- Total costs
    COALESCE(jl.total_labor_cost, 0) +
    COALESCE(jpa.payroll_adj, 0) +
    COALESCE(jm.material_cost, 0) -
    COALESCE(jr.return_credit, 0) as total_cost,

    -- Gross profit and margin
    COALESCE(ji.invoice_subtotal, 0) - (
      COALESCE(jl.total_labor_cost, 0) +
      COALESCE(jpa.payroll_adj, 0) +
      COALESCE(jm.material_cost, 0) -
      COALESCE(jr.return_credit, 0)
    ) as gross_profit,

    -- GPM %
    SAFE_DIVIDE(
      COALESCE(ji.invoice_subtotal, 0) - (
        COALESCE(jl.total_labor_cost, 0) +
        COALESCE(jpa.payroll_adj, 0) +
        COALESCE(jm.material_cost, 0) -
        COALESCE(jr.return_credit, 0)
      ),
      NULLIF(COALESCE(ji.invoice_subtotal, 0), 0)
    ) * 100 as gpm_percent,

    -- Component details for troubleshooting
    COALESCE(jl.labor_gross_pay, 0) as labor_gross_pay,
    COALESCE(jpa.payroll_adj, 0) as payroll_adjustments,
    COALESCE(jm.material_cost, 0) as material_cost_raw,
    COALESCE(jr.return_credit, 0) as return_credit,
    COALESCE(ji.invoice_count, 0) as invoice_count,
    COALESCE(jl.tech_count, 0) as tech_count,
    COALESCE(jm.po_count, 0) as po_count,
    COALESCE(jm.invoice_material_count, 0) as invoice_material_count,

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
  LEFT JOIN job_payroll_adjustments jpa ON j.id = jpa.jobId
  LEFT JOIN job_materials jm ON j.id = jm.jobId
  LEFT JOIN job_returns jr ON j.id = jr.jobId

  -- Only include jobs with appointments (needed for job start date)
  WHERE ja.job_start_date IS NOT NULL
)

SELECT * FROM job_costing;