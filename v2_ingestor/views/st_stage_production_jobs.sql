-- st_stage.production_jobs
-- Jobs filtered for production KPIs (dollars produced, GPM, etc.)
-- UPDATED: 2025-10-29 to include Scheduled and InProgress jobs
--
-- Business Logic:
--   - Production jobs = jobs from Production BUs with status Completed, Hold, Scheduled, or InProgress
--   - Uses job_costing table which has revenue and cost breakdowns
--   - Date: job_start_date (from first appointment) in America/Phoenix timezone
--   - Matches ServiceTitan FOREMAN Job Cost report methodology
--
-- Grain: One row per production job from job_costing table

CREATE OR REPLACE VIEW `kpi-auto-471020.st_stage.production_jobs` AS

SELECT
  jc.job_id,
  jc.businessUnitNormalized as business_unit,
  jc.jobStatus as job_status,

  -- Revenue and costs
  jc.revenue_subtotal,
  jc.gross_profit,
  jc.total_cost,
  jc.labor_cost,
  jc.material_cost_net,

  -- Job characteristics
  jc.is_warranty,

  -- Dates (already in Phoenix timezone from job_costing)
  jc.job_start_date as start_date,
  jc.completed_date as completion_date,

  -- Metadata
  CURRENT_TIMESTAMP() as view_created_at

FROM `kpi-auto-471020.st_mart_v2.job_costing` jc

WHERE
  -- Filter: Production business units
  jc.businessUnitNormalized IN (
    'Phoenix-Production',
    'Tucson-Production',
    'Nevada-Production',
    "Andy's Painting-Production",
    'Commercial-AZ-Production',
    'Guaranteed Painting-Production'
  )

  -- Filter: Include all active production job statuses
  -- ServiceTitan FOREMAN report includes: Completed, Hold, Scheduled, InProgress
  AND jc.jobStatus IN ('Completed', 'Hold', 'Scheduled', 'InProgress')

  -- Basic nullability checks
  AND jc.job_id IS NOT NULL
  AND jc.job_start_date IS NOT NULL
;
