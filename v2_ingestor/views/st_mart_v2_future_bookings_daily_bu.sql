-- Future Bookings Daily by Business Unit
-- Shows the value of production jobs scheduled for future dates
--
-- Business Logic:
--   - Future Bookings = invoice total for jobs with scheduled start dates > today
--   - Only includes Production business units
--   - Excludes canceled/cancelled appointments
--   - Date based on scheduled start date from appointments
--
-- Grain: One row per scheduled date per business unit

CREATE OR REPLACE VIEW `kpi-auto-471020.st_mart_v2.future_bookings_daily_bu` AS

WITH future_appointments AS (
  SELECT
    a.id as appointment_id,
    a.jobId,
    a.scheduledStart,
    a.status as appointment_status,
    j.jobStatus,
    j.businessUnitId
  FROM `kpi-auto-471020.st_raw_v2.raw_appointments` a
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_jobs` j ON a.jobId = j.id
  WHERE DATE(a.scheduledStart) > CURRENT_DATE()
    AND a.status NOT IN ('Canceled', 'Cancelled')
    AND j.businessUnitId IS NOT NULL
),

job_values AS (
  SELECT
    fa.jobId,
    fa.scheduledStart,
    fa.businessUnitId,
    -- Use invoice total as the booking value
    COALESCE(SUM(i.total), 0) as booking_value
  FROM future_appointments fa
  LEFT JOIN `kpi-auto-471020.st_raw_v2.raw_invoices` i ON fa.jobId = i.jobId
  GROUP BY fa.jobId, fa.scheduledStart, fa.businessUnitId
)

SELECT
  DATE(jv.scheduledStart) as scheduled_date,
  bu.name as business_unit,
  COUNT(DISTINCT jv.jobId) as job_count,
  ROUND(SUM(jv.booking_value), 2) as total_future_bookings,
  ROUND(AVG(jv.booking_value), 2) as avg_booking_value,
  CURRENT_TIMESTAMP() as view_created_at
FROM job_values jv
LEFT JOIN `kpi-auto-471020.st_ref_v2.dim_business_units` bu ON jv.businessUnitId = bu.id
WHERE bu.name LIKE '%-Production'
  AND bu.name IS NOT NULL
GROUP BY DATE(jv.scheduledStart), bu.name
ORDER BY scheduled_date DESC, bu.name;
