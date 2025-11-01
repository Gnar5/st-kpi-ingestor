-- ServiceTitan Baseline Data for Week 8/18/2025 - 8/24/2025
-- This is the TRUTH from ServiceTitan that we need to match in BigQuery

-- Expected Results by Business Unit:
-- BU: Tucson (ID: 899)
--   lead_count: 39
--   num_estimates: 46
--   close_rate_percent: 51.22%
--   total_booked: $89,990.11
--   dollars_produced: $83,761.16
--   gpm_percent: 48.00%
--   warranty_percent: 0.38%
--   outstanding_ar: $412,265.50
--   future_bookings: $150,992.16
--   dollars_collected: $92,624.87

-- BU: Phoenix (ID: 898)
--   lead_count: 96
--   num_estimates: 85
--   close_rate_percent: 39.74%
--   total_booked: $116,551.26
--   dollars_produced: $232,891.98
--   gpm_percent: 50.83%
--   warranty_percent: 1.26%
--   outstanding_ar: $269,530.00
--   future_bookings: $1,076,890.00
--   dollars_collected: $250,825.33

-- BU: Nevada (ID: 901)
--   lead_count: 28
--   num_estimates: 22
--   close_rate_percent: 60.87%
--   total_booked: $105,890.00
--   dollars_produced: $23,975.00
--   gpm_percent: 24.04%
--   warranty_percent: 10.46%
--   outstanding_ar: $216,853.00
--   future_bookings: $239,753.00
--   dollars_collected: $95,877.28

-- BU: Andy's Painting (ID: 95763481)
--   lead_count: 25
--   num_estimates: 24
--   close_rate_percent: 35.71%
--   total_booked: $30,896.91
--   dollars_produced: $53,752.56
--   gpm_percent: 47.83%
--   warranty_percent: 1.42%
--   outstanding_ar: $164,367.00
--   future_bookings: $249,145.00
--   dollars_collected: $65,297.29

-- BU: Commercial AZ (ID: 2305)
--   lead_count: 22
--   num_estimates: 24
--   close_rate_percent: 26.92%
--   total_booked: $119,803.60
--   dollars_produced: $77,345.25
--   gpm_percent: 46.98%
--   warranty_percent: 0.00%
--   outstanding_ar: $488,924.00
--   future_bookings: $355,529.00
--   dollars_collected: $62,439.50

-- BU: Guaranteed Painting (ID: 117043321)
--   lead_count: 8
--   num_estimates: 7
--   close_rate_percent: 77.78%
--   total_booked: $26,067.40
--   dollars_produced: $30,472.30
--   gpm_percent: 45.84%
--   warranty_percent: 0.00%
--   outstanding_ar: $195,840.00
--   future_bookings: $174,697.00
--   dollars_collected: $65,521.11

-- Query to validate against BigQuery
WITH servicetitan_baseline AS (
  SELECT 'Tucson' as business_unit, 39 as lead_count, 46 as num_estimates, 51.22 as close_rate_percent, 89990.11 as total_booked, 83761.16 as dollars_produced, 48.00 as gpm_percent, 0.38 as warranty_percent, 412265.50 as outstanding_ar, 150992.16 as future_bookings, 92624.87 as dollars_collected
  UNION ALL SELECT 'Phoenix', 96, 85, 39.74, 116551.26, 232891.98, 50.83, 1.26, 269530.00, 1076890.00, 250825.33
  UNION ALL SELECT 'Nevada', 28, 22, 60.87, 105890.00, 23975.00, 24.04, 10.46, 216853.00, 239753.00, 95877.28
  UNION ALL SELECT 'Andy''s Painting', 25, 24, 35.71, 30896.91, 53752.56, 47.83, 1.42, 164367.00, 249145.00, 65297.29
  UNION ALL SELECT 'Commercial AZ', 22, 24, 26.92, 119803.60, 77345.25, 46.98, 0.00, 488924.00, 355529.00, 62439.50
  UNION ALL SELECT 'Guaranteed Painting', 8, 7, 77.78, 26067.40, 30472.30, 45.84, 0.00, 195840.00, 174697.00, 65521.11
)
SELECT * FROM servicetitan_baseline;
