-- Dashboard 1: Daily activity totals
SELECT
  date(start_dt) AS day,
  SUM(CASE WHEN type = 'HKQuantityTypeIdentifierStepCount' THEN value_num ELSE 0 END) AS steps,
  SUM(CASE WHEN type = 'HKQuantityTypeIdentifierDistanceWalkingRunning' THEN value_num ELSE 0 END) AS distance_m,
  SUM(CASE WHEN type = 'HKQuantityTypeIdentifierActiveEnergyBurned' THEN value_num ELSE 0 END) AS active_energy_kcal,
  SUM(CASE WHEN type = 'HKQuantityTypeIdentifierAppleExerciseTime' THEN value_num ELSE 0 END) AS exercise_min,
  SUM(CASE WHEN type = 'HKQuantityTypeIdentifierAppleStandTime' THEN value_num ELSE 0 END) AS stand_min
FROM records_norm
WHERE type IN (
  'HKQuantityTypeIdentifierStepCount',
  'HKQuantityTypeIdentifierDistanceWalkingRunning',
  'HKQuantityTypeIdentifierActiveEnergyBurned',
  'HKQuantityTypeIdentifierAppleExerciseTime',
  'HKQuantityTypeIdentifierAppleStandTime'
)
GROUP BY day
ORDER BY day;

-- Dashboard 2: Sleep duration and efficiency
WITH sleep AS (
  SELECT
    date(start_dt) AS day,
    value,
    SUM((julianday(end_dt) - julianday(start_dt)) * 24.0) AS hours
  FROM records_norm
  WHERE type = 'HKCategoryTypeIdentifierSleepAnalysis'
    AND value IN (
      'HKCategoryValueSleepAnalysisInBed',
      'HKCategoryValueSleepAnalysisAsleep',
      'HKCategoryValueSleepAnalysisAsleepUnspecified'
    )
  GROUP BY day, value
)
SELECT
  day,
  SUM(CASE WHEN value LIKE '%Asleep%' THEN hours END) AS asleep_hours,
  SUM(CASE WHEN value LIKE '%InBed%' THEN hours END) AS in_bed_hours,
  CASE
    WHEN SUM(CASE WHEN value LIKE '%InBed%' THEN hours END) > 0
    THEN SUM(CASE WHEN value LIKE '%Asleep%' THEN hours END)
         / SUM(CASE WHEN value LIKE '%InBed%' THEN hours END)
  END AS sleep_efficiency
FROM sleep
GROUP BY day
ORDER BY day;

-- Dashboard 3: Recovery signals (RHR, HRV, respiratory rate)
WITH daily AS (
  SELECT
    date(start_dt) AS day,
    AVG(CASE WHEN type = 'HKQuantityTypeIdentifierRestingHeartRate' THEN value_num END) AS rhr,
    AVG(CASE WHEN type = 'HKQuantityTypeIdentifierHeartRateVariabilitySDNN' THEN value_num END) AS hrv,
    AVG(CASE WHEN type = 'HKQuantityTypeIdentifierRespiratoryRate' THEN value_num END) AS resp_rate
  FROM records_norm
  WHERE type IN (
    'HKQuantityTypeIdentifierRestingHeartRate',
    'HKQuantityTypeIdentifierHeartRateVariabilitySDNN',
    'HKQuantityTypeIdentifierRespiratoryRate'
  )
  GROUP BY day
)
SELECT
  day,
  rhr,
  hrv,
  resp_rate,
  AVG(rhr) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rhr_7d,
  AVG(hrv) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS hrv_7d
FROM daily
ORDER BY day;

-- Dashboard 4: Weekly training load (workouts)
SELECT
  strftime('%Y-%W', start_dt) AS week,
  COUNT(*) AS workouts,
  SUM(duration) AS total_minutes,
  SUM(total_distance) AS total_distance,
  SUM(total_energy_burned) AS total_energy
FROM workouts_norm
GROUP BY week
ORDER BY week;

-- Dashboard 5: Mobility and running form metrics
SELECT
  date(start_dt) AS day,
  AVG(CASE WHEN type = 'HKQuantityTypeIdentifierWalkingSpeed' THEN value_num END) AS walking_speed,
  AVG(CASE WHEN type = 'HKQuantityTypeIdentifierWalkingStepLength' THEN value_num END) AS walking_step_length,
  AVG(CASE WHEN type = 'HKQuantityTypeIdentifierWalkingDoubleSupportPercentage' THEN value_num END) AS double_support_pct,
  AVG(CASE WHEN type = 'HKQuantityTypeIdentifierRunningSpeed' THEN value_num END) AS running_speed,
  AVG(CASE WHEN type = 'HKQuantityTypeIdentifierRunningStrideLength' THEN value_num END) AS running_stride_length,
  AVG(CASE WHEN type = 'HKQuantityTypeIdentifierRunningPower' THEN value_num END) AS running_power,
  AVG(CASE WHEN type = 'HKQuantityTypeIdentifierRunningVerticalOscillation' THEN value_num END) AS running_vertical_oscillation,
  AVG(CASE WHEN type = 'HKQuantityTypeIdentifierRunningGroundContactTime' THEN value_num END) AS running_ground_contact_time
FROM records_norm
WHERE type IN (
  'HKQuantityTypeIdentifierWalkingSpeed',
  'HKQuantityTypeIdentifierWalkingStepLength',
  'HKQuantityTypeIdentifierWalkingDoubleSupportPercentage',
  'HKQuantityTypeIdentifierRunningSpeed',
  'HKQuantityTypeIdentifierRunningStrideLength',
  'HKQuantityTypeIdentifierRunningPower',
  'HKQuantityTypeIdentifierRunningVerticalOscillation',
  'HKQuantityTypeIdentifierRunningGroundContactTime'
)
GROUP BY day
ORDER BY day;
