-- 1) Sleep midpoint by weekday (social jetlag)
WITH asleep AS (
  SELECT
    start_dt,
    end_dt
  FROM records_norm
  WHERE type = 'HKCategoryTypeIdentifierSleepAnalysis'
    AND value LIKE '%Asleep%'
),
nights AS (
  SELECT
    date(end_dt) AS sleep_date,
    MIN(start_dt) AS first_start,
    MAX(end_dt) AS last_end
  FROM asleep
  GROUP BY sleep_date
),
midpoints AS (
  SELECT
    sleep_date,
    datetime(
      julianday(first_start) + (julianday(last_end) - julianday(first_start)) / 2.0
    ) AS midpoint
  FROM nights
)
SELECT
  strftime('%w', sleep_date) AS weekday,
  ROUND(AVG((julianday(midpoint) - julianday(date(midpoint))) * 24.0), 2) AS midpoint_hour
FROM midpoints
GROUP BY weekday
ORDER BY weekday;

-- 2) Sleep efficiency vs next-day HRV and RHR
WITH sleep AS (
  SELECT
    date(start_dt) AS day,
    SUM(CASE WHEN value LIKE '%Asleep%' THEN (julianday(end_dt) - julianday(start_dt)) * 24.0 END) AS asleep_hours,
    SUM(CASE WHEN value LIKE '%InBed%' THEN (julianday(end_dt) - julianday(start_dt)) * 24.0 END) AS in_bed_hours
  FROM records_norm
  WHERE type = 'HKCategoryTypeIdentifierSleepAnalysis'
  GROUP BY day
),
recovery AS (
  SELECT
    date(start_dt) AS day,
    AVG(CASE WHEN type = 'HKQuantityTypeIdentifierHeartRateVariabilitySDNN' THEN value_num END) AS hrv,
    AVG(CASE WHEN type = 'HKQuantityTypeIdentifierRestingHeartRate' THEN value_num END) AS rhr
  FROM records_norm
  WHERE type IN (
    'HKQuantityTypeIdentifierHeartRateVariabilitySDNN',
    'HKQuantityTypeIdentifierRestingHeartRate'
  )
  GROUP BY day
)
SELECT
  sleep.day AS sleep_day,
  sleep.asleep_hours,
  sleep.in_bed_hours,
  ROUND(sleep.asleep_hours / NULLIF(sleep.in_bed_hours, 0), 3) AS sleep_efficiency,
  recovery.hrv AS next_day_hrv,
  recovery.rhr AS next_day_rhr
FROM sleep
LEFT JOIN recovery ON recovery.day = date(sleep.day, '+1 day')
ORDER BY sleep.day;

-- 3) Training load vs next-day HRV (active energy + workout duration)
WITH load AS (
  SELECT
    date(start_dt) AS day,
    SUM(CASE WHEN type = 'HKQuantityTypeIdentifierActiveEnergyBurned' THEN value_num ELSE 0 END) AS active_energy_kcal
  FROM records_norm
  WHERE type = 'HKQuantityTypeIdentifierActiveEnergyBurned'
  GROUP BY day
),
workout_load AS (
  SELECT
    date(start_dt) AS day,
    SUM(duration) AS workout_minutes
  FROM workouts_norm
  GROUP BY day
),
hrv AS (
  SELECT
    date(start_dt) AS day,
    AVG(value_num) AS hrv
  FROM records_norm
  WHERE type = 'HKQuantityTypeIdentifierHeartRateVariabilitySDNN'
  GROUP BY day
)
SELECT
  load.day,
  load.active_energy_kcal,
  workout_load.workout_minutes,
  hrv.hrv AS next_day_hrv
FROM load
LEFT JOIN workout_load ON workout_load.day = load.day
LEFT JOIN hrv ON hrv.day = date(load.day, '+1 day')
ORDER BY load.day;

-- 4) Oura vs Apple Watch bias for resting heart rate
WITH daily AS (
  SELECT
    date(start_dt) AS day,
    source_name_norm,
    AVG(value_num) AS rhr
  FROM records_norm
  WHERE type = 'HKQuantityTypeIdentifierRestingHeartRate'
    AND source_name_norm IN ('Oura', 'Colin''s Apple Watch')
  GROUP BY day, source_name_norm
),
paired AS (
  SELECT
    day,
    MAX(CASE WHEN source_name_norm = 'Oura' THEN rhr END) AS oura_rhr,
    MAX(CASE WHEN source_name_norm = 'Colin''s Apple Watch' THEN rhr END) AS watch_rhr
  FROM daily
  GROUP BY day
)
SELECT
  day,
  oura_rhr,
  watch_rhr,
  ROUND(oura_rhr - watch_rhr, 3) AS delta
FROM paired
WHERE oura_rhr IS NOT NULL AND watch_rhr IS NOT NULL
ORDER BY day;

-- 5) Audio exposure vs sleep duration
WITH audio AS (
  SELECT
    date(start_dt) AS day,
    SUM(CASE WHEN type = 'HKQuantityTypeIdentifierEnvironmentalAudioExposure' THEN value_num ELSE 0 END) AS env_audio,
    SUM(CASE WHEN type = 'HKQuantityTypeIdentifierHeadphoneAudioExposure' THEN value_num ELSE 0 END) AS headphone_audio
  FROM records_norm
  WHERE type IN (
    'HKQuantityTypeIdentifierEnvironmentalAudioExposure',
    'HKQuantityTypeIdentifierHeadphoneAudioExposure'
  )
  GROUP BY day
),
sleep AS (
  SELECT
    date(start_dt) AS day,
    SUM(CASE WHEN value LIKE '%Asleep%' THEN (julianday(end_dt) - julianday(start_dt)) * 24.0 END) AS asleep_hours
  FROM records_norm
  WHERE type = 'HKCategoryTypeIdentifierSleepAnalysis'
  GROUP BY day
)
SELECT
  audio.day,
  audio.env_audio,
  audio.headphone_audio,
  sleep.asleep_hours
FROM audio
LEFT JOIN sleep ON sleep.day = audio.day
ORDER BY audio.day;

-- 6) Time in daylight vs sleep duration
WITH daylight AS (
  SELECT
    date(start_dt) AS day,
    SUM(value_num) AS daylight_minutes
  FROM records_norm
  WHERE type = 'HKQuantityTypeIdentifierTimeInDaylight'
  GROUP BY day
),
sleep AS (
  SELECT
    date(start_dt) AS day,
    SUM(CASE WHEN value LIKE '%Asleep%' THEN (julianday(end_dt) - julianday(start_dt)) * 24.0 END) AS asleep_hours
  FROM records_norm
  WHERE type = 'HKCategoryTypeIdentifierSleepAnalysis'
  GROUP BY day
)
SELECT
  daylight.day,
  daylight.daylight_minutes,
  sleep.asleep_hours
FROM daylight
LEFT JOIN sleep ON sleep.day = daylight.day
ORDER BY daylight.day;

-- 7) Running economy (speed vs power, stride, vertical oscillation)
SELECT
  date(start_dt) AS day,
  AVG(CASE WHEN type = 'HKQuantityTypeIdentifierRunningSpeed' THEN value_num END) AS running_speed,
  AVG(CASE WHEN type = 'HKQuantityTypeIdentifierRunningPower' THEN value_num END) AS running_power,
  AVG(CASE WHEN type = 'HKQuantityTypeIdentifierRunningStrideLength' THEN value_num END) AS stride_length,
  AVG(CASE WHEN type = 'HKQuantityTypeIdentifierRunningVerticalOscillation' THEN value_num END) AS vertical_oscillation,
  AVG(CASE WHEN type = 'HKQuantityTypeIdentifierRunningGroundContactTime' THEN value_num END) AS ground_contact_time
FROM records_norm
WHERE type IN (
  'HKQuantityTypeIdentifierRunningSpeed',
  'HKQuantityTypeIdentifierRunningPower',
  'HKQuantityTypeIdentifierRunningStrideLength',
  'HKQuantityTypeIdentifierRunningVerticalOscillation',
  'HKQuantityTypeIdentifierRunningGroundContactTime'
)
GROUP BY day
ORDER BY day;

-- 8) Walking stability vs activity volume
WITH walking AS (
  SELECT
    date(start_dt) AS day,
    AVG(CASE WHEN type = 'HKQuantityTypeIdentifierWalkingAsymmetryPercentage' THEN value_num END) AS asymmetry_pct,
    AVG(CASE WHEN type = 'HKQuantityTypeIdentifierWalkingDoubleSupportPercentage' THEN value_num END) AS double_support_pct,
    AVG(CASE WHEN type = 'HKQuantityTypeIdentifierWalkingSpeed' THEN value_num END) AS walking_speed
  FROM records_norm
  WHERE type IN (
    'HKQuantityTypeIdentifierWalkingAsymmetryPercentage',
    'HKQuantityTypeIdentifierWalkingDoubleSupportPercentage',
    'HKQuantityTypeIdentifierWalkingSpeed'
  )
  GROUP BY day
),
steps AS (
  SELECT
    date(start_dt) AS day,
    SUM(value_num) AS steps
  FROM records_norm
  WHERE type = 'HKQuantityTypeIdentifierStepCount'
  GROUP BY day
)
SELECT
  walking.day,
  walking.asymmetry_pct,
  walking.double_support_pct,
  walking.walking_speed,
  steps.steps
FROM walking
LEFT JOIN steps ON steps.day = walking.day
ORDER BY walking.day;

-- 9) VO2Max vs weekly training volume
WITH vo2 AS (
  SELECT
    strftime('%Y-%W', start_dt) AS week,
    AVG(value_num) AS vo2max
  FROM records_norm
  WHERE type = 'HKQuantityTypeIdentifierVO2Max'
  GROUP BY week
),
volume AS (
  SELECT
    strftime('%Y-%W', start_dt) AS week,
    SUM(duration) AS workout_minutes,
    SUM(total_distance) AS total_distance
  FROM workouts_norm
  GROUP BY week
)
SELECT
  vo2.week,
  vo2.vo2max,
  volume.workout_minutes,
  volume.total_distance
FROM vo2
LEFT JOIN volume ON volume.week = vo2.week
ORDER BY vo2.week;

-- 10) Heart rate recovery vs workout intensity
WITH recovery AS (
  SELECT
    date(start_dt) AS day,
    AVG(value_num) AS hr_recovery_1min
  FROM records_norm
  WHERE type = 'HKQuantityTypeIdentifierHeartRateRecoveryOneMinute'
  GROUP BY day
),
workout_intensity AS (
  SELECT
    date(start_dt) AS day,
    AVG(total_energy_burned / NULLIF(duration, 0)) AS energy_per_min
  FROM workouts_norm
  GROUP BY day
)
SELECT
  recovery.day,
  recovery.hr_recovery_1min,
  workout_intensity.energy_per_min
FROM recovery
LEFT JOIN workout_intensity ON workout_intensity.day = recovery.day
ORDER BY recovery.day;

-- 11) ECG classification counts
SELECT
  classification,
  COUNT(*) AS count
FROM ecg_records
GROUP BY classification
ORDER BY count DESC;

-- 12) Route distance vs workout distance (closest workout by time)
WITH closest AS (
  SELECT
    r.id AS route_id,
    r.file_path,
    r.distance_km,
    w.id AS workout_id,
    w.total_distance,
    w.start_dt,
    r.start_time,
    ABS(julianday(w.start_dt) - julianday(substr(r.start_time, 1, 19))) AS time_diff_days
  FROM workout_routes r
  JOIN workouts_norm w
    ON date(w.start_dt) = date(substr(r.start_time, 1, 19))
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY time_diff_days) AS rn
  FROM closest
)
SELECT
  route_id,
  file_path,
  distance_km,
  workout_id,
  total_distance,
  start_time,
  start_dt
FROM ranked
WHERE rn = 1
ORDER BY route_id;

-- 13) Recovery score vs 28-day baseline (higher is better)
WITH daily AS (
  SELECT
    date(start_dt) AS day,
    AVG(CASE WHEN type = 'HKQuantityTypeIdentifierRestingHeartRate' THEN value_num END) AS rhr,
    AVG(CASE WHEN type = 'HKQuantityTypeIdentifierHeartRateVariabilitySDNN' THEN value_num END) AS hrv,
    AVG(CASE WHEN type = 'HKQuantityTypeIdentifierRespiratoryRate' THEN value_num END) AS resp,
    SUM(CASE WHEN type = 'HKCategoryTypeIdentifierSleepAnalysis' AND value LIKE '%Asleep%'
      THEN (julianday(end_dt) - julianday(start_dt)) * 24.0 END) AS sleep_hours
  FROM records_norm
  WHERE type IN (
    'HKQuantityTypeIdentifierRestingHeartRate',
    'HKQuantityTypeIdentifierHeartRateVariabilitySDNN',
    'HKQuantityTypeIdentifierRespiratoryRate',
    'HKCategoryTypeIdentifierSleepAnalysis'
  )
  GROUP BY day
),
baseline AS (
  SELECT
    day,
    rhr,
    hrv,
    resp,
    sleep_hours,
    AVG(rhr) OVER (ORDER BY day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS rhr_28,
    AVG(hrv) OVER (ORDER BY day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS hrv_28,
    AVG(resp) OVER (ORDER BY day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS resp_28,
    AVG(sleep_hours) OVER (ORDER BY day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS sleep_28
  FROM daily
)
SELECT
  day,
  rhr,
  hrv,
  resp,
  sleep_hours,
  ROUND(
    (
      (rhr_28 - rhr) / NULLIF(rhr_28, 0) +
      (hrv - hrv_28) / NULLIF(hrv_28, 0) +
      (resp_28 - resp) / NULLIF(resp_28, 0) +
      (sleep_hours - sleep_28) / NULLIF(sleep_28, 0)
    ) / 4.0,
    4
  ) AS recovery_score
FROM baseline
WHERE rhr_28 IS NOT NULL AND hrv_28 IS NOT NULL AND resp_28 IS NOT NULL AND sleep_28 IS NOT NULL
ORDER BY day;

-- 14) Training monotony and strain (weekly load variability)
WITH daily AS (
  SELECT
    date(start_dt) AS day,
    SUM(CASE WHEN type = 'HKQuantityTypeIdentifierActiveEnergyBurned' THEN value_num ELSE 0 END) AS energy_kcal
  FROM records_norm
  WHERE type = 'HKQuantityTypeIdentifierActiveEnergyBurned'
  GROUP BY day
),
workouts AS (
  SELECT
    date(start_dt) AS day,
    SUM(duration) AS workout_minutes
  FROM workouts_norm
  GROUP BY day
),
load AS (
  SELECT
    daily.day,
    daily.energy_kcal,
    workouts.workout_minutes,
    (daily.energy_kcal + COALESCE(workouts.workout_minutes, 0)) AS load_score
  FROM daily
  LEFT JOIN workouts ON workouts.day = daily.day
),
weekly AS (
  SELECT
    strftime('%Y-%W', day) AS week,
    AVG(load_score) AS mean_load,
    AVG(load_score * load_score) AS mean_sq_load,
    SUM(load_score) AS total_load
  FROM load
  GROUP BY week
)
SELECT
  week,
  mean_load,
  ROUND(SQRT(mean_sq_load - mean_load * mean_load), 4) AS load_stddev,
  ROUND(mean_load / NULLIF(SQRT(mean_sq_load - mean_load * mean_load), 0), 4) AS monotony,
  total_load,
  ROUND(
    (mean_load / NULLIF(SQRT(mean_sq_load - mean_load * mean_load), 0)) * total_load,
    2
  ) AS strain
FROM weekly
ORDER BY week;

-- 15) Time-in-zone per workout (counts of HR samples per zone)
WITH hr AS (
  SELECT
    w.id AS workout_id,
    r.value_num AS hr
  FROM records_norm r
  JOIN workouts_norm w
    ON r.start_dt BETWEEN w.start_dt AND w.end_dt
  WHERE r.type = 'HKQuantityTypeIdentifierHeartRate'
),
zones AS (
  SELECT
    workout_id,
    SUM(CASE WHEN hr < 120 THEN 1 ELSE 0 END) AS z1_samples,
    SUM(CASE WHEN hr >= 120 AND hr < 140 THEN 1 ELSE 0 END) AS z2_samples,
    SUM(CASE WHEN hr >= 140 AND hr < 160 THEN 1 ELSE 0 END) AS z3_samples,
    SUM(CASE WHEN hr >= 160 AND hr < 180 THEN 1 ELSE 0 END) AS z4_samples,
    SUM(CASE WHEN hr >= 180 THEN 1 ELSE 0 END) AS z5_samples
  FROM hr
  GROUP BY workout_id
)
SELECT
  w.id AS workout_id,
  w.workout_activity_type,
  w.start_dt,
  z.z1_samples,
  z.z2_samples,
  z.z3_samples,
  z.z4_samples,
  z.z5_samples
FROM workouts_norm w
LEFT JOIN zones z ON z.workout_id = w.id
ORDER BY w.start_dt;

-- 16) Oura vs Apple Watch bias for HRV and respiratory rate
WITH daily AS (
  SELECT
    date(start_dt) AS day,
    source_name_norm,
    AVG(CASE WHEN type = 'HKQuantityTypeIdentifierHeartRateVariabilitySDNN' THEN value_num END) AS hrv,
    AVG(CASE WHEN type = 'HKQuantityTypeIdentifierRespiratoryRate' THEN value_num END) AS resp
  FROM records_norm
  WHERE type IN (
    'HKQuantityTypeIdentifierHeartRateVariabilitySDNN',
    'HKQuantityTypeIdentifierRespiratoryRate'
  )
    AND source_name_norm IN ('Oura', 'Colin''s Apple Watch')
  GROUP BY day, source_name_norm
),
paired AS (
  SELECT
    day,
    MAX(CASE WHEN source_name_norm = 'Oura' THEN hrv END) AS oura_hrv,
    MAX(CASE WHEN source_name_norm = 'Colin''s Apple Watch' THEN hrv END) AS watch_hrv,
    MAX(CASE WHEN source_name_norm = 'Oura' THEN resp END) AS oura_resp,
    MAX(CASE WHEN source_name_norm = 'Colin''s Apple Watch' THEN resp END) AS watch_resp
  FROM daily
  GROUP BY day
)
SELECT
  day,
  oura_hrv,
  watch_hrv,
  ROUND(oura_hrv - watch_hrv, 3) AS hrv_delta,
  oura_resp,
  watch_resp,
  ROUND(oura_resp - watch_resp, 3) AS resp_delta
FROM paired
WHERE (oura_hrv IS NOT NULL AND watch_hrv IS NOT NULL)
   OR (oura_resp IS NOT NULL AND watch_resp IS NOT NULL)
ORDER BY day;

-- 17) Caffeine and calorie intake vs sleep and next-day RHR
WITH intake AS (
  SELECT
    date(start_dt) AS day,
    SUM(CASE WHEN type = 'HKQuantityTypeIdentifierDietaryCaffeine' THEN value_num ELSE 0 END) AS caffeine_mg,
    SUM(CASE WHEN type = 'HKQuantityTypeIdentifierDietaryEnergyConsumed' THEN value_num ELSE 0 END) AS calories
  FROM records_norm
  WHERE type IN (
    'HKQuantityTypeIdentifierDietaryCaffeine',
    'HKQuantityTypeIdentifierDietaryEnergyConsumed'
  )
  GROUP BY day
),
sleep AS (
  SELECT
    date(start_dt) AS day,
    SUM(CASE WHEN value LIKE '%Asleep%' THEN (julianday(end_dt) - julianday(start_dt)) * 24.0 END) AS asleep_hours
  FROM records_norm
  WHERE type = 'HKCategoryTypeIdentifierSleepAnalysis'
  GROUP BY day
),
rhr AS (
  SELECT
    date(start_dt) AS day,
    AVG(value_num) AS rhr
  FROM records_norm
  WHERE type = 'HKQuantityTypeIdentifierRestingHeartRate'
  GROUP BY day
)
SELECT
  intake.day,
  intake.caffeine_mg,
  intake.calories,
  sleep.asleep_hours,
  rhr.rhr AS next_day_rhr
FROM intake
LEFT JOIN sleep ON sleep.day = intake.day
LEFT JOIN rhr ON rhr.day = date(intake.day, '+1 day')
ORDER BY intake.day;

-- 18) ECG timing vs nearest workout and intensity
WITH nearest AS (
  SELECT
    e.id AS ecg_id,
    e.recorded_date,
    e.classification,
    e.symptoms,
    w.id AS workout_id,
    w.workout_activity_type,
    w.total_energy_burned,
    w.duration,
    ABS(julianday(substr(e.recorded_date, 1, 19)) - julianday(w.start_dt)) * 24.0 AS diff_hours
  FROM ecg_records e
  LEFT JOIN workouts_norm w
    ON date(w.start_dt) = date(substr(e.recorded_date, 1, 19))
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ecg_id ORDER BY diff_hours) AS rn
  FROM nearest
)
SELECT
  ecg_id,
  recorded_date,
  classification,
  symptoms,
  workout_id,
  workout_activity_type,
  total_energy_burned,
  duration,
  ROUND(diff_hours, 2) AS diff_hours
FROM ranked
WHERE rn = 1
ORDER BY recorded_date;

-- 19) Sleep duration by source (compare devices/apps)
WITH sleep AS (
  SELECT
    date(start_dt) AS day,
    source_name_norm,
    SUM(CASE WHEN value LIKE '%Asleep%' THEN (julianday(end_dt) - julianday(start_dt)) * 24.0 END) AS asleep_hours
  FROM records_norm
  WHERE type = 'HKCategoryTypeIdentifierSleepAnalysis'
  GROUP BY day, source_name_norm
)
SELECT
  day,
  source_name_norm,
  asleep_hours
FROM sleep
WHERE asleep_hours IS NOT NULL
ORDER BY day, source_name_norm;
