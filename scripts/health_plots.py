#!/usr/bin/env python3
import argparse
import os
import sqlite3
from datetime import datetime

os.environ.setdefault("MPLCONFIGDIR", os.path.join(os.getcwd(), ".mplconfig"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate plots from health.sqlite (requires postprocess views)."
    )
    parser.add_argument(
        "--db",
        default="health.sqlite",
        help="SQLite DB path (default: health.sqlite)",
    )
    parser.add_argument(
        "--out-dir",
        default="plots",
        help="Output directory for images (default: plots)",
    )
    return parser.parse_args()


PALETTE = {
    "blue": "#3b82f6",
    "teal": "#14b8a6",
    "orange": "#f97316",
    "purple": "#8b5cf6",
    "pink": "#ec4899",
    "green": "#10b981",
    "gray": "#64748b",
}


def ensure_views(conn):
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view' AND name='records_norm'"
    )
    if cursor.fetchone() is None:
        raise RuntimeError(
            "records_norm view missing. Run scripts/health_postprocess.py first."
        )


def fetch_rows(conn, sql, params=None):
    cur = conn.execute(sql, params or ())
    return cur.fetchall()


def to_dates(rows):
    return [datetime.strptime(row[0], "%Y-%m-%d") for row in rows]


def rolling_avg(values, window):
    if not values:
        return []
    out = []
    buf = []
    total = 0.0
    for value in values:
        value = 0.0 if value is None else float(value)
        buf.append(value)
        total += value
        if len(buf) > window:
            total -= buf.pop(0)
        out.append(total / len(buf))
    return out


def format_time_axis(ax):
    locator = mdates.AutoDateLocator(minticks=6, maxticks=12)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    ax.grid(True, axis="y", alpha=0.2)


def save_fig(fig, path):
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)


def plot_daily_activity(conn, out_dir):
    rows = fetch_rows(
        conn,
        """
        SELECT
          date(start_dt) AS day,
          COALESCE(SUM(CASE WHEN type = 'HKQuantityTypeIdentifierStepCount' THEN value_num END), 0) AS steps,
          COALESCE(SUM(CASE WHEN type = 'HKQuantityTypeIdentifierActiveEnergyBurned' THEN value_num END), 0) AS active_kcal
        FROM records_norm
        WHERE type IN (
          'HKQuantityTypeIdentifierStepCount',
          'HKQuantityTypeIdentifierActiveEnergyBurned'
        )
        GROUP BY day
        ORDER BY day
        """,
    )
    if not rows:
        return None
    dates = to_dates(rows)
    steps = [row[1] for row in rows]
    active = [row[2] for row in rows]

    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
    axes[0].plot(dates, steps, color=PALETTE["blue"], alpha=0.25, linewidth=0.8)
    axes[0].plot(dates, rolling_avg(steps, 7), color=PALETTE["blue"], linewidth=2)
    axes[0].set_title("Daily Steps (7-day avg)")
    axes[0].set_ylabel("Steps")
    format_time_axis(axes[0])

    axes[1].plot(dates, active, color=PALETTE["orange"], alpha=0.25, linewidth=0.8)
    axes[1].plot(dates, rolling_avg(active, 7), color=PALETTE["orange"], linewidth=2)
    axes[1].set_title("Active Energy (7-day avg)")
    axes[1].set_ylabel("kcal")
    format_time_axis(axes[1])

    path = os.path.join(out_dir, "daily_activity.png")
    save_fig(fig, path)
    return path


def plot_sleep_efficiency(conn, out_dir):
    rows = fetch_rows(
        conn,
        """
        SELECT
          date(start_dt) AS day,
          SUM(CASE WHEN value LIKE '%Asleep%' THEN (julianday(end_dt) - julianday(start_dt)) * 24.0 END) AS asleep_hours,
          SUM(CASE WHEN value LIKE '%InBed%' THEN (julianday(end_dt) - julianday(start_dt)) * 24.0 END) AS in_bed_hours
        FROM records_norm
        WHERE type = 'HKCategoryTypeIdentifierSleepAnalysis'
        GROUP BY day
        ORDER BY day
        """,
    )
    if not rows:
        return None
    dates = to_dates(rows)
    asleep = [row[1] for row in rows]
    in_bed = [row[2] for row in rows]
    efficiency = [
        (a / b) if a is not None and b else None for a, b in zip(asleep, in_bed)
    ]

    fig, ax = plt.subplots(figsize=(12, 4.5))
    ax.plot(dates, asleep, color=PALETTE["teal"], alpha=0.25, linewidth=0.8)
    ax.plot(dates, rolling_avg(asleep, 7), color=PALETTE["teal"], linewidth=2)
    ax.set_ylabel("Hours asleep")
    ax.set_title("Sleep Duration & Efficiency")
    format_time_axis(ax)

    ax2 = ax.twinx()
    ax2.plot(dates, efficiency, color=PALETTE["purple"], alpha=0.35, linewidth=1.2)
    ax2.set_ylabel("Efficiency")
    ax2.set_ylim(0, 1.1)

    path = os.path.join(out_dir, "sleep_duration_efficiency.png")
    save_fig(fig, path)
    return path


def plot_recovery_score(conn, out_dir):
    rows = fetch_rows(
        conn,
        """
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
        ORDER BY day
        """,
    )
    if not rows:
        return None
    dates = to_dates(rows)
    scores = [row[1] for row in rows]

    fig, ax = plt.subplots(figsize=(12, 4.5))
    ax.plot(dates, scores, color=PALETTE["green"], alpha=0.25, linewidth=0.8)
    ax.plot(dates, rolling_avg(scores, 7), color=PALETTE["green"], linewidth=2)
    ax.axhline(0, color=PALETTE["gray"], linewidth=1, alpha=0.5)
    ax.set_title("Recovery Score (28-day baseline)")
    ax.set_ylabel("Score")
    format_time_axis(ax)

    path = os.path.join(out_dir, "recovery_score.png")
    save_fig(fig, path)
    return path


def plot_social_jetlag(conn, out_dir):
    rows = fetch_rows(
        conn,
        """
        WITH asleep AS (
          SELECT start_dt, end_dt
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
          AVG((julianday(midpoint) - julianday(date(midpoint))) * 24.0) AS midpoint_hour
        FROM midpoints
        GROUP BY weekday
        ORDER BY weekday
        """,
    )
    if not rows:
        return None
    weekdays = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    x_labels = [weekdays[int(row[0])] for row in rows]
    values = [row[1] for row in rows]

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.bar(x_labels, values, color=PALETTE["pink"], alpha=0.85)
    ax.set_title("Average Sleep Midpoint by Weekday")
    ax.set_ylabel("Midpoint hour")
    ax.grid(True, axis="y", alpha=0.2)

    path = os.path.join(out_dir, "social_jetlag.png")
    save_fig(fig, path)
    return path


def plot_monotony_strain(conn, out_dir):
    rows = fetch_rows(
        conn,
        """
        WITH daily AS (
          SELECT
            date(start_dt) AS day,
            COALESCE(SUM(CASE WHEN type = 'HKQuantityTypeIdentifierActiveEnergyBurned' THEN value_num END), 0) AS energy_kcal
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
          SQRT(mean_sq_load - mean_load * mean_load) AS load_stddev,
          mean_load / NULLIF(SQRT(mean_sq_load - mean_load * mean_load), 0) AS monotony,
          total_load,
          (mean_load / NULLIF(SQRT(mean_sq_load - mean_load * mean_load), 0)) * total_load AS strain
        FROM weekly
        ORDER BY week
        """,
    )
    if not rows:
        return None
    weeks = [row[0] for row in rows]
    monotony = [row[3] for row in rows]
    strain = [row[5] for row in rows]

    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
    axes[0].plot(weeks, monotony, color=PALETTE["orange"], linewidth=1.8)
    axes[0].set_title("Training Monotony (weekly)")
    axes[0].set_ylabel("Monotony")
    axes[0].grid(True, axis="y", alpha=0.2)

    axes[1].plot(weeks, strain, color=PALETTE["purple"], linewidth=1.8)
    axes[1].set_title("Training Strain (weekly)")
    axes[1].set_ylabel("Strain")
    axes[1].grid(True, axis="y", alpha=0.2)
    axes[1].tick_params(axis="x", rotation=45)

    path = os.path.join(out_dir, "training_monotony_strain.png")
    save_fig(fig, path)
    return path


def plot_oura_watch_bias(conn, out_dir):
    rows = fetch_rows(
        conn,
        """
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
          oura_rhr - watch_rhr AS delta
        FROM paired
        WHERE oura_rhr IS NOT NULL AND watch_rhr IS NOT NULL
        ORDER BY day
        """,
    )
    if not rows:
        return None
    dates = to_dates(rows)
    deltas = [row[1] for row in rows]

    fig, ax = plt.subplots(figsize=(12, 4.5))
    ax.plot(dates, deltas, color=PALETTE["blue"], alpha=0.25, linewidth=0.8)
    ax.plot(dates, rolling_avg(deltas, 14), color=PALETTE["blue"], linewidth=2)
    ax.axhline(0, color=PALETTE["gray"], linewidth=1, alpha=0.5)
    ax.set_title("Oura vs Apple Watch Resting HR Bias")
    ax.set_ylabel("Oura - Watch (bpm)")
    format_time_axis(ax)

    path = os.path.join(out_dir, "oura_watch_rhr_bias.png")
    save_fig(fig, path)
    return path


def plot_daylight_sleep(conn, out_dir):
    rows = fetch_rows(
        conn,
        """
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
        WHERE daylight.daylight_minutes IS NOT NULL
        ORDER BY daylight.day
        """,
    )
    if not rows:
        return None
    daylight = [row[1] for row in rows]
    asleep = [row[2] for row in rows]

    fig, ax = plt.subplots(figsize=(6.5, 5.5))
    ax.scatter(daylight, asleep, color=PALETTE["teal"], alpha=0.5, s=16)
    ax.set_title("Time in Daylight vs Sleep Duration")
    ax.set_xlabel("Daylight minutes")
    ax.set_ylabel("Asleep hours")
    ax.grid(True, alpha=0.2)

    path = os.path.join(out_dir, "daylight_vs_sleep.png")
    save_fig(fig, path)
    return path


def plot_audio_sleep(conn, out_dir):
    rows = fetch_rows(
        conn,
        """
        WITH audio AS (
          SELECT
            date(start_dt) AS day,
            AVG(CASE WHEN type = 'HKQuantityTypeIdentifierEnvironmentalAudioExposure' THEN value_num END) AS env_audio,
            AVG(CASE WHEN type = 'HKQuantityTypeIdentifierHeadphoneAudioExposure' THEN value_num END) AS headphone_audio
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
          audio.headphone_audio,
          sleep.asleep_hours
        FROM audio
        LEFT JOIN sleep ON sleep.day = audio.day
        WHERE audio.headphone_audio IS NOT NULL
        ORDER BY audio.day
        """,
    )
    if not rows:
        return None
    headphone = [row[1] for row in rows]
    asleep = [row[2] for row in rows]

    fig, ax = plt.subplots(figsize=(6.5, 5.5))
    ax.scatter(headphone, asleep, color=PALETTE["orange"], alpha=0.5, s=16)
    ax.set_title("Headphone Audio Exposure vs Sleep Duration")
    ax.set_xlabel("Headphone audio (avg dB)")
    ax.set_ylabel("Asleep hours")
    ax.grid(True, alpha=0.2)

    path = os.path.join(out_dir, "audio_vs_sleep.png")
    save_fig(fig, path)
    return path


def main():
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    plt.style.use("seaborn-v0_8")
    conn = sqlite3.connect(args.db)
    try:
        ensure_views(conn)
        outputs = [
            plot_daily_activity(conn, args.out_dir),
            plot_sleep_efficiency(conn, args.out_dir),
            plot_recovery_score(conn, args.out_dir),
            plot_social_jetlag(conn, args.out_dir),
            plot_monotony_strain(conn, args.out_dir),
            plot_oura_watch_bias(conn, args.out_dir),
            plot_daylight_sleep(conn, args.out_dir),
            plot_audio_sleep(conn, args.out_dir),
        ]
    finally:
        conn.close()

    outputs = [path for path in outputs if path]
    for path in outputs:
        print(f"Wrote {path}")


if __name__ == "__main__":
    raise SystemExit(main())
