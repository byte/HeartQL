#!/usr/bin/env python3
import argparse
import os
import sqlite3
from datetime import datetime

import numpy as np

os.environ.setdefault("MPLCONFIGDIR", os.path.join(os.getcwd(), ".mplconfig"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate social-ready insights from health.sqlite."
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
    "ink": "#0f172a",
    "slate": "#475569",
    "muted": "#94a3b8",
    "blue": "#2563eb",
    "teal": "#14b8a6",
    "orange": "#f97316",
    "pink": "#ec4899",
    "green": "#10b981",
    "gold": "#f59e0b",
    "purple": "#7c3aed",
}


def style():
    plt.rcParams.update(
        {
            "figure.facecolor": "#f8fafc",
            "axes.facecolor": "#ffffff",
            "axes.edgecolor": "#e2e8f0",
            "axes.labelcolor": PALETTE["ink"],
            "text.color": PALETTE["ink"],
            "axes.titleweight": "bold",
            "axes.titlesize": 18,
            "axes.labelsize": 12,
            "xtick.color": PALETTE["slate"],
            "ytick.color": PALETTE["slate"],
            "grid.color": "#e2e8f0",
            "grid.alpha": 0.6,
            "font.size": 12,
        }
    )


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


def save_fig(fig, path):
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def to_dates(rows):
    return [datetime.strptime(row[0], "%Y-%m-%d") for row in rows]


def plot_chronotype_heatmap(conn, out_dir):
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
          strftime('%Y', sleep_date) AS year,
          strftime('%w', sleep_date) AS weekday,
          AVG((julianday(midpoint) - julianday(date(midpoint))) * 24.0) AS midpoint_hour
        FROM midpoints
        GROUP BY year, weekday
        ORDER BY year, weekday
        """,
    )
    if not rows:
        return None

    years = sorted({int(row[0]) for row in rows})
    year_index = {year: idx for idx, year in enumerate(years)}
    data = np.full((len(years), 7), np.nan)
    for year, weekday, midpoint in rows:
        y = year_index[int(year)]
        x = int(weekday)
        data[y, x] = midpoint

    fig, ax = plt.subplots(figsize=(10, 7))
    im = ax.imshow(data, aspect="auto", cmap="cividis", vmin=0, vmax=24)
    ax.set_xticks(range(7))
    ax.set_xticklabels(["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"])
    ax.set_yticks(range(len(years)))
    ax.set_yticklabels(years)
    ax.set_title("Chronotype Map: Sleep Midpoint by Weekday and Year")
    ax.set_ylabel("Year")
    ax.set_xlabel("Weekday")
    cbar = fig.colorbar(im, ax=ax, shrink=0.8)
    cbar.set_label("Midpoint hour")
    path = os.path.join(out_dir, "chronotype_heatmap.png")
    save_fig(fig, path)
    return path


def plot_load_recovery_quadrants(conn, out_dir):
    rows = fetch_rows(
        conn,
        """
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
        recovery AS (
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
          FROM (
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
            FROM (
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
            )
          )
          WHERE rhr_28 IS NOT NULL AND hrv_28 IS NOT NULL AND resp_28 IS NOT NULL AND sleep_28 IS NOT NULL
        )
        SELECT
          load.day,
          load.load_score,
          recovery.recovery_score
        FROM load
        JOIN recovery ON recovery.day = load.day
        ORDER BY load.day
        """,
    )
    if not rows:
        return None

    days = [row[0] for row in rows]
    loads = np.array([row[1] for row in rows], dtype=float)
    recovery = np.array([row[2] for row in rows], dtype=float)

    years = np.array([int(day[:4]) for day in days])
    year_min = years.min()
    year_max = years.max()
    colors = (years - year_min) / max(year_max - year_min, 1)

    load_median = float(np.nanmedian(loads))

    fig, ax = plt.subplots(figsize=(10, 7))
    scatter = ax.scatter(
        loads,
        recovery,
        c=colors,
        cmap="viridis",
        alpha=0.6,
        edgecolors="none",
        s=22,
    )
    ax.axvline(load_median, color=PALETTE["muted"], linestyle="--", linewidth=1.2)
    ax.axhline(0, color=PALETTE["muted"], linestyle="--", linewidth=1.2)
    ax.set_title("Training Load vs Recovery (Quadrants)")
    ax.set_xlabel("Daily load score (kcal + workout minutes)")
    ax.set_ylabel("Recovery score (28-day baseline)")
    ax.grid(True, alpha=0.4)
    cbar = fig.colorbar(scatter, ax=ax, shrink=0.8)
    cbar.set_label("Year (normalized)")

    # Quadrant labels
    x_min, x_max = ax.get_xlim()
    y_min, y_max = ax.get_ylim()
    ax.text(
        load_median + (x_max - x_min) * 0.05,
        y_max * 0.8,
        "High load / High recovery",
        color=PALETTE["green"],
        fontsize=10,
    )
    ax.text(
        load_median + (x_max - x_min) * 0.05,
        y_min + (y_max - y_min) * 0.15,
        "High load / Low recovery",
        color=PALETTE["orange"],
        fontsize=10,
    )
    ax.text(
        x_min + (x_max - x_min) * 0.05,
        y_max * 0.8,
        "Low load / High recovery",
        color=PALETTE["teal"],
        fontsize=10,
    )
    ax.text(
        x_min + (x_max - x_min) * 0.05,
        y_min + (y_max - y_min) * 0.15,
        "Low load / Low recovery",
        color=PALETTE["slate"],
        fontsize=10,
    )

    path = os.path.join(out_dir, "load_vs_recovery_quadrants.png")
    save_fig(fig, path)
    return path


def plot_gait_signature(conn, out_dir):
    rows = fetch_rows(
        conn,
        """
        WITH steps AS (
          SELECT date(start_dt) AS day, SUM(value_num) AS steps
          FROM records_norm
          WHERE type = 'HKQuantityTypeIdentifierStepCount'
          GROUP BY day
        ),
        gait AS (
          SELECT
            date(start_dt) AS day,
            AVG(CASE WHEN type = 'HKQuantityTypeIdentifierWalkingAsymmetryPercentage' THEN value_num END) AS asymmetry,
            AVG(CASE WHEN type = 'HKQuantityTypeIdentifierWalkingSpeed' THEN value_num END) AS speed
          FROM records_norm
          WHERE type IN (
            'HKQuantityTypeIdentifierWalkingAsymmetryPercentage',
            'HKQuantityTypeIdentifierWalkingSpeed'
          )
          GROUP BY day
        )
        SELECT
          gait.day,
          steps.steps,
          gait.asymmetry,
          gait.speed
        FROM gait
        JOIN steps ON steps.day = gait.day
        WHERE gait.asymmetry IS NOT NULL AND gait.speed IS NOT NULL
        ORDER BY gait.day
        """,
    )
    if not rows:
        return None

    steps = np.array([row[1] for row in rows], dtype=float)
    asymmetry = np.array([row[2] for row in rows], dtype=float)
    speed = np.array([row[3] for row in rows], dtype=float)

    log_steps = np.log10(steps + 1)
    if len(log_steps) > 1:
        corr = np.corrcoef(log_steps, asymmetry)[0, 1]
    else:
        corr = float("nan")

    fig, ax = plt.subplots(figsize=(10, 7))
    scatter = ax.scatter(
        steps,
        asymmetry,
        c=speed,
        cmap="plasma",
        alpha=0.5,
        s=20,
        edgecolors="none",
    )
    ax.set_xscale("log")
    ax.set_title("Gait Signature: Steps vs Asymmetry (color = walking speed)")
    ax.set_xlabel("Daily steps (log scale)")
    ax.set_ylabel("Walking asymmetry (%)")
    ax.grid(True, alpha=0.35)
    cbar = fig.colorbar(scatter, ax=ax, shrink=0.8)
    cbar.set_label("Walking speed (m/s)")

    if not np.isnan(corr):
        ax.text(
            0.02,
            0.96,
            f"Correlation (log steps vs asymmetry): {corr:.2f}",
            transform=ax.transAxes,
            fontsize=10,
            color=PALETTE["slate"],
        )

    path = os.path.join(out_dir, "gait_signature.png")
    save_fig(fig, path)
    return path


def plot_hrv_rhr_sleep(conn, out_dir):
    rows = fetch_rows(
        conn,
        """
        WITH hrv AS (
          SELECT date(start_dt) AS day, AVG(value_num) AS hrv
          FROM records_norm
          WHERE type = 'HKQuantityTypeIdentifierHeartRateVariabilitySDNN'
          GROUP BY day
        ),
        rhr AS (
          SELECT date(start_dt) AS day, AVG(value_num) AS rhr
          FROM records_norm
          WHERE type = 'HKQuantityTypeIdentifierRestingHeartRate'
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
          hrv.day,
          rhr.rhr,
          hrv.hrv,
          sleep.asleep_hours
        FROM hrv
        JOIN rhr ON rhr.day = hrv.day
        LEFT JOIN sleep ON sleep.day = hrv.day
        WHERE rhr.rhr IS NOT NULL AND hrv.hrv IS NOT NULL
        ORDER BY hrv.day
        """,
    )
    if not rows:
        return None

    rhr = np.array([row[1] for row in rows], dtype=float)
    hrv = np.array([row[2] for row in rows], dtype=float)
    sleep = np.array([row[3] if row[3] is not None else np.nan for row in rows])

    fig, ax = plt.subplots(figsize=(10, 7))
    scatter = ax.scatter(
        rhr,
        hrv,
        c=sleep,
        cmap="viridis",
        alpha=0.65,
        s=26,
        edgecolors="none",
    )
    ax.set_title("HRV vs Resting HR (color = sleep hours)")
    ax.set_xlabel("Resting heart rate (bpm)")
    ax.set_ylabel("HRV SDNN (ms)")
    ax.grid(True, alpha=0.35)
    cbar = fig.colorbar(scatter, ax=ax, shrink=0.8)
    cbar.set_label("Sleep hours")

    if len(rhr) > 1:
        coef = np.polyfit(rhr, hrv, 1)
        xs = np.linspace(rhr.min(), rhr.max(), 100)
        ax.plot(xs, coef[0] * xs + coef[1], color=PALETTE["orange"], linewidth=2)

    path = os.path.join(out_dir, "hrv_vs_rhr_sleep.png")
    save_fig(fig, path)
    return path


def main():
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    style()

    conn = sqlite3.connect(args.db)
    try:
        ensure_views(conn)
        outputs = [
            plot_chronotype_heatmap(conn, args.out_dir),
            plot_load_recovery_quadrants(conn, args.out_dir),
            plot_gait_signature(conn, args.out_dir),
            plot_hrv_rhr_sleep(conn, args.out_dir),
        ]
    finally:
        conn.close()

    for path in [p for p in outputs if p]:
        print(f"Wrote {path}")


if __name__ == "__main__":
    raise SystemExit(main())
