#!/usr/bin/env python3
import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from xml.etree.ElementTree import iterparse


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stream Apple Health export.xml into a SQLite database."
    )
    parser.add_argument(
        "export",
        nargs="?",
        default="export.xml",
        help="Path to export.xml (default: export.xml)",
    )
    parser.add_argument(
        "--out",
        default="health.sqlite",
        help="Output SQLite path (default: health.sqlite)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Rows per batch insert (default: 10000)",
    )
    parser.add_argument(
        "--with-metadata",
        action="store_true",
        help="Store Record MetadataEntry values (slower, larger DB).",
    )
    return parser.parse_args()


SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
  id INTEGER PRIMARY KEY,
  type TEXT,
  unit TEXT,
  value TEXT,
  source_name TEXT,
  source_version TEXT,
  device TEXT,
  creation_date TEXT,
  start_date TEXT,
  end_date TEXT
);

CREATE TABLE IF NOT EXISTS workouts (
  id INTEGER PRIMARY KEY,
  workout_activity_type TEXT,
  duration REAL,
  duration_unit TEXT,
  total_energy_burned REAL,
  total_energy_burned_unit TEXT,
  total_distance REAL,
  total_distance_unit TEXT,
  source_name TEXT,
  source_version TEXT,
  device TEXT,
  creation_date TEXT,
  start_date TEXT,
  end_date TEXT
);

CREATE TABLE IF NOT EXISTS correlations (
  id INTEGER PRIMARY KEY,
  type TEXT,
  source_name TEXT,
  source_version TEXT,
  device TEXT,
  creation_date TEXT,
  start_date TEXT,
  end_date TEXT
);

CREATE TABLE IF NOT EXISTS activity_summaries (
  id INTEGER PRIMARY KEY,
  date_components TEXT,
  active_energy_burned REAL,
  active_energy_burned_goal REAL,
  active_energy_burned_unit TEXT,
  apple_move_time REAL,
  apple_move_time_goal REAL,
  apple_exercise_time REAL,
  apple_exercise_time_goal REAL,
  apple_stand_hours REAL,
  apple_stand_hours_goal REAL
);

CREATE TABLE IF NOT EXISTS clinical_records (
  id INTEGER PRIMARY KEY,
  type TEXT,
  source_name TEXT,
  source_version TEXT,
  device TEXT,
  creation_date TEXT,
  start_date TEXT,
  end_date TEXT,
  display_name TEXT,
  extra_json TEXT
);

CREATE TABLE IF NOT EXISTS audiograms (
  id INTEGER PRIMARY KEY,
  source_name TEXT,
  source_version TEXT,
  device TEXT,
  creation_date TEXT,
  start_date TEXT,
  end_date TEXT,
  extra_json TEXT
);

CREATE TABLE IF NOT EXISTS vision_prescriptions (
  id INTEGER PRIMARY KEY,
  source_name TEXT,
  source_version TEXT,
  device TEXT,
  creation_date TEXT,
  start_date TEXT,
  end_date TEXT,
  extra_json TEXT
);

CREATE TABLE IF NOT EXISTS record_metadata (
  record_id INTEGER,
  key TEXT,
  value TEXT
);
"""


def init_db(conn):
    conn.executescript(SCHEMA)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")


def flush(conn, rows_by_table):
    cursor = conn.cursor()
    for table, rows in rows_by_table.items():
        if not rows:
            continue
        if table == "records":
            cursor.executemany(
                """
                INSERT INTO records (
                  id, type, unit, value, source_name, source_version, device,
                  creation_date, start_date, end_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        elif table == "workouts":
            cursor.executemany(
                """
                INSERT INTO workouts (
                  id, workout_activity_type, duration, duration_unit,
                  total_energy_burned, total_energy_burned_unit,
                  total_distance, total_distance_unit,
                  source_name, source_version, device, creation_date,
                  start_date, end_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        elif table == "correlations":
            cursor.executemany(
                """
                INSERT INTO correlations (
                  id, type, source_name, source_version, device,
                  creation_date, start_date, end_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        elif table == "activity_summaries":
            cursor.executemany(
                """
                INSERT INTO activity_summaries (
                  id, date_components, active_energy_burned,
                  active_energy_burned_goal, active_energy_burned_unit,
                  apple_move_time, apple_move_time_goal,
                  apple_exercise_time, apple_exercise_time_goal,
                  apple_stand_hours, apple_stand_hours_goal
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        elif table == "clinical_records":
            cursor.executemany(
                """
                INSERT INTO clinical_records (
                  id, type, source_name, source_version, device,
                  creation_date, start_date, end_date, display_name, extra_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        elif table == "audiograms":
            cursor.executemany(
                """
                INSERT INTO audiograms (
                  id, source_name, source_version, device,
                  creation_date, start_date, end_date, extra_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        elif table == "vision_prescriptions":
            cursor.executemany(
                """
                INSERT INTO vision_prescriptions (
                  id, source_name, source_version, device,
                  creation_date, start_date, end_date, extra_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        elif table == "record_metadata":
            cursor.executemany(
                "INSERT INTO record_metadata (record_id, key, value) VALUES (?, ?, ?)",
                rows,
            )
        rows.clear()
    conn.commit()


def main():
    args = parse_args()
    conn = sqlite3.connect(args.out)
    init_db(conn)

    record_id = 0
    workout_id = 0
    correlation_id = 0
    activity_summary_id = 0
    clinical_record_id = 0
    audiogram_id = 0
    vision_id = 0

    rows_by_table = defaultdict(list)

    in_record = False
    record_metadata = []

    try:
        for event, elem in iterparse(args.export, events=("start", "end")):
            if event == "start":
                if elem.tag == "Record":
                    in_record = True
                    record_metadata = []
            elif event == "end":
                if elem.tag == "MetadataEntry" and args.with_metadata and in_record:
                    key = elem.attrib.get("key")
                    value = elem.attrib.get("value")
                    record_metadata.append((key, value))
                elif elem.tag == "Record":
                    record_id += 1
                    attrs = elem.attrib
                    rows_by_table["records"].append(
                        (
                            record_id,
                            attrs.get("type"),
                            attrs.get("unit"),
                            attrs.get("value"),
                            attrs.get("sourceName"),
                            attrs.get("sourceVersion"),
                            attrs.get("device"),
                            attrs.get("creationDate"),
                            attrs.get("startDate"),
                            attrs.get("endDate"),
                        )
                    )
                    if args.with_metadata and record_metadata:
                        rows_by_table["record_metadata"].extend(
                            (record_id, key, value) for key, value in record_metadata
                        )
                    in_record = False
                elif elem.tag == "Workout":
                    workout_id += 1
                    attrs = elem.attrib
                    rows_by_table["workouts"].append(
                        (
                            workout_id,
                            attrs.get("workoutActivityType"),
                            _to_float(attrs.get("duration")),
                            attrs.get("durationUnit"),
                            _to_float(attrs.get("totalEnergyBurned")),
                            attrs.get("totalEnergyBurnedUnit"),
                            _to_float(attrs.get("totalDistance")),
                            attrs.get("totalDistanceUnit"),
                            attrs.get("sourceName"),
                            attrs.get("sourceVersion"),
                            attrs.get("device"),
                            attrs.get("creationDate"),
                            attrs.get("startDate"),
                            attrs.get("endDate"),
                        )
                    )
                elif elem.tag == "Correlation":
                    correlation_id += 1
                    attrs = elem.attrib
                    rows_by_table["correlations"].append(
                        (
                            correlation_id,
                            attrs.get("type"),
                            attrs.get("sourceName"),
                            attrs.get("sourceVersion"),
                            attrs.get("device"),
                            attrs.get("creationDate"),
                            attrs.get("startDate"),
                            attrs.get("endDate"),
                        )
                    )
                elif elem.tag == "ActivitySummary":
                    activity_summary_id += 1
                    attrs = elem.attrib
                    rows_by_table["activity_summaries"].append(
                        (
                            activity_summary_id,
                            attrs.get("dateComponents"),
                            _to_float(attrs.get("activeEnergyBurned")),
                            _to_float(attrs.get("activeEnergyBurnedGoal")),
                            attrs.get("activeEnergyBurnedUnit"),
                            _to_float(attrs.get("appleMoveTime")),
                            _to_float(attrs.get("appleMoveTimeGoal")),
                            _to_float(attrs.get("appleExerciseTime")),
                            _to_float(attrs.get("appleExerciseTimeGoal")),
                            _to_float(attrs.get("appleStandHours")),
                            _to_float(attrs.get("appleStandHoursGoal")),
                        )
                    )
                elif elem.tag == "ClinicalRecord":
                    clinical_record_id += 1
                    attrs = elem.attrib
                    rows_by_table["clinical_records"].append(
                        (
                            clinical_record_id,
                            attrs.get("type"),
                            attrs.get("sourceName"),
                            attrs.get("sourceVersion"),
                            attrs.get("device"),
                            attrs.get("creationDate"),
                            attrs.get("startDate"),
                            attrs.get("endDate"),
                            attrs.get("displayName"),
                            json.dumps(attrs, ensure_ascii=True),
                        )
                    )
                elif elem.tag == "Audiogram":
                    audiogram_id += 1
                    attrs = elem.attrib
                    rows_by_table["audiograms"].append(
                        (
                            audiogram_id,
                            attrs.get("sourceName"),
                            attrs.get("sourceVersion"),
                            attrs.get("device"),
                            attrs.get("creationDate"),
                            attrs.get("startDate"),
                            attrs.get("endDate"),
                            json.dumps(attrs, ensure_ascii=True),
                        )
                    )
                elif elem.tag == "VisionPrescription":
                    vision_id += 1
                    attrs = elem.attrib
                    rows_by_table["vision_prescriptions"].append(
                        (
                            vision_id,
                            attrs.get("sourceName"),
                            attrs.get("sourceVersion"),
                            attrs.get("device"),
                            attrs.get("creationDate"),
                            attrs.get("startDate"),
                            attrs.get("endDate"),
                            json.dumps(attrs, ensure_ascii=True),
                        )
                    )

                elem.clear()

                if len(rows_by_table["records"]) >= args.batch_size:
                    flush(conn, rows_by_table)
    except FileNotFoundError:
        print(f"File not found: {args.export}", file=sys.stderr)
        return 2

    flush(conn, rows_by_table)
    create_indexes(conn)
    conn.close()
    print(f"Wrote {args.out}")
    return 0


def _to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def create_indexes(conn):
    conn.execute("CREATE INDEX IF NOT EXISTS idx_records_type ON records(type);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_records_start_date ON records(start_date);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_records_source ON records(source_name);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_workouts_type ON workouts(workout_activity_type);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_workouts_start_date ON workouts(start_date);")
    conn.commit()


if __name__ == "__main__":
    raise SystemExit(main())
