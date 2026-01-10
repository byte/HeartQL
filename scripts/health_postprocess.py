#!/usr/bin/env python3
import argparse
import csv
import json
import math
import os
import re
import sqlite3
from collections import defaultdict
from xml.etree.ElementTree import iterparse


def parse_args():
    parser = argparse.ArgumentParser(
        description="Post-process health.sqlite: normalize sources, import routes and ECGs."
    )
    parser.add_argument(
        "--db",
        default="health.sqlite",
        help="SQLite DB path (default: health.sqlite)",
    )
    parser.add_argument(
        "--routes-dir",
        default="workout-routes",
        help="Workout routes directory (default: workout-routes)",
    )
    parser.add_argument(
        "--ecg-dir",
        default="electrocardiograms",
        help="ECG CSV directory (default: electrocardiograms)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip routes/ECGs already ingested.",
    )
    parser.add_argument(
        "--no-skip-existing",
        dest="skip_existing",
        action="store_false",
        help="Re-ingest routes/ECGs even if already present.",
    )
    parser.add_argument(
        "--no-routes",
        action="store_true",
        help="Skip workout route import.",
    )
    parser.add_argument(
        "--no-ecg",
        action="store_true",
        help="Skip ECG import.",
    )
    parser.set_defaults(skip_existing=True)
    return parser.parse_args()


SCHEMA = """
CREATE TABLE IF NOT EXISTS source_aliases (
  raw_source TEXT PRIMARY KEY,
  normalized_source TEXT
);

CREATE TABLE IF NOT EXISTS workout_routes (
  id INTEGER PRIMARY KEY,
  file_path TEXT UNIQUE,
  start_time TEXT,
  end_time TEXT,
  point_count INTEGER,
  distance_km REAL,
  min_lat REAL,
  max_lat REAL,
  min_lon REAL,
  max_lon REAL
);

CREATE TABLE IF NOT EXISTS workout_route_points (
  route_id INTEGER,
  point_index INTEGER,
  lat REAL,
  lon REAL,
  ele REAL,
  time TEXT
);

CREATE TABLE IF NOT EXISTS ecg_records (
  id INTEGER PRIMARY KEY,
  file_path TEXT UNIQUE,
  recorded_date TEXT,
  classification TEXT,
  symptoms TEXT,
  sample_rate_hz REAL,
  lead TEXT,
  unit TEXT,
  device TEXT,
  software_version TEXT,
  extra_json TEXT
);

CREATE TABLE IF NOT EXISTS ecg_samples (
  ecg_id INTEGER,
  sample_index INTEGER,
  value REAL
);
"""


def normalize_source_name(value):
    if value is None:
        return None
    cleaned = value.replace("\u2019", "'").replace("\u2018", "'")
    cleaned = cleaned.replace("\u00a0", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def init_db(conn):
    conn.executescript(SCHEMA)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")


def create_views(conn):
    conn.execute(
        """
        CREATE VIEW IF NOT EXISTS records_norm AS
        SELECT
          r.*,
          sa.normalized_source AS source_name_norm,
          CAST(r.value AS REAL) AS value_num,
          substr(r.start_date, 1, 19) AS start_dt,
          substr(r.end_date, 1, 19) AS end_dt
        FROM records r
        LEFT JOIN source_aliases sa ON r.source_name = sa.raw_source
        """
    )
    conn.execute(
        """
        CREATE VIEW IF NOT EXISTS workouts_norm AS
        SELECT
          w.*,
          sa.normalized_source AS source_name_norm,
          substr(w.start_date, 1, 19) AS start_dt,
          substr(w.end_date, 1, 19) AS end_dt
        FROM workouts w
        LEFT JOIN source_aliases sa ON w.source_name = sa.raw_source
        """
    )
    conn.execute(
        """
        CREATE VIEW IF NOT EXISTS correlations_norm AS
        SELECT
          c.*,
          sa.normalized_source AS source_name_norm,
          substr(c.start_date, 1, 19) AS start_dt,
          substr(c.end_date, 1, 19) AS end_dt
        FROM correlations c
        LEFT JOIN source_aliases sa ON c.source_name = sa.raw_source
        """
    )
    conn.commit()


def build_source_aliases(conn):
    cursor = conn.cursor()
    sources = set()
    for table, column in [
        ("records", "source_name"),
        ("workouts", "source_name"),
        ("correlations", "source_name"),
        ("clinical_records", "source_name"),
        ("audiograms", "source_name"),
        ("vision_prescriptions", "source_name"),
    ]:
        try:
            cursor.execute(f"SELECT DISTINCT {column} FROM {table}")
        except sqlite3.OperationalError:
            continue
        sources.update(row[0] for row in cursor.fetchall() if row[0] is not None)

    rows = [(src, normalize_source_name(src)) for src in sources]
    cursor.executemany(
        "INSERT OR REPLACE INTO source_aliases (raw_source, normalized_source) VALUES (?, ?)",
        rows,
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_source_aliases_norm ON source_aliases(normalized_source)"
    )
    conn.commit()


def list_existing(conn, table):
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT file_path FROM {table}")
    except sqlite3.OperationalError:
        return set()
    return {row[0] for row in cursor.fetchall()}


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0088
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(
        dlambda / 2.0
    ) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def import_workout_routes(conn, routes_dir, skip_existing):
    if not os.path.isdir(routes_dir):
        return 0

    existing = list_existing(conn, "workout_routes") if skip_existing else set()
    route_rows = []
    point_rows = []
    route_id = _next_id(conn, "workout_routes")
    inserted = 0

    for name in sorted(os.listdir(routes_dir)):
        if not name.lower().endswith(".gpx"):
            continue
        path = os.path.join(routes_dir, name)
        if skip_existing and path in existing:
            continue

        points = []
        for _event, elem in iterparse(path, events=("end",)):
            tag = _strip_ns(elem.tag)
            if tag == "trkpt":
                lat = _to_float(elem.attrib.get("lat"))
                lon = _to_float(elem.attrib.get("lon"))
                ele = None
                time = None
                for child in elem:
                    child_tag = _strip_ns(child.tag)
                    if child_tag == "ele":
                        ele = _to_float(child.text)
                    elif child_tag == "time":
                        time = child.text
                points.append((lat, lon, ele, time))
            elem.clear()

        if not points:
            continue

        distance_km = 0.0
        lats = [p[0] for p in points if p[0] is not None]
        lons = [p[1] for p in points if p[1] is not None]
        min_lat = min(lats) if lats else None
        max_lat = max(lats) if lats else None
        min_lon = min(lons) if lons else None
        max_lon = max(lons) if lons else None
        start_time = points[0][3]
        end_time = points[-1][3]

        last_point = None
        for idx, (lat, lon, ele, time) in enumerate(points):
            if lat is not None and lon is not None:
                if last_point is not None:
                    distance_km += haversine_km(
                        lat, lon, last_point[0], last_point[1]
                    )
                last_point = (lat, lon)
            point_rows.append((route_id, idx, lat, lon, ele, time))

        route_rows.append(
            (
                route_id,
                path,
                start_time,
                end_time,
                len(points),
                distance_km,
                min_lat,
                max_lat,
                min_lon,
                max_lon,
            )
        )
        inserted += 1
        route_id += 1

        if len(point_rows) >= 20000:
            _flush_routes(conn, route_rows, point_rows)

    _flush_routes(conn, route_rows, point_rows)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_workout_route_points_route ON workout_route_points(route_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_workout_routes_start_time ON workout_routes(start_time)"
    )
    conn.commit()
    return inserted


def _flush_routes(conn, route_rows, point_rows):
    if route_rows:
        conn.executemany(
            """
            INSERT OR IGNORE INTO workout_routes (
              id, file_path, start_time, end_time, point_count, distance_km,
              min_lat, max_lat, min_lon, max_lon
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            route_rows,
        )
        route_rows.clear()
    if point_rows:
        conn.executemany(
            """
            INSERT INTO workout_route_points (
              route_id, point_index, lat, lon, ele, time
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            point_rows,
        )
        point_rows.clear()
    conn.commit()


def import_ecg(conn, ecg_dir, skip_existing):
    if not os.path.isdir(ecg_dir):
        return 0

    existing = list_existing(conn, "ecg_records") if skip_existing else set()
    record_rows = []
    sample_rows = []
    ecg_id = _next_id(conn, "ecg_records")
    inserted = 0

    for name in sorted(os.listdir(ecg_dir)):
        if not name.lower().endswith(".csv"):
            continue
        path = os.path.join(ecg_dir, name)
        if skip_existing and path in existing:
            continue

        metadata, samples = parse_ecg_csv(path)
        if not samples:
            continue

        record_rows.append(
            (
                ecg_id,
                path,
                metadata.get("Recorded Date"),
                metadata.get("Classification"),
                metadata.get("Symptoms"),
                _parse_sample_rate(metadata.get("Sample Rate")),
                metadata.get("Lead"),
                metadata.get("Unit"),
                metadata.get("Device"),
                metadata.get("Software Version"),
                json.dumps(metadata, ensure_ascii=True),
            )
        )
        for idx, value in enumerate(samples):
            sample_rows.append((ecg_id, idx, value))

        inserted += 1
        ecg_id += 1

        if len(sample_rows) >= 20000:
            _flush_ecg(conn, record_rows, sample_rows)

    _flush_ecg(conn, record_rows, sample_rows)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ecg_samples_ecg ON ecg_samples(ecg_id)"
    )
    conn.commit()
    return inserted


def _flush_ecg(conn, record_rows, sample_rows):
    if record_rows:
        conn.executemany(
            """
            INSERT OR IGNORE INTO ecg_records (
              id, file_path, recorded_date, classification, symptoms,
              sample_rate_hz, lead, unit, device, software_version, extra_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            record_rows,
        )
        record_rows.clear()
    if sample_rows:
        conn.executemany(
            "INSERT INTO ecg_samples (ecg_id, sample_index, value) VALUES (?, ?, ?)",
            sample_rows,
        )
        sample_rows.clear()
    conn.commit()


def parse_ecg_csv(path):
    metadata = {}
    samples = []
    number_re = re.compile(r"^[+-]?\d+(\.\d+)?$")
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            if len(row) == 1:
                value = row[0].strip()
                if not value:
                    continue
                if number_re.match(value):
                    samples.append(float(value))
                else:
                    continue
            else:
                key = row[0].strip()
                value = row[1].strip() if len(row) > 1 else ""
                metadata[key] = value
    return metadata, samples


def _parse_sample_rate(value):
    if not value:
        return None
    match = re.search(r"([0-9]+(\.[0-9]+)?)", value)
    if not match:
        return None
    return float(match.group(1))


def _next_id(conn, table):
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT MAX(id) FROM {table}")
        row = cursor.fetchone()
        if row and row[0] is not None:
            return row[0] + 1
    except sqlite3.OperationalError:
        pass
    return 1


def _to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _strip_ns(tag):
    return tag.split("}")[-1]


def main():
    args = parse_args()
    conn = sqlite3.connect(args.db)
    init_db(conn)
    build_source_aliases(conn)
    create_views(conn)

    routes_added = 0
    ecg_added = 0
    if not args.no_routes:
        routes_added = import_workout_routes(conn, args.routes_dir, args.skip_existing)
    if not args.no_ecg:
        ecg_added = import_ecg(conn, args.ecg_dir, args.skip_existing)

    conn.close()
    print(f"Routes added: {routes_added}")
    print(f"ECG records added: {ecg_added}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
