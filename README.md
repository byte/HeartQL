# Apple Health Export Toolkit

Convert an Apple Health export into a local SQLite database and generate charts
and advanced queries that the Health app does not expose.

## Requirements
- Python 3
- sqlite3

## Quickstart
1) Place your Apple Health `export.xml` in this folder.
2) Build the SQLite database:
   ```bash
   python3 scripts/health_to_sqlite.py export.xml --out health.sqlite
   python3 scripts/health_postprocess.py --db health.sqlite
   ```
3) Run queries:
   ```bash
   sqlite3 health.sqlite < queries/dashboards.sql
   sqlite3 health.sqlite < queries/advanced_queries.sql
   ```
4) Generate charts:
   ```bash
   python3 -m venv .venv
   .venv/bin/python -m pip install matplotlib
   .venv/bin/python scripts/health_plots.py --db health.sqlite --out-dir plots
   .venv/bin/python scripts/health_social_plots.py --db health.sqlite --out-dir plots
   ```

## Optional data
- `workout-routes/` (GPX files) for route imports
- `electrocardiograms/` (CSV files) for ECG imports

## Notes
- `scripts/health_postprocess.py` normalizes source names and builds views.
- Large files are excluded by `.gitignore` by default.
