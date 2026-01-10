# Package guide

This repo contains personal health data. Distribute only to trusted recipients.

## What to include
- `scripts/health_inventory.py`
- `scripts/health_to_sqlite.py`
- `scripts/health_postprocess.py`
- `scripts/health_plots.py`
- `scripts/health_social_plots.py`
- `queries/README.md`
- `queries/dashboards.sql`
- `queries/advanced_queries.sql`
- `export.xml` (optional, very large; required to rebuild SQLite)
- `workout-routes/` (optional, for route import)
- `electrocardiograms/` (optional, for ECG import)
- `inventory.json` (optional, precomputed summary)
- `health.sqlite` (optional, prebuilt DB; large)
- `plots/` (optional, pre-rendered images)

## What not to include
- `.venv/` (recommend the recipient creates their own)
- Any other files not listed above

## Recipient setup (recommended)
1) Create a venv and install matplotlib:
   ```bash
   python3 -m venv .venv
   .venv/bin/python -m pip install matplotlib
   ```
2) Build the SQLite DB (if not shipping `health.sqlite`):
   ```bash
   python3 scripts/health_to_sqlite.py export.xml --out health.sqlite
   python3 scripts/health_postprocess.py --db health.sqlite
   ```
3) Generate charts:
   ```bash
   .venv/bin/python scripts/health_plots.py --db health.sqlite --out-dir plots
   ```

## File size notes
- `export.xml` and `health.sqlite` can be multiple GB.
- `workout-routes/` and `electrocardiograms/` add size but are optional.
