# Health SQLite queries

Run the postprocess step once to build normalized views and import routes/ECGs:

```bash
python3 scripts/health_postprocess.py --db health.sqlite
```

Generate charts (if using the local venv, call the venv Python):

```bash
.venv/bin/python scripts/health_plots.py --db health.sqlite --out-dir plots
```

Generate social-ready charts:

```bash
.venv/bin/python scripts/health_social_plots.py --db health.sqlite --out-dir plots
```

Then run any query file:

```bash
sqlite3 health.sqlite < queries/dashboards.sql
sqlite3 health.sqlite < queries/advanced_queries.sql
```

If you re-run the import and want to force re-ingest routes/ECGs:

```bash
python3 scripts/health_postprocess.py --db health.sqlite --no-skip-existing
```
