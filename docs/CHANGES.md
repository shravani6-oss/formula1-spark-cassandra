# Files changed and why

Original project (untouched, separate copy): `formula1-spark-analysis/`. This project is a modified copy.

## New files
| File | Purpose |
|---|---|
| `config.py` | Central settings from environment variables / `.env`; Cassandra password is never in code or `repr`. |
| `cassandra_schema.py` | Data model of the 14 Cassandra tables (columns, partition/clustering keys, purpose) + CQL generator. |
| `cql/schema.cql` | Generated, idempotent keyspace/table script (`IF NOT EXISTS`, no DROP/TRUNCATE). |
| `cassandra_db.py` | Driver connection, real connection test, schema runner, previews, counts, friendly error messages. |
| `create_schema.py` | CLI: create keyspace + tables. |
| `ingest_to_cassandra.py` | CLI: validate and upsert the CSVs, skip/count bad rows, write `ingestion_log`; `--dry-run`, `--tables`, `--limit`. |
| `cassandra_spark.py` | Spark reads Cassandra tables, runs the analyses, compares with the CSV pipeline. |
| `cassandra_page.py` | Dashboard page 7. |
| `docker-compose.yml`, `.env.example`, `.gitignore` | Cassandra container, configuration template. |
| `docs/` | `architecture.svg`, `VIVA_QA.md`, this file. |
| `tests/conftest.py`, `tests/test_cassandra_logic.py`, `tests/test_cassandra_integration.py` | Offline tests, optional live-Cassandra tests. |

## Modified files
| File | Important changes |
|---|---|
| `data_loader.py` | Schemas rewritten for the real 14 files (checked against the headers, in file order); `\N` = null; pandas validation report; lazy Spark types so validation runs without Spark; all files except the original six are optional. |
| `spark_processor.py` | Connector-aware `get_spark` with fallback; `clean_data` works on any subset of tables; deterministic lap de-duplication (first row); pit duration from `milliseconds`, outlier cut-off `PIT_MAX_SEC`; new functions `season_champions`, `qualifying_vs_race`, `pit_by_season`, `season_summary`; races counted only if they have results; extra KPIs (circuits, seasons, year range); lap/pit data cached after cleaning. |
| `app.py` | Page 7 added; dataset banner replaces the "synthetic data" banner; Overview KPIs, champions tab and validation expander; qualifying-vs-race section; pit-by-season chart; top-N / "Other" grouping so charts stay readable with hundreds of drivers. The six original pages and their filters/tables/downloads are kept. |
| `visualizations.py` | Added `architecture_html()`. |
| `tests/test_spark_processor.py` | Updated to the real schemas; added tests for duplicate laps, pit milliseconds/outliers, and a Spark-vs-pandas check on the real data (old synthetic-data test removed because the synthetic data is gone). |
| `requirements.txt` | Added `cassandra-driver`. |
| `README.md` | Rewritten. |
| `data/*.csv` | Synthetic files replaced by the 14 supplied CSVs (unchanged). |
| `.streamlit/config.toml` | Unchanged. |
