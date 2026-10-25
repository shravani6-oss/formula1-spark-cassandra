"""Load the Kaggle CSV files into Apache Cassandra (keyspace `f1_analysis`).

Usage (from the project folder, with the virtual environment active):

    python ingest_to_cassandra.py --create-schema              # all tables
    python ingest_to_cassandra.py --tables drivers races       # only some tables
    python ingest_to_cassandra.py --dry-run                    # validate CSVs, do NOT touch Cassandra
    python ingest_to_cassandra.py --tables results_by_race --limit 1000

Safe to repeat: every row is an INSERT (an upsert) on its full primary key, so running the script
again overwrites the same logical rows instead of creating duplicates. The script never truncates or
drops anything. Rows that cannot be loaded (null key, bad number, duplicate key) are skipped and
counted - nothing is silently invented.
"""
import argparse
import csv
import hashlib
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from itertools import islice

import cassandra_db as cdb
from cassandra_schema import INGESTION_LOG, TABLES, TABLES_BY_NAME, default_required
from config import data_dir as default_data_dir, get_cassandra_settings
from data_loader import csv_path

log = logging.getLogger("ingest")
NULL_TOKENS = ("\\N", "")          # the Kaggle files use \N for "missing"
CHUNK = 2000                        # rows sent to Cassandra per round


# ------------------------------------------------------------------ pure row preparation
@dataclass
class IngestStats:
    rows_read: int = 0
    rows_ready: int = 0
    skipped: dict = field(default_factory=dict)

    def skip(self, reason):
        self.skipped[reason] = self.skipped.get(reason, 0) + 1

    @property
    def rows_skipped(self):
        return sum(self.skipped.values())


def convert(value, cql_type):
    """CSV text -> Python value for a CQL type. Raises ValueError for malformed values."""
    if value is None or value in NULL_TOKENS:
        return None
    if cql_type in ("int", "bigint"):
        try:
            return int(value)
        except ValueError:
            number = float(value)               # accepts "44.0"
            if not number.is_integer():
                raise ValueError(f"not an integer: {value!r}") from None
            return int(number)
    if cql_type == "double":
        return float(value)
    if cql_type == "date":
        return date.fromisoformat(value)
    return value                                # text


def prepare_rows(table, reader, stats, limit=None):
    """Yield parameter tuples (in `table.cql_columns` order) for valid, de-duplicated rows."""
    required = default_required(table)
    key_cols = table.key_columns
    seen = set()
    for record in islice(reader, limit) if limit else reader:
        stats.rows_read += 1
        values = {}
        try:
            for col in table.columns:
                values[col.cql] = convert(record.get(col.csv), col.type)
        except ValueError:
            stats.skip("invalid value (not a valid number/date)")
            continue
        if any(values[c] is None for c in required):
            stats.skip("missing required value")
            continue
        key = tuple(values[c] for c in key_cols)
        if key in seen:
            stats.skip("duplicate primary key (first occurrence kept)")
            continue
        seen.add(key)
        stats.rows_ready += 1
        yield tuple(values[c] for c in table.cql_columns)


def insert_cql(table, keyspace):
    cols = ", ".join(table.cql_columns)
    marks = ", ".join("?" for _ in table.cql_columns)
    return f"INSERT INTO {keyspace}.{table.name} ({cols}) VALUES ({marks})"


def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def open_reader(table, directory):
    """Open the table's CSV and verify that all needed columns exist. Returns (file, DictReader)."""
    path = csv_path(table.csv_file, directory)
    if not path.exists():
        raise FileNotFoundError(f"{path} not found")
    fh = open(path, newline="", encoding="utf-8-sig")
    reader = csv.DictReader(fh)
    missing = [c.csv for c in table.columns if c.csv not in (reader.fieldnames or [])]
    if missing:
        fh.close()
        raise ValueError(f"{path.name} is missing expected columns: {', '.join(missing)}")
    return fh, reader


# ------------------------------------------------------------------ writing to Cassandra
def load_table(session, keyspace, table, directory, limit=None, dry_run=False, concurrency=64):
    """Load one table. Returns a dict with the outcome (also written to ingestion_log unless dry-run)."""
    stats = IngestStats()
    path = csv_path(table.csv_file, directory)
    outcome = {"table": table.name, "status": "failed", "rows_written": 0, "message": ""}
    fh = None
    try:
        fh, reader = open_reader(table, directory)
        rows = prepare_rows(table, reader, stats, limit)
        if dry_run:
            for _ in rows:       # just consume: validation only
                pass
            outcome.update(status="dry-run", message="validated only, nothing written")
        else:
            from cassandra.concurrent import execute_concurrent_with_args
            from cassandra.query import UNSET_VALUE
            prepared = session.prepare(insert_cql(table, keyspace))
            written, errors, first_error = 0, 0, ""
            key_idx = {table.cql_columns.index(c) for c in table.key_columns}
            while True:
                chunk = list(islice(rows, CHUNK))
                if not chunk:
                    break
                # UNSET (instead of null) for empty non-key cells avoids writing tombstones.
                params = [tuple(UNSET_VALUE if (v is None and i not in key_idx) else v
                                for i, v in enumerate(row)) for row in chunk]
                results = execute_concurrent_with_args(session, prepared, params, concurrency=concurrency,
                                                       raise_on_first_error=False)
                for ok, res in results:
                    if ok:
                        written += 1
                    else:
                        errors += 1
                        first_error = first_error or str(res)
            outcome["rows_written"] = written
            if errors == 0:
                outcome.update(status="success", message="")
            else:
                outcome.update(status="partial" if written else "failed",
                               message=f"{errors} write errors, first: {first_error[:200]}")
    except (OSError, ValueError, cdb.CassandraError) as exc:
        outcome["message"] = str(exc)
    except Exception as exc:  # noqa: BLE001 - report instead of crashing the whole run
        outcome["message"] = cdb.describe_error(exc)
    finally:
        if fh:
            fh.close()

    outcome.update(rows_read=stats.rows_read, rows_skipped=stats.rows_skipped, skipped=dict(stats.skipped))
    if not dry_run:
        _write_log(session, keyspace, table, path, outcome)
    return outcome


def _write_log(session, keyspace, table, path, outcome):
    """Record the real outcome. A failed log write is reported but never hides the load result."""
    try:
        session.execute(
            f"INSERT INTO {keyspace}.{INGESTION_LOG} (table_name, source_file, source_sha256, rows_read, "
            "rows_written, rows_skipped, status, message, finished_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (table.name, path.name, file_sha256(path) if path.exists() else "", outcome.get("rows_read", 0),
             outcome["rows_written"], outcome.get("rows_skipped", 0), outcome["status"],
             outcome["message"], datetime.now(timezone.utc)))
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not write ingestion_log for %s: %s", table.name, exc)


def ingest(table_names=None, directory=None, limit=None, dry_run=False, create_schema=False, concurrency=64):
    """Ingest the given tables (default: all). Returns a list of outcome dicts."""
    directory = directory or default_data_dir()
    names = list(table_names) if table_names else [t.name for t in TABLES]
    unknown = [n for n in names if n not in TABLES_BY_NAME]
    if unknown:
        raise ValueError(f"Unknown table(s): {', '.join(unknown)}. Valid: {', '.join(TABLES_BY_NAME)}")
    settings = get_cassandra_settings()
    outcomes = []
    if dry_run:
        for name in names:
            outcomes.append(load_table(None, settings.keyspace, TABLES_BY_NAME[name], directory, limit, True))
        return outcomes
    cluster, session = cdb.connect(settings, use_keyspace=False)
    try:
        if create_schema:
            log.info("Applied %d schema statements", cdb.apply_schema(session))
        try:
            session.set_keyspace(settings.keyspace)
        except Exception as exc:  # noqa: BLE001
            raise cdb.CassandraError(cdb.describe_error(exc, settings)) from exc
        for name in names:
            log.info("Loading %s ...", name)
            out = load_table(session, settings.keyspace, TABLES_BY_NAME[name], directory, limit, False, concurrency)
            outcomes.append(out)
            log.info("%s: %s (written %s, read %s, skipped %s) %s", name, out["status"], out["rows_written"],
                     out["rows_read"], out["rows_skipped"], out["message"])
    finally:
        cluster.shutdown()
    return outcomes


def main(argv=None):
    ap = argparse.ArgumentParser(description="Load the Formula 1 CSV files into Apache Cassandra.")
    ap.add_argument("--tables", nargs="*", help=f"tables to load (default all): {', '.join(TABLES_BY_NAME)}")
    ap.add_argument("--data-dir", help="folder with the CSV files (default: ./data or F1_DATA_DIR)")
    ap.add_argument("--limit", type=int, help="only read the first N CSV rows of each table (for testing)")
    ap.add_argument("--dry-run", action="store_true", help="validate the CSVs without connecting to Cassandra")
    ap.add_argument("--create-schema", action="store_true", help="run cql/schema.cql first (idempotent)")
    ap.add_argument("--concurrency", type=int, default=64, help="parallel writes (default 64)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from pathlib import Path
    directory = Path(args.data_dir).resolve() if args.data_dir else default_data_dir()
    try:
        outcomes = ingest(args.tables, directory, args.limit, args.dry_run, args.create_schema, args.concurrency)
    except (cdb.CassandraError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(f"\n{'table':34} {'status':9} {'read':>8} {'written':>8} {'skipped':>8}")
    for o in outcomes:
        print(f"{o['table']:34} {o['status']:9} {o['rows_read']:>8} {o['rows_written']:>8} {o['rows_skipped']:>8}")
        for reason, n in o["skipped"].items():
            print(f"{'':34}   skipped {n}: {reason}")
        if o["message"]:
            print(f"{'':34}   {o['message']}")
    ok = all(o["status"] in ("success", "dry-run") for o in outcomes)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
