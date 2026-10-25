"""Tests that need NO running Cassandra or Spark: data model, CQL file, row preparation, validation,
configuration. They read the real CSV files where useful."""
import re
from datetime import date

import pandas as pd
import pytest

import cassandra_schema as cschema
import data_loader as dl
import ingest_to_cassandra as ing
from config import CQL_SCHEMA_FILE, get_cassandra_settings

RESERVED = {"add", "allow", "alter", "and", "apply", "asc", "authorize", "batch", "begin", "by", "columnfamily",
            "create", "delete", "desc", "describe", "drop", "entries", "execute", "from", "full", "grant", "if",
            "in", "index", "infinity", "insert", "into", "keyspace", "limit", "modify", "nan", "norecursive",
            "not", "null", "of", "on", "or", "order", "primary", "rename", "revoke", "schema", "select", "set",
            "table", "to", "token", "truncate", "unlogged", "update", "use", "using", "view", "where", "with"}


def test_schema_file_is_in_sync_with_python_definitions():
    assert CQL_SCHEMA_FILE.read_text(encoding="utf-8") == cschema.generate_cql()


def test_every_table_column_exists_in_the_real_csv_header():
    for t in cschema.TABLES:
        csv_cols = dl.column_names(t.csv_file)
        for c in t.columns:
            assert c.csv in csv_cols, f"{t.name}: {c.csv} not in {t.csv_file}.csv"


def test_keys_are_columns_and_names_are_safe():
    for t in cschema.TABLES:
        names = t.cql_columns
        assert len(set(names)) == len(names), t.name
        assert set(t.key_columns) <= set(names), t.name
        assert not RESERVED & set(names), t.name
        assert t.partition_key, t.name


def test_statement_split_gives_keyspace_plus_all_tables():
    stmts = cschema.split_statements(cschema.generate_cql())
    assert stmts[0].startswith("CREATE KEYSPACE IF NOT EXISTS f1_analysis")
    assert len(stmts) == 1 + len(cschema.TABLES) + 1          # keyspace + tables + ingestion_log
    assert all("IF NOT EXISTS" in s for s in stmts)
    assert not any(re.search(r"\b(DROP|TRUNCATE)\b", s, re.I) for s in stmts)


def test_convert():
    assert ing.convert("44", "int") == 44 and ing.convert("44.0", "int") == 44
    assert ing.convert("\\N", "int") is None and ing.convert("", "text") is None
    assert ing.convert("1.5", "double") == 1.5
    assert ing.convert("1985-01-07", "date") == date(1985, 1, 7)
    assert ing.convert("1:30.5", "text") == "1:30.5"
    with pytest.raises(ValueError):
        ing.convert("1.5", "int")
    with pytest.raises(ValueError):
        ing.convert("abc", "double")


def _rows(table_name, records):
    t = cschema.TABLES_BY_NAME[table_name]
    stats = ing.IngestStats()
    return t, stats, list(ing.prepare_rows(t, iter(records), stats))


def test_prepare_rows_skips_bad_rows_and_keeps_first_duplicate():
    recs = [
        {"raceId": "1", "driverId": "1", "stop": "1", "lap": "5", "time": "12:00:00", "milliseconds": "24000"},
        {"raceId": "1", "driverId": "1", "stop": "1", "lap": "6", "time": "12:01:00", "milliseconds": "25000"},  # dup key
        {"raceId": "1", "driverId": "2", "stop": "1", "lap": "5", "time": "12:00:00", "milliseconds": "\\N"},    # no ms
        {"raceId": "x", "driverId": "2", "stop": "1", "lap": "5", "time": "", "milliseconds": "1"},              # bad int
    ]
    t, stats, rows = _rows("pit_stops_by_race", recs)
    assert len(rows) == 1 and rows[0][t.cql_columns.index("lap")] == 5      # first occurrence kept
    assert stats.rows_read == 4 and stats.rows_ready == 1 and stats.rows_skipped == 3


def test_insert_statement_matches_columns():
    t = cschema.TABLES_BY_NAME["drivers"]
    q = ing.insert_cql(t, "f1_analysis")
    assert q.startswith("INSERT INTO f1_analysis.drivers (driver_id, driver_ref")
    assert q.count("?") == len(t.columns)


def test_real_data_dry_run_counts_match_pandas():
    """Validate every table against the real CSV files (no Cassandra involved)."""
    out = {o["table"]: o for o in ing.ingest(dry_run=True)}
    assert set(out) == {t.name for t in cschema.TABLES}
    laps = pd.read_csv(dl.csv_path("lap_times"), dtype=str)
    assert out["lap_times_by_race_driver"]["rows_read"] == len(laps)
    assert out["lap_times_by_race_driver"]["rows_skipped"] == int(laps.duplicated(["raceId", "driverId", "lap"]).sum())
    assert out["results_by_race"]["rows_skipped"] == 0
    assert out["drivers"]["rows_read"] == len(pd.read_csv(dl.csv_path("drivers"), dtype=str))
    assert all(o["status"] == "dry-run" for o in out.values())


def test_dataset_validation_on_real_files():
    rep = dl.validate_dataset().set_index("file")
    assert rep["exists"].all() and rep["header_ok"].all()
    assert (rep["bad_numeric_cells"] == 0).all()
    assert rep.loc["lap_times.csv", "duplicate_keys"] > 0         # known property of this dataset


def test_validation_detects_wrong_header_and_missing_file(tmp_path):
    (tmp_path / "status.csv").write_text("statusId,wrong\n1,Finished\n", encoding="utf-8")
    rep = dl.validate_file("status", tmp_path)
    assert rep["header_ok"] is False and rep["missing_columns"] == "status" and rep["status"] == "check"
    assert dl.validate_file("drivers", tmp_path)["status"] == "missing"
    assert "drivers.csv" in dl.missing_files(tmp_path)


def test_settings_come_from_environment_and_hide_password(monkeypatch):
    monkeypatch.setenv("CASSANDRA_HOSTS", "10.0.0.5, 10.0.0.6")
    monkeypatch.setenv("CASSANDRA_PORT", "9142")
    monkeypatch.setenv("CASSANDRA_USERNAME", "u")
    monkeypatch.setenv("CASSANDRA_PASSWORD", "s3cret")
    s = get_cassandra_settings()
    assert s.hosts == ("10.0.0.5", "10.0.0.6") and s.port == 9142 and s.uses_auth
    assert "s3cret" not in repr(s)
    assert s.keyspace == "f1_analysis"
