"""Integration tests against a REAL Cassandra (and Spark). Skipped unless you opt in:

    set F1_RUN_CASSANDRA_TESTS=1        (PowerShell: $env:F1_RUN_CASSANDRA_TESTS="1")

Read-only: they only check that the data you ingested is present and that Spark can read it.
Run this file on its own (`pytest tests/test_cassandra_integration.py`): it starts Spark WITH the connector,
which the unit tests deliberately do not. Run `python create_schema.py` and `python ingest_to_cassandra.py` first.
"""
import os

import pytest

import cassandra_db as cdb
import ingest_to_cassandra as ing
from cassandra_schema import TABLES
from config import get_cassandra_settings

pytestmark = pytest.mark.skipif(os.environ.get("F1_RUN_CASSANDRA_TESTS") != "1",
                                reason="set F1_RUN_CASSANDRA_TESTS=1 to run tests against a live Cassandra")


def test_connection_and_keyspace():
    st = cdb.check_connection()
    assert st.connected, st.message
    assert st.keyspace_exists, st.message
    assert {t.name for t in TABLES} <= set(st.tables)


def test_small_table_counts_match_csv():
    s = get_cassandra_settings()
    expected = {o["table"]: o["rows_read"] - o["rows_skipped"] for o in ing.ingest(dry_run=True)}
    with cdb.open_session(s) as session:
        for t in TABLES:
            if not t.large:
                assert cdb.count_rows(session, s.keyspace, t.name, 60) == expected[t.name], t.name


def test_spark_reads_cassandra_and_matches_csv_pipeline():
    pytest.importorskip("pyspark")
    import cassandra_spark as cs
    import spark_processor as sp
    spark = sp.get_spark("f1-cassandra-tests")
    assert sp.CONNECTOR_STATUS["loaded"], sp.CONNECTOR_STATUS["error"]
    ctx = sp.prepare(spark)
    res = cs.run_analytics(spark)
    cmp = cs.compare_with_csv(res["totals"], ctx["full"])
    assert (cmp["match"] == "yes").all(), cmp
