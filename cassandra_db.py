"""Cassandra connection helpers (cassandra-driver). No Streamlit code here.

Everything that talks to the network returns a clear result or raises `CassandraError` with a
human-readable message, so the dashboard and scripts can show helpful text instead of tracebacks.
Nothing in this module reports "connected" unless a real query (`SELECT release_version`) succeeded.
"""
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field

import pandas as pd

from cassandra_schema import INGESTION_LOG, TABLES_BY_NAME
from config import CQL_SCHEMA_FILE, get_cassandra_settings

log = logging.getLogger(__name__)

try:  # the driver is optional at import time so the rest of the dashboard keeps working without it
    from cassandra import AuthenticationFailed, InvalidRequest, OperationTimedOut, ReadTimeout
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster, NoHostAvailable
    DRIVER_AVAILABLE = True
    DRIVER_IMPORT_ERROR = ""
except ImportError as _exc:  # pragma: no cover - depends on the machine
    DRIVER_AVAILABLE = False
    DRIVER_IMPORT_ERROR = str(_exc)


class CassandraError(RuntimeError):
    """Raised with a user-friendly message (the original exception is chained)."""


def describe_error(exc, settings=None):
    """Translate driver exceptions into advice a student can act on."""
    s = settings or get_cassandra_settings()
    if not DRIVER_AVAILABLE:
        return ("The `cassandra-driver` package is not installed. Run `pip install -r requirements.txt`. "
                f"({DRIVER_IMPORT_ERROR})")
    if isinstance(exc, NoHostAvailable):
        return (f"Cannot reach Cassandra at {s.host_string}:{s.port}. Is the container/service running? "
                "Start it with `docker compose up -d` and wait about a minute until it is healthy.")
    if isinstance(exc, AuthenticationFailed):
        return "Cassandra rejected the credentials. Check CASSANDRA_USERNAME / CASSANDRA_PASSWORD in your .env."
    if isinstance(exc, (OperationTimedOut, ReadTimeout)):
        return "Cassandra took too long to answer (timeout). It may still be starting, or the query is too heavy."
    if isinstance(exc, InvalidRequest):
        text = str(exc)
        if "nconfigured" in text or "does not exist" in text or "Keyspace" in text:
            return (f"Keyspace or table not found ({text}). Create them with `python create_schema.py`.")
        return f"Cassandra rejected the request: {text}"
    return f"{type(exc).__name__}: {exc}"


@dataclass
class ConnectionStatus:
    connected: bool = False
    message: str = ""
    release_version: str = ""
    cluster_name: str = ""
    host: str = ""
    keyspace: str = ""
    keyspace_exists: bool = False
    tables: list = field(default_factory=list)


def _new_cluster(settings):
    if not DRIVER_AVAILABLE:
        raise CassandraError(describe_error(RuntimeError(), settings))
    auth = PlainTextAuthProvider(settings.username, settings.password) if settings.uses_auth else None
    return Cluster(list(settings.hosts), port=settings.port, auth_provider=auth,
                   connect_timeout=settings.connect_timeout)


def connect(settings=None, use_keyspace=True):
    """Open a session. Returns (cluster, session). The caller must call `cluster.shutdown()`."""
    s = settings or get_cassandra_settings()
    cluster = _new_cluster(s)
    try:
        session = cluster.connect()
        session.default_timeout = s.request_timeout
        if use_keyspace:
            session.set_keyspace(s.keyspace)
    except Exception as exc:  # noqa: BLE001
        cluster.shutdown()
        raise CassandraError(describe_error(exc, s)) from exc
    return cluster, session


@contextmanager
def open_session(settings=None, use_keyspace=True):
    """`with open_session() as session:` - always closes the connection."""
    cluster, session = connect(settings, use_keyspace)
    try:
        yield session
    finally:
        cluster.shutdown()


def check_connection(settings=None):
    """Test the connection for real and return a ConnectionStatus. Never raises."""
    s = settings or get_cassandra_settings()
    st = ConnectionStatus(keyspace=s.keyspace, host=f"{s.host_string}:{s.port}")
    if not DRIVER_AVAILABLE:
        st.message = describe_error(RuntimeError(), s)
        return st
    cluster = None
    try:
        cluster, session = connect(s, use_keyspace=False)
        row = session.execute("SELECT release_version, cluster_name FROM system.local").one()
        st.connected = True
        st.release_version, st.cluster_name = row.release_version, row.cluster_name
        ks = cluster.metadata.keyspaces.get(s.keyspace)
        st.keyspace_exists = ks is not None
        st.tables = sorted(ks.tables) if ks else []
        st.message = ("Connected." if st.keyspace_exists else
                      f"Connected, but keyspace `{s.keyspace}` does not exist yet - run `python create_schema.py`.")
    except CassandraError as exc:
        st.message = str(exc)
    except Exception as exc:  # noqa: BLE001
        st.message = describe_error(exc, s)
    finally:
        if cluster is not None:
            cluster.shutdown()
    return st


# ------------------------------------------------------------------ schema
def apply_schema(session, schema_file=CQL_SCHEMA_FILE):
    """Run cql/schema.cql. Every statement is IF NOT EXISTS, so nothing is dropped or truncated."""
    from cassandra_schema import split_statements
    try:
        text = schema_file.read_text(encoding="utf-8")
    except OSError as exc:
        raise CassandraError(f"Cannot read the schema file {schema_file}: {exc}") from exc
    statements = split_statements(text)
    for stmt in statements:
        try:
            session.execute(stmt)
        except Exception as exc:  # noqa: BLE001
            raise CassandraError(f"Schema statement failed: {stmt[:80]!r}... -> {describe_error(exc)}") from exc
    return len(statements)


# ------------------------------------------------------------------ reading
def _known_table(name):
    """Only allow table names we defined (table names cannot be bound as parameters in CQL)."""
    if name not in TABLES_BY_NAME and name != INGESTION_LOG:
        raise CassandraError(f"Unknown table: {name!r}")
    return name


def _plain(value):
    """Make driver types (cassandra.util.Date, UUID, sets...) safe for pandas/Streamlit."""
    if value is None or isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


def preview_table(session, keyspace, table, limit=20):
    """First `limit` rows of a table as a DataFrame (empty DataFrame if the table has no rows)."""
    _known_table(table)
    limit = max(1, min(int(limit), 500))
    try:
        rows = session.execute(f"SELECT * FROM {keyspace}.{table} LIMIT {limit}")
        data = [{k: _plain(v) for k, v in r._asdict().items()} for r in rows]
    except Exception as exc:  # noqa: BLE001
        raise CassandraError(describe_error(exc)) from exc
    return pd.DataFrame(data)


def count_rows(session, keyspace, table, timeout=20):
    """Exact COUNT(*), or None when Cassandra cannot answer within the timeout (very large tables)."""
    _known_table(table)
    try:
        return int(session.execute(f"SELECT COUNT(*) FROM {keyspace}.{table}", timeout=timeout).one()[0])
    except (OperationTimedOut, ReadTimeout):
        return None
    except Exception as exc:  # noqa: BLE001
        raise CassandraError(describe_error(exc)) from exc


def read_ingestion_log(session, keyspace):
    """The ingestion_log table as a DataFrame (empty if nothing has been ingested yet)."""
    try:
        return preview_table(session, keyspace, INGESTION_LOG, 100)
    except CassandraError:
        return pd.DataFrame()


def table_overview(settings=None, count_timeout=20):
    """One row per dataset table: purpose, key design, live row count and ingestion-log status."""
    s = settings or get_cassandra_settings()
    with open_session(s) as session:
        log_df = read_ingestion_log(session, s.keyspace)
        logged = {r["table_name"]: r for r in log_df.to_dict("records")} if not log_df.empty else {}
        existing = set(session.cluster.metadata.keyspaces[s.keyspace].tables)
        rows = []
        for t in TABLES_BY_NAME.values():
            entry = {"table": t.name, "purpose": t.purpose,
                     "partition key": ", ".join(t.partition_key),
                     "clustering columns": ", ".join(t.clustering_key) or "-",
                     "exists": t.name in existing, "rows": None, "rows source": "-"}
            if entry["exists"]:
                if t.large:
                    entry["rows"], entry["rows source"] = None, "too large for COUNT(*) - see ingestion log / Spark"
                else:
                    try:
                        entry["rows"] = count_rows(session, s.keyspace, t.name, count_timeout)
                        entry["rows source"] = "COUNT(*)" if entry["rows"] is not None else "timeout"
                    except CassandraError as exc:   # one failing table must not hide the others
                        entry["rows source"] = f"error: {exc}"
            rec = logged.get(t.name)
            entry["last ingestion"] = (f'{rec["status"]} · {rec["rows_written"]} rows · {rec["finished_at"]}'
                                       if rec else "never")
            rows.append(entry)
        return pd.DataFrame(rows)
