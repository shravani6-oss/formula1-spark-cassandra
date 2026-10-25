"""Central configuration. Every setting can be overridden with an environment variable
or with a `.env` file in the project folder (copy `.env.example` to `.env`).

No passwords or secrets are stored in the source code: the Cassandra password is only
ever read from the environment.
"""
import os
from dataclasses import dataclass, field
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def _load_dotenv(path):
    """Minimal .env reader (KEY=VALUE lines). Real environment variables always win."""
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError:
        return
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv(BASE_DIR / ".env")


def _env(name, default=""):
    value = os.environ.get(name)
    return default if value is None or value.strip() == "" else value.strip()


def _env_int(name, default):
    try:
        return int(_env(name, str(default)))
    except ValueError:
        return default


def _env_bool(name, default):
    return _env(name, str(default)).lower() in ("1", "true", "yes", "on")


# ------------------------------------------------------------------ dataset
def data_dir():
    """Folder with the Kaggle CSV files (relative to the project unless F1_DATA_DIR is set)."""
    override = _env("F1_DATA_DIR")
    return Path(override).expanduser().resolve() if override else BASE_DIR / "data"


CQL_SCHEMA_FILE = BASE_DIR / "cql" / "schema.cql"

# ------------------------------------------------------------------ Cassandra
KEYSPACE_DEFAULT = "f1_analysis"


@dataclass(frozen=True)
class CassandraSettings:
    hosts: tuple = ("127.0.0.1",)
    port: int = 9042
    keyspace: str = KEYSPACE_DEFAULT
    username: str = ""
    password: str = field(default="", repr=False)   # never printed in logs / repr
    connect_timeout: float = 8.0                    # seconds
    request_timeout: float = 30.0                   # seconds

    @property
    def host_string(self):
        return ",".join(self.hosts)

    @property
    def uses_auth(self):
        return bool(self.username)


def get_cassandra_settings():
    hosts = tuple(h.strip() for h in _env("CASSANDRA_HOSTS", "127.0.0.1").split(",") if h.strip())
    return CassandraSettings(
        hosts=hosts or ("127.0.0.1",),
        port=_env_int("CASSANDRA_PORT", 9042),
        keyspace=_env("CASSANDRA_KEYSPACE", KEYSPACE_DEFAULT),
        username=_env("CASSANDRA_USERNAME"),
        password=_env("CASSANDRA_PASSWORD"),
    )


# ------------------------------------------------------------------ Spark <-> Cassandra
# Spark 3.5.x is built for Scala 2.12, so the matching connector artifact is *_2.12 and its
# version line is 3.5.x. Change the package only if you also change the PySpark version.
DEFAULT_CONNECTOR_PACKAGE = "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"


def spark_cassandra_enabled():
    return _env_bool("SPARK_CASSANDRA_ENABLED", True)


def connector_package():
    return _env("SPARK_CASSANDRA_CONNECTOR_PACKAGE", DEFAULT_CONNECTOR_PACKAGE)


def connector_jar():
    """Optional local connector assembly JAR for offline machines (overrides the package)."""
    path = _env("SPARK_CASSANDRA_CONNECTOR_JAR")
    return Path(path).expanduser().resolve() if path else None
