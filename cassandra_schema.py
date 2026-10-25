"""Single source of truth for the Cassandra data model.

Cassandra has no joins, so each table is designed around ONE query. Relationships between
tables are kept only as identifiers (driver_id, race_id, ...) and are joined later by Spark.

`python cassandra_schema.py --write` regenerates `cql/schema.cql` from the definitions below and
`tests/test_cassandra_logic.py` checks that the committed file has not drifted from them.

Naming rule: CQL column = snake_case of the CSV column. A few CSV names are renamed because they
are CQL type keywords (date -> race_date, time -> race_time, ...) or ambiguous across tables.
"""
import sys
from dataclasses import dataclass, field

from config import CQL_SCHEMA_FILE, KEYSPACE_DEFAULT


@dataclass(frozen=True)
class Col:
    csv: str    # column name in the CSV file
    cql: str    # column name in Cassandra
    type: str   # CQL type: int | bigint | double | text | date


@dataclass(frozen=True)
class Table:
    name: str
    csv_file: str            # dataset name without .csv
    purpose: str             # shown in the README and on the dashboard
    columns: tuple
    partition_key: tuple     # CQL column names
    clustering_key: tuple = ()
    query: str = ""          # the query this table is designed to answer
    large: bool = False      # True -> exact COUNT(*) is too slow for the driver; use log / Spark
    required_cols: tuple = field(default=())   # rows with null here are skipped (default: key columns)

    @property
    def key_columns(self):
        return self.partition_key + self.clustering_key

    @property
    def cql_columns(self):
        return [c.cql for c in self.columns]

    def col_by_cql(self, name):
        return next(c for c in self.columns if c.cql == name)


def _cols(spec):
    """'csv:cql:type' strings -> Col objects."""
    out = []
    for item in spec:
        csv, cql, typ = item.split(":")
        out.append(Col(csv, cql, typ))
    return tuple(out)


_RACE_COLS = _cols(["raceId:race_id:int", "year:year:int", "round:round:int", "circuitId:circuit_id:int",
                    "name:name:text", "date:race_date:date", "time:race_time:text", "url:url:text"])
_RESULT_COLS = _cols([
    "resultId:result_id:int", "raceId:race_id:int", "driverId:driver_id:int", "constructorId:constructor_id:int",
    "number:car_number:int", "grid:grid:int", "position:position:int", "positionText:position_text:text",
    "positionOrder:position_order:int", "points:points:double", "laps:laps:int", "time:finish_time:text",
    "milliseconds:milliseconds:bigint", "fastestLap:fastest_lap:int", "rank:fastest_lap_rank:int",
    "fastestLapTime:fastest_lap_time:text", "fastestLapSpeed:fastest_lap_speed:double", "statusId:status_id:int"])

TABLES = (
    Table("drivers", "drivers", "Driver master data (one row per driver).",
          _cols(["driverId:driver_id:int", "driverRef:driver_ref:text", "number:car_number:int", "code:code:text",
                 "forename:forename:text", "surname:surname:text", "dob:dob:date",
                 "nationality:nationality:text", "url:url:text"]),
          ("driver_id",), query="Look up a driver by id."),
    Table("constructors", "constructors", "Constructor (team) master data.",
          _cols(["constructorId:constructor_id:int", "constructorRef:constructor_ref:text", "name:name:text",
                 "nationality:nationality:text", "url:url:text"]),
          ("constructor_id",), query="Look up a constructor by id."),
    Table("circuits", "circuits", "Circuit master data (location, country, coordinates).",
          _cols(["circuitId:circuit_id:int", "circuitRef:circuit_ref:text", "name:name:text",
                 "location:location:text", "country:country:text", "lat:lat:double", "lng:lng:double",
                 "alt:alt:double", "url:url:text"]),
          ("circuit_id",), query="Look up a circuit by id."),
    Table("seasons", "seasons", "List of championship seasons.",
          _cols(["year:year:int", "url:url:text"]), ("year",), query="List all seasons."),
    Table("status", "status", "Finishing-status lookup (Finished, Accident, +1 Lap, ...).",
          _cols(["statusId:status_id:int", "status:status_name:text"]), ("status_id",),
          query="Translate a status_id into text."),
    Table("races", "races", "Race calendar keyed by race_id.", _RACE_COLS, ("race_id",),
          query="Look up one race by race_id (name, season, round, circuit)."),
    Table("races_by_season", "races", "Same race data denormalised by season (Cassandra duplicates data per query).",
          _RACE_COLS, ("year",), ("round",), query="List all races of a season in round order."),
    Table("results_by_race", "results", "Race results, partitioned by race.", _RESULT_COLS, ("race_id",),
          ("result_id",), query="Get every result of one race."),
    Table("results_by_driver", "results", "Race results duplicated by driver for career queries.", _RESULT_COLS,
          ("driver_id",), ("race_id", "result_id"), query="Get a driver's whole career, ordered by race."),
    Table("lap_times_by_race_driver", "lap_times",
          "Lap-by-lap times. Partition = (race, driver) so one driver's race is a single partition.",
          _cols(["raceId:race_id:int", "driverId:driver_id:int", "lap:lap:int", "position:lap_position:int",
                 "time:lap_time:text", "milliseconds:milliseconds:int"]),
          ("race_id", "driver_id"), ("lap",), query="Get all laps of one driver in one race in lap order.",
          large=True, required_cols=("race_id", "driver_id", "lap", "milliseconds")),
    Table("pit_stops_by_race", "pit_stops", "Pit stops, partitioned by race.",
          _cols(["raceId:race_id:int", "driverId:driver_id:int", "stop:stop:int", "lap:lap:int",
                 "time:time_of_day:text", "milliseconds:milliseconds:int"]),
          ("race_id",), ("driver_id", "stop"), query="Get every pit stop of one race.",
          required_cols=("race_id", "driver_id", "stop", "milliseconds")),
    Table("qualifying_by_race", "qualifying", "Qualifying results, partitioned by race.",
          _cols(["qualifyId:qualify_id:int", "raceId:race_id:int", "driverId:driver_id:int",
                 "constructorId:constructor_id:int", "number:car_number:int", "position:position:int",
                 "q1:q1:text", "q2:q2:text", "q3:q3:text"]),
          ("race_id",), ("qualify_id",), query="Get the qualifying session of one race."),
    Table("driver_standings_by_race", "driver_standings", "Championship standings after each race (drivers).",
          _cols(["driverStandingsId:standings_id:int", "raceId:race_id:int", "driverId:driver_id:int",
                 "points:points:double", "position:position:int", "positionText:position_text:text",
                 "wins:wins:int"]),
          ("race_id",), ("driver_id",), query="Driver standings after a given race."),
    Table("constructor_standings_by_race", "constructor_standings",
          "Championship standings after each race (constructors).",
          _cols(["constructorStandingsId:standings_id:int", "raceId:race_id:int",
                 "constructorId:constructor_id:int", "points:points:double", "position:position:int",
                 "positionText:position_text:text", "wins:wins:int"]),
          ("race_id",), ("constructor_id",), query="Constructor standings after a given race."),
)

TABLES_BY_NAME = {t.name: t for t in TABLES}

# Bookkeeping table written by the ingestion script (not a dataset table).
INGESTION_LOG = "ingestion_log"
INGESTION_LOG_COLUMNS = ("table_name", "source_file", "source_sha256", "rows_read", "rows_written",
                         "rows_skipped", "status", "message", "finished_at")


def default_required(table):
    return table.required_cols or table.key_columns


# ------------------------------------------------------------------ CQL generation
def _table_cql(t, ks):
    lines = [f"-- {t.name}: {t.purpose}", f"-- Query: {t.query}"]
    body = [f"    {c.cql} {c.type}" for c in t.columns]
    part = ", ".join(t.partition_key)
    part = f"({part})" if len(t.partition_key) > 1 else part       # composite partition key
    pk = "(" + part + (", " + ", ".join(t.clustering_key) if t.clustering_key else "") + ")"
    body.append(f"    PRIMARY KEY {pk}")
    lines.append(f"CREATE TABLE IF NOT EXISTS {ks}.{t.name} (")
    lines.append(",\n".join(body))
    lines.append(");")
    return "\n".join(lines)


def generate_cql(keyspace=KEYSPACE_DEFAULT):
    head = [
        "-- Apache Cassandra schema for the Formula 1 analysis project.",
        "-- Generated by `python cassandra_schema.py --write` - edit cassandra_schema.py, not this file.",
        "-- Safe to run repeatedly: every statement uses IF NOT EXISTS, nothing is dropped or truncated.",
        "-- SimpleStrategy / replication_factor 1 is for a single-node development machine only.",
        "",
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace}",
        "    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
        "",
    ]
    parts = ["\n".join(head)]
    for t in TABLES:
        parts.append(_table_cql(t, keyspace))
    parts.append("-- ingestion_log: one row per dataset table, written by the ingestion script after a load.\n"
                 f"CREATE TABLE IF NOT EXISTS {keyspace}.{INGESTION_LOG} (\n"
                 "    table_name text PRIMARY KEY,\n    source_file text,\n    source_sha256 text,\n"
                 "    rows_read int,\n    rows_written int,\n    rows_skipped int,\n    status text,\n"
                 "    message text,\n    finished_at timestamp\n);")
    return "\n\n".join(parts) + "\n"


def split_statements(cql_text):
    """Split a .cql file into single statements (comment lines removed)."""
    cleaned = "\n".join(ln for ln in cql_text.splitlines() if not ln.strip().startswith("--"))
    return [s.strip() for s in cleaned.split(";") if s.strip()]


if __name__ == "__main__":
    text = generate_cql()
    if "--write" in sys.argv:
        CQL_SCHEMA_FILE.parent.mkdir(parents=True, exist_ok=True)
        CQL_SCHEMA_FILE.write_text(text, encoding="utf-8", newline="\n")
        print(f"Wrote {CQL_SCHEMA_FILE}")
    else:
        print(text)
