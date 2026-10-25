"""Apache Spark <-> Apache Cassandra. No Streamlit code here.

Spark reads the Cassandra tables through the DataStax Spark Cassandra Connector
(`format("org.apache.spark.sql.cassandra")`). Cassandra has no joins, so the relationships
(results -> drivers, constructors, races) are re-created HERE, in Spark, using the identifiers that
were stored in the tables. The aggregation code is the same code the CSV pipeline uses
(`spark_processor`), applied to DataFrames whose source is Cassandra - never to the CSV files.
"""
import logging

import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

import spark_processor as sp
from cassandra_schema import TABLES_BY_NAME
from config import get_cassandra_settings

log = logging.getLogger(__name__)
SOURCE_FORMAT = "org.apache.spark.sql.cassandra"

# Cassandra tables Spark needs for the analyses, keyed by the CSV-style name used in spark_processor.
ANALYSIS_TABLES = {"drivers": "drivers", "constructors": "constructors", "races": "races",
                   "results": "results_by_race", "pit_stops": "pit_stops_by_race"}


class CassandraSparkError(RuntimeError):
    """User-friendly error for connector / connection / missing-table problems."""


def explain_error(exc, settings=None):
    """Turn a long JVM stack trace into one actionable sentence."""
    s = settings or get_cassandra_settings()
    text = str(exc)
    low = text.lower()
    if "failed to find data source" in low or "classnotfound" in low or "datasource" in low and "cassandra" in low:
        why = sp.CONNECTOR_STATUS.get("error") or (
            "Spark was started without the Cassandra connector (SPARK_CASSANDRA_ENABLED=false?).")
        return f"The Spark Cassandra Connector is not loaded. {why}"
    if "nohostavailable" in low or "failed to open native connection" in low or "connection refused" in low:
        return (f"Spark could not reach Cassandra at {s.host_string}:{s.port}. Start it with "
                "`docker compose up -d`, wait until it is healthy, then press Refresh.")
    if "keyspace" in low and ("not found" in low or "does not exist" in low or "unknown" in low):
        return f"Keyspace `{s.keyspace}` not found. Run `python create_schema.py` first."
    if "table or view not found" in low or "unknown table" in low or "table" in low and "not found" in low:
        return "A Cassandra table is missing. Run `python create_schema.py` and then `python ingest_to_cassandra.py`."
    if "incompatible" in low or "nosuchmethoderror" in low or "unsupportedclassversion" in low:
        return ("Spark / connector / Java versions are not compatible. Use PySpark 3.5.x with Java 17 and "
                f"connector 3.5.x (see README). Details: {text[:300]}")
    return f"{type(exc).__name__}: {text[:400]}"


def read_table(spark, table, settings=None):
    """Read one Cassandra table as a Spark DataFrame and restore the CSV column names."""
    s = settings or get_cassandra_settings()
    spec = TABLES_BY_NAME[table]
    df = spark.read.format(SOURCE_FORMAT).options(table=table, keyspace=s.keyspace).load()
    for col in spec.columns:
        if col.cql != col.csv and col.cql in df.columns:
            df = df.withColumnRenamed(col.cql, col.csv)
    return df


def load_frames(spark, settings=None):
    """Raw DataFrames (CSV-style names) for the tables the analyses need. Lazy: nothing is read yet."""
    return {name: read_table(spark, table, settings) for name, table in ANALYSIS_TABLES.items()}


def totals(full):
    """A few control totals used to compare the Cassandra pipeline with the CSV pipeline."""
    row = full.agg(F.count("*").alias("result_rows"), F.countDistinct("raceId").alias("races"),
                   F.coalesce(F.sum("points"), F.lit(0.0)).alias("total_points"),
                   F.sum(F.when(F.col("classified") & (F.col("positionOrder") == 1), 1).otherwise(0)).alias("wins")
                   ).first()
    return {k: row[k] for k in ("result_rows", "races", "total_points", "wins")}


def constructor_season_leaders(full):
    """Season-wise constructor performance: the top-scoring constructor of every season."""
    w = Window.partitionBy("year").orderBy(F.desc("points"), F.desc("wins"), "constructorName")
    per = full.groupBy("year", "constructorName").agg(sp._points(), sp._win())
    return (per.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") == 1)
            .select("year", "constructorName", "points", "wins").orderBy("year"))


def run_analytics(spark, settings=None):
    """Run all Cassandra-sourced Spark analyses. Returns small pandas objects for the dashboard.

    Raises CassandraSparkError with a clear message if Spark cannot read Cassandra.
    """
    try:
        raw = load_frames(spark, settings)
        clean = sp.clean_data(raw)
        full = sp.join_results(clean).cache()
        cass_totals = totals(full)            # first action: this is where Cassandra is actually read
        if cass_totals["result_rows"] == 0:
            raise CassandraSparkError("The Cassandra tables are empty. Run `python ingest_to_cassandra.py` first.")
        return {
            "totals": cass_totals,
            # 1) driver performance: aggregate real race results read from Cassandra
            "driver_performance": sp.driver_stats(full).toPandas(),
            # 2) constructor performance / season statistics from Cassandra
            "constructor_season_points": sp.season_points(full, "constructor").toPandas(),
            "constructor_season_leaders": constructor_season_leaders(full).toPandas(),
            "season_summary": sp.season_summary(full).toPandas(),
            # 3) pit stops per season (second Cassandra table + join with races on race_id)
            "pit_by_season": sp.pit_by_season(clean["pit_stops"], clean["races"]).toPandas(),
        }
    except CassandraSparkError:
        raise
    except Exception as exc:  # noqa: BLE001 - Py4J errors are wide; translate them
        raise CassandraSparkError(explain_error(exc, settings)) from exc


def compare_with_csv(cass_totals, csv_full):
    """Control totals from Cassandra vs the same totals computed from the CSV pipeline."""
    csv_totals = totals(csv_full)
    rows = []
    for key in ("result_rows", "races", "total_points", "wins"):
        a, b = csv_totals[key], cass_totals[key]
        rows.append({"metric": key, "from CSV (Spark)": a, "from Cassandra (Spark)": b,
                     "match": "yes" if abs(float(a) - float(b)) < 1e-6 else "NO"})
    return pd.DataFrame(rows)


def count_table(spark, table, settings=None):
    """Exact row count through Spark (works for tables too large for CQL COUNT(*))."""
    try:
        return read_table(spark, table, settings).count()
    except Exception as exc:  # noqa: BLE001
        raise CassandraSparkError(explain_error(exc, settings)) from exc
