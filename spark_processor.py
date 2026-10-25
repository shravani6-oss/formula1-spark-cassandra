"""Spark processing layer. No Streamlit code here.

The same cleaning / join / aggregation functions are used for DataFrames loaded from the CSV files
AND for DataFrames read from Apache Cassandra (see cassandra_spark.py), so both paths follow
exactly the same business rules.
"""
import logging
import os
import sys
import time

import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

import config
from data_loader import DATA_DIR, load_csvs

log = logging.getLogger(__name__)

# Pit stops longer than this (red-flag stops, repairs, data glitches) are kept in the data and in the
# stop counts, but excluded from duration averages / minima / maxima. 120 s is a documented choice.
PIT_MAX_SEC = 120.0

RESULT_COLS = ["resultId", "raceId", "driverId", "constructorId", "year", "round", "race_label",
               "driverName", "constructorName", "grid", "position", "positionOrder", "classified",
               "points", "laps", "milliseconds", "fastestLapTime", "statusId"]

SQL_QUERIES = {
    "Driver wins and starts (top 25)": """SELECT driverName,
       SUM(CASE WHEN classified AND positionOrder = 1 THEN 1 ELSE 0 END) AS wins,
       COUNT(*) AS starts
FROM race_results GROUP BY driverName ORDER BY wins DESC, driverName LIMIT 25""",
    "Constructor points per season (2020 onwards)": """SELECT year, constructorName, SUM(points) AS points
FROM race_results WHERE year >= 2020 GROUP BY year, constructorName ORDER BY year, points DESC""",
    "Average places gained vs grid (min. 50 races)": """SELECT driverName, ROUND(AVG(grid - positionOrder), 2) AS avg_places_gained,
       COUNT(*) AS races
FROM race_results WHERE classified AND grid > 0
GROUP BY driverName HAVING COUNT(*) >= 50 ORDER BY avg_places_gained DESC LIMIT 25""",
    "Average lap time by driver (JOIN, min. 1000 laps)": """SELECT d.driverName, ROUND(AVG(l.seconds), 3) AS avg_lap_sec,
       ROUND(MIN(l.seconds), 3) AS best_lap_sec, COUNT(*) AS laps
FROM lap_times l JOIN drivers d ON l.driverId = d.driverId
GROUP BY d.driverName HAVING COUNT(*) >= 1000 ORDER BY avg_lap_sec LIMIT 25""",
}


# ---------------------------------------------------------------- session
CONNECTOR_STATUS = {"requested": False, "loaded": False, "error": None, "package": None}


def _build_session(app_name, with_connector):
    builder = (SparkSession.builder.master("local[*]").appName(app_name)
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.driver.bindAddress", "127.0.0.1")
               .config("spark.driver.host", "127.0.0.1"))
    if with_connector:
        s = config.get_cassandra_settings()
        jar = config.connector_jar()
        if jar is not None:
            if not jar.exists():
                raise FileNotFoundError(f"SPARK_CASSANDRA_CONNECTOR_JAR points to a missing file: {jar}")
            builder = builder.config("spark.jars", str(jar))
            CONNECTOR_STATUS["package"] = str(jar)
        else:
            builder = builder.config("spark.jars.packages", config.connector_package())
            CONNECTOR_STATUS["package"] = config.connector_package()
        builder = (builder.config("spark.cassandra.connection.host", s.host_string)
                   .config("spark.cassandra.connection.port", str(s.port)))
        if s.uses_auth:      # credentials come from the environment, never from source code
            builder = (builder.config("spark.cassandra.auth.username", s.username)
                       .config("spark.cassandra.auth.password", s.password))
    return builder.getOrCreate()


def get_spark(app_name="F1Analysis"):
    """Create (or reuse) a local SparkSession.

    The Spark Cassandra Connector is requested at start-up (Spark downloads it from Maven Central
    the first time, so the first start needs internet). If that fails, Spark is started again WITHOUT
    the connector so the six CSV-based pages keep working; the reason is stored in CONNECTOR_STATUS
    and shown on the "Cassandra Database" page.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    want = config.spark_cassandra_enabled()
    CONNECTOR_STATUS.update(requested=want, loaded=False, error=None)
    spark = None
    if want:
        try:
            spark = _build_session(app_name, True)
            CONNECTOR_STATUS["loaded"] = True
        except Exception as exc:  # noqa: BLE001
            CONNECTOR_STATUS["error"] = (
                "Spark could not start with the Cassandra connector (needs Java 17 and, on first use, internet "
                f"access to Maven Central; or set SPARK_CASSANDRA_CONNECTOR_JAR). Details: {exc}")
            log.warning(CONNECTOR_STATUS["error"])
    if spark is None:
        try:
            spark = _build_session(app_name, False)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("Could not start Spark. Check Java 17 (`java -version`) and JAVA_HOME. "
                               f"Details: {exc}") from exc
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------------------------------------------------------------- cleaning
def clean_data(raw):
    """Drop invalid records, keep missing values as null, add derived columns.

    Works on whichever tables are present in `raw`, so it can be used for the CSV files and for the
    (smaller) set of tables read back from Cassandra.
    """
    c = {}
    if "drivers" in raw:
        c["drivers"] = (raw["drivers"].dropna(subset=["driverId"]).dropDuplicates(["driverId"])
                        .withColumn("driverName", F.trim(F.concat_ws(" ", "forename", "surname"))))
    if "constructors" in raw:
        c["constructors"] = raw["constructors"].dropna(subset=["constructorId"]).dropDuplicates(["constructorId"])
    if "circuits" in raw:
        c["circuits"] = raw["circuits"].dropna(subset=["circuitId"]).dropDuplicates(["circuitId"])
    if "races" in raw:
        c["races"] = (raw["races"].dropna(subset=["raceId", "year"]).dropDuplicates(["raceId"])
                      .withColumn("date", F.to_date("date")))
    if "results" in raw:
        # classified = has a finishing position and was not disqualified (statusId 2)
        classified = F.coalesce((F.col("position") > 0) & (F.coalesce(F.col("statusId"), F.lit(0)) != 2),
                                F.lit(False))
        fl = F.col("fastestLapTime")
        c["results"] = (raw["results"]
                        .dropna(subset=["resultId", "raceId", "driverId", "constructorId", "positionOrder"])
                        .dropDuplicates(["resultId"]).filter(F.col("positionOrder") > 0)
                        .withColumn("points", F.when(F.col("points") >= 0, F.col("points")))
                        .withColumn("milliseconds", F.when(F.col("milliseconds") > 0, F.col("milliseconds")))
                        .withColumn("classified", classified)
                        .withColumn("fastest_lap_sec",
                                    F.when(fl.rlike(r"^\d+:\d{2}(\.\d+)?$"),
                                           F.split(fl, ":")[0].cast("double") * 60 + F.split(fl, ":")[1].cast("double"))))
    if "lap_times" in raw:
        # The real file has duplicate (race, driver, lap) keys: keep the FIRST occurrence in file order,
        # which is exactly what the Cassandra ingestion script keeps too.
        w = Window.partitionBy("raceId", "driverId", "lap").orderBy("_ord")
        c["lap_times"] = (raw["lap_times"].dropna(subset=["raceId", "driverId", "lap", "milliseconds"])
                          .filter(F.col("milliseconds") > 0)
                          .withColumn("_ord", F.monotonically_increasing_id())
                          .withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1)
                          .drop("_ord", "_rn")
                          .withColumn("seconds", F.col("milliseconds") / 1000.0))
    if "pit_stops" in raw:
        # `duration` is text in the real file ("23.4" or "1:09.764"); `milliseconds` is reliable.
        c["pit_stops"] = (raw["pit_stops"].dropna(subset=["raceId", "driverId", "stop", "milliseconds"])
                          .filter(F.col("milliseconds") > 0).dropDuplicates(["raceId", "driverId", "stop"])
                          .withColumn("duration", F.col("milliseconds") / 1000.0)
                          .withColumn("valid_duration", F.when(F.col("duration") <= PIT_MAX_SEC, F.col("duration"))))
    for name in ("qualifying", "driver_standings", "constructor_standings", "status", "seasons"):
        if name in raw:
            c[name] = raw[name]
    return c


def join_results(clean):
    """Join results with drivers, constructors and races (inner joins drop orphan keys)."""
    d = clean["drivers"].select("driverId", "driverName", "driverRef")
    k = clean["constructors"].select("constructorId", F.col("name").alias("constructorName"))
    r = clean["races"].select("raceId", "year", "round", F.col("name").alias("raceName"), "date")
    return (clean["results"].join(d, "driverId").join(k, "constructorId").join(r, "raceId")
            .withColumn("race_label", F.concat_ws(" ", F.col("year").cast("string"), F.col("raceName"))))


def register_views(clean, full):
    for name, df in clean.items():
        df.createOrReplaceTempView(name)
    full.createOrReplaceTempView("race_results")


def prepare(spark, data_dir=DATA_DIR):
    """Load, clean, join, register SQL views, and time the pipeline."""
    t0 = time.perf_counter()
    spark.catalog.clearCache()
    raw = load_csvs(spark, data_dir)
    clean = clean_data(raw)
    clean["lap_times"] = clean["lap_times"].cache()      # window de-duplication is costly: do it once
    clean["pit_stops"] = clean["pit_stops"].cache()
    full = join_results(clean).cache()
    register_views(clean, full)
    rows = full.count()
    clean["lap_times"].count()
    clean["pit_stops"].count()
    meta = {"spark_version": spark.version, "master": spark.sparkContext.master,
            "cores": spark.sparkContext.defaultParallelism, "joined_rows": rows,
            "seconds": round(time.perf_counter() - t0, 2)}
    return {"spark": spark, "raw": raw, "clean": clean, "full": full, "meta": meta}


# ---------------------------------------------------------------- filters / metrics
def filter_results(full, years=(), driver_ids=(), constructor_ids=(), race_ids=()):
    df = full
    for col, vals in (("year", years), ("driverId", driver_ids),
                      ("constructorId", constructor_ids), ("raceId", race_ids)):
        if vals:
            df = df.filter(F.col(col).isin(list(vals)))
    return df


def _sel(df, race_ids=(), driver_ids=()):
    if race_ids:
        df = df.filter(F.col("raceId").isin(list(race_ids)))
    if driver_ids:
        df = df.filter(F.col("driverId").isin(list(driver_ids)))
    return df


def _win():
    return F.sum(F.when(F.col("classified") & (F.col("positionOrder") == 1), 1).otherwise(0)).alias("wins")


def _points():
    return F.coalesce(F.sum("points"), F.lit(0.0)).alias("points")


def _metrics():
    return [_win(),
            F.sum(F.when(F.col("classified") & (F.col("positionOrder") <= 3), 1).otherwise(0)).alias("podiums"),
            _points(),
            F.round(F.avg(F.when(F.col("classified"), F.col("positionOrder"))), 2).alias("avg_finish"),
            F.countDistinct("raceId").alias("races")]


def driver_stats(full):
    return (full.groupBy("driverId", "driverName")
            .agg(*_metrics(), F.round(F.min("fastest_lap_sec"), 3).alias("best_lap_sec"))
            .orderBy(F.desc("wins"), F.desc("points"), "driverName"))


def constructor_stats(full):
    return (full.groupBy("constructorId", "constructorName").agg(*_metrics())
            .orderBy(F.desc("points"), F.desc("wins"), "constructorName"))


def driver_wins(full):
    return driver_stats(full).select("driverName", "wins")


def constructor_wins(full):
    return constructor_stats(full).select("constructorName", "wins").orderBy(F.desc("wins"))


def avg_finishing_position(full):
    return (full.filter("classified").groupBy("driverName")
            .agg(F.round(F.avg("positionOrder"), 2).alias("avg_finish")).orderBy("avg_finish"))


def season_points(full, by="driver"):
    key = "constructorName" if by == "constructor" else "driverName"
    return full.groupBy("year", key).agg(_points(), _win()).orderBy("year", F.desc("points"))


def driver_contributions(full):
    return (full.groupBy("year", "constructorName", "driverName").agg(_points(), _win())
            .orderBy("year", "constructorName", F.desc("points")))


def race_winners(full):
    return (full.filter(F.col("classified") & (F.col("positionOrder") == 1))
            .select("year", "round", "race_label", "driverName", "constructorName", "points",
                    (F.col("milliseconds") / 1000.0).alias("race_time_sec")).orderBy("year", "round"))


def season_summary(full):
    """One row per season: races, entries, points, distinct winners (used by the Cassandra comparison too)."""
    return (full.groupBy("year")
            .agg(F.countDistinct("raceId").alias("races"), F.count("*").alias("entries"),
                 F.coalesce(F.sum("points"), F.lit(0.0)).alias("total_points"),
                 F.countDistinct(F.when(F.col("classified") & (F.col("positionOrder") == 1), F.col("driverId")))
                 .alias("different_winners")).orderBy("year"))


# ---------------------------------------------------------------- standings / qualifying
def season_champions(clean, kind="driver"):
    """Final championship standing (position 1 after the last race of each season with standings)."""
    table = "driver_standings" if kind == "driver" else "constructor_standings"
    if table not in clean:
        return None
    key, names = ("driverId", clean["drivers"].select("driverId", "driverName")) if kind == "driver" else \
        ("constructorId", clean["constructors"].select("constructorId", F.col("name").alias("constructorName")))
    name_col = "driverName" if kind == "driver" else "constructorName"
    s = clean[table].join(clean["races"].select("raceId", "year", "round"), "raceId")
    last = s.groupBy("year").agg(F.max("round").alias("round"))
    return (s.join(last, ["year", "round"]).filter(F.col("position") == 1)
            .join(names, key).select("year", name_col, "points", "wins").orderBy("year"))


def qualifying_vs_race(clean, filtered_full):
    """Qualifying position vs race finishing position for classified results of the filtered data."""
    if "qualifying" not in clean:
        return None
    q = clean["qualifying"].filter(F.col("position").isNotNull()) \
        .select("raceId", "driverId", F.col("position").alias("quali_position"))
    return (filtered_full.filter("classified").join(q, ["raceId", "driverId"])
            .select("race_label", "driverName", "constructorName", "quali_position", "positionOrder",
                    (F.col("quali_position") - F.col("positionOrder")).alias("places_gained"))
            .orderBy("race_label", "positionOrder"))


# ---------------------------------------------------------------- laps / pits
def lap_summary(laps, drivers, race_ids=(), driver_ids=()):
    return (_sel(laps, race_ids, driver_ids).groupBy("raceId", "driverId")
            .agg(F.count("*").alias("laps"), F.round(F.avg("seconds"), 3).alias("avg_sec"),
                 F.round(F.min("seconds"), 3).alias("best_sec"), F.round(F.stddev_samp("seconds"), 3).alias("std_sec"))
            .join(drivers.select("driverId", "driverName"), "driverId").orderBy("raceId", "avg_sec"))


def flag_slow_laps(laps, drivers, race_ids=(), driver_ids=(), k=2.0):
    """Flag laps slower than mean + k * sample std-dev, per driver per race."""
    w = Window.partitionBy("raceId", "driverId")
    df = (_sel(laps, race_ids, driver_ids)
          .withColumn("mean_sec", F.round(F.avg("seconds").over(w), 3))
          .withColumn("std_sec", F.stddev_samp("seconds").over(w))
          .withColumn("threshold_sec", F.round(F.col("mean_sec") + k * F.col("std_sec"), 3))
          .withColumn("is_slow", F.coalesce(F.col("seconds") > F.col("threshold_sec"), F.lit(False))))
    return df.join(drivers.select("driverId", "driverName"), "driverId").orderBy("raceId", "driverName", "lap")


def pit_summary(pits, drivers, race_ids=(), driver_ids=()):
    """Stops are counted in full; durations only use stops of at most PIT_MAX_SEC seconds."""
    return (_sel(pits, race_ids, driver_ids).groupBy("driverId")
            .agg(F.count("*").alias("stops"), F.round(F.avg("valid_duration"), 3).alias("avg_duration"),
                 F.round(F.min("valid_duration"), 3).alias("fastest_stop"),
                 F.round(F.max("valid_duration"), 3).alias("slowest_stop"))
            .join(drivers.select("driverId", "driverName"), "driverId").orderBy("avg_duration"))


def pit_vs_position(pits, full, race_ids=(), driver_ids=()):
    p = (_sel(pits, race_ids, driver_ids).groupBy("raceId", "driverId")
         .agg(F.count("*").alias("stops"), F.round(F.avg("valid_duration"), 3).alias("avg_duration"))
         .filter(F.col("avg_duration").isNotNull()))
    f = full.filter("classified").select("raceId", "driverId", "driverName", "race_label", "positionOrder")
    return p.join(f, ["raceId", "driverId"]).orderBy("raceId", "positionOrder")


def pit_by_season(pits, races):
    """Pit-stop statistics per season (works for CSV and Cassandra-sourced frames)."""
    return (pits.join(races.select("raceId", "year"), "raceId").groupBy("year")
            .agg(F.count("*").alias("stops"), F.round(F.avg("valid_duration"), 3).alias("avg_duration"),
                 F.round(F.percentile_approx("valid_duration", 0.5), 3).alias("median_duration"),
                 F.round(F.min("valid_duration"), 3).alias("fastest_stop")).orderBy("year"))


# ---------------------------------------------------------------- overview / spark page
def overview_kpis(clean, full):
    raced = full.select("raceId").distinct().count()
    k = {"races": raced, "calendar_races": clean["races"].count(), "drivers": clean["drivers"].count(),
         "constructors": clean["constructors"].count(),
         "circuits": clean["circuits"].count() if "circuits" in clean else None,
         "seasons": clean["races"].select("year").distinct().count(),
         "fastest_lap": None, "top_driver": None, "top_constructor": None}
    lap = (clean["lap_times"].join(clean["drivers"].select("driverId", "driverName"), "driverId")
           .join(clean["races"].select("raceId", F.col("name").alias("raceName"), "year"), "raceId")
           .orderBy("seconds").limit(1).collect())
    if lap:
        k["fastest_lap"] = {"seconds": lap[0]["seconds"], "driver": lap[0]["driverName"],
                            "race": f'{lap[0]["year"]} {lap[0]["raceName"]}'}
    for key, df, col in (("top_driver", driver_stats(full), "driverName"),
                         ("top_constructor", constructor_stats(full), "constructorName")):
        top = df.orderBy(F.desc("wins"), F.desc("points")).limit(1).collect()
        if top and top[0]["wins"] and top[0]["wins"] > 0:
            k[key] = {"name": top[0][col], "wins": top[0]["wins"]}
    yrs = full.agg(F.min("year"), F.max("year")).first()
    k["first_year"], k["last_year"] = yrs[0], yrs[1]
    return k


def operation_demos(full):
    """Name -> (code snippet, DataFrame) for filter / join / groupBy / agg demonstrations."""
    yr = full.agg(F.max("year")).first()[0]
    return {
        "filter(): latest-season podiums": (
            "full.filter((year == latest) & classified & (positionOrder <= 3))",
            full.filter((F.col("year") == yr) & F.col("classified") & (F.col("positionOrder") <= 3))
            .select("race_label", "driverName", "constructorName", "positionOrder", "points")
            .orderBy("race_label", "positionOrder")),
        "join(): results + drivers + constructors + races": (
            "results.join(drivers,'driverId').join(constructors,'constructorId').join(races,'raceId')",
            full.select("race_label", "driverName", "constructorName", "grid", "positionOrder", "points").limit(8)),
        "groupBy(): entries and points per season": (
            "full.groupBy('year').agg(countDistinct('raceId'), count('*'), sum('points'))",
            full.groupBy("year").agg(F.countDistinct("raceId").alias("races"), F.count("*").alias("entries"),
                                     F.sum("points").alias("total_points")).orderBy("year")),
        "agg(): overall summary": (
            "full.agg(count('*'), avg('points'), max('points'), avg('milliseconds'))",
            full.agg(F.count("*").alias("entries"), F.round(F.avg("points"), 2).alias("avg_points"),
                     F.max("points").alias("max_points"),
                     F.round(F.avg("milliseconds") / 1000.0, 1).alias("avg_race_time_sec"))),
    }


def spark_overview(ctx):
    """Everything the Spark Analytics page needs, as small pandas objects."""
    raw, clean, full, spark = ctx["raw"], ctx["clean"], ctx["full"], ctx["spark"]
    counts = pd.DataFrame([{"dataset": k, "columns": len(v.columns), "raw_rows": v.count(),
                            "clean_rows": clean[k].count() if k in clean else None} for k, v in raw.items()])
    schemas = {k: pd.DataFrame([(f.name, f.dataType.simpleString()) for f in v.schema.fields],
                               columns=["column", "type"]) for k, v in raw.items()}
    return {"counts": counts, "schemas": schemas, "preview": full.limit(10).toPandas(),
            "sql": {n: (q, spark.sql(q).toPandas()) for n, q in SQL_QUERIES.items()},
            "ops": {n: (code, df.toPandas()) for n, (code, df) in operation_demos(full).items()}}
