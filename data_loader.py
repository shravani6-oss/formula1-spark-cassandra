"""Dataset locations, verified CSV schemas, pandas-based validation and Spark CSV loading.

The schemas below were checked against the real header rows of the supplied Kaggle CSV files.
Spark's CSV reader with an explicit schema maps columns BY POSITION, so every schema lists the
columns in exactly the same order as the file. `validate_dataset` re-checks this on every run.

The Kaggle files mark missing values with the literal text `\\N`; that token is treated as null.
"""
from pathlib import Path

import pandas as pd

from config import data_dir

DATA_DIR = data_dir()
NULL_TOKEN = "\\N"
I, L, D, S = "int", "long", "double", "string"   # plain type names; Spark types are built lazily

DATASET_NOTE = (
    "Analysis is based on the supplied historical Formula 1 CSV dataset (Kaggle download, 14 CSV files). "
    "The dataset's exact title, author, source URL and licence are NOT stated in the supplied files and "
    "could not be verified, so they are not claimed here.")


def _schema(*cols):
    return list(cols)


# Column order == column order of the real CSV header.
COLUMNS = {
    "circuits": _schema(("circuitId", I), ("circuitRef", S), ("name", S), ("location", S), ("country", S),
                        ("lat", D), ("lng", D), ("alt", D), ("url", S)),
    "constructor_results": _schema(("constructorResultsId", I), ("raceId", I), ("constructorId", I),
                                   ("points", D), ("status", S)),
    "constructor_standings": _schema(("constructorStandingsId", I), ("raceId", I), ("constructorId", I),
                                     ("points", D), ("position", I), ("positionText", S), ("wins", I)),
    "constructors": _schema(("constructorId", I), ("constructorRef", S), ("name", S), ("nationality", S),
                            ("url", S)),
    "driver_standings": _schema(("driverStandingsId", I), ("raceId", I), ("driverId", I), ("points", D),
                                ("position", I), ("positionText", S), ("wins", I)),
    "drivers": _schema(("driverId", I), ("driverRef", S), ("number", I), ("code", S), ("forename", S),
                       ("surname", S), ("dob", S), ("nationality", S), ("url", S)),
    # `time` is the lap time text (m:ss.SSS); `milliseconds` is the numeric lap time.
    "lap_times": _schema(("raceId", I), ("driverId", I), ("lap", I), ("position", I), ("time", S),
                         ("milliseconds", L)),
    # `duration` is TEXT in the real file (seconds like "23.4" OR minutes like "1:09.764"), so the
    # numeric `milliseconds` column is the reliable one and is what the dashboard uses.
    "pit_stops": _schema(("raceId", I), ("driverId", I), ("stop", I), ("lap", I), ("time", S),
                         ("duration", S), ("milliseconds", L)),
    "qualifying": _schema(("qualifyId", I), ("raceId", I), ("driverId", I), ("constructorId", I),
                          ("number", I), ("position", I), ("q1", S), ("q2", S), ("q3", S)),
    "races": _schema(("raceId", I), ("year", I), ("round", I), ("circuitId", I), ("name", S), ("date", S),
                     ("time", S), ("url", S), ("fp1_date", S), ("fp1_time", S), ("fp2_date", S),
                     ("fp2_time", S), ("fp3_date", S), ("fp3_time", S), ("quali_date", S),
                     ("quali_time", S), ("sprint_date", S), ("sprint_time", S)),
    "results": _schema(("resultId", I), ("raceId", I), ("driverId", I), ("constructorId", I), ("number", I),
                       ("grid", I), ("position", I), ("positionText", S), ("positionOrder", I),
                       ("points", D), ("laps", I), ("time", S), ("milliseconds", L), ("fastestLap", I),
                       ("rank", I), ("fastestLapTime", S), ("fastestLapSpeed", D), ("statusId", I)),
    "seasons": _schema(("year", I), ("url", S)),
    "sprint_results": _schema(("resultId", I), ("raceId", I), ("driverId", I), ("constructorId", I),
                              ("number", I), ("grid", I), ("position", I), ("positionText", S),
                              ("positionOrder", I), ("points", D), ("laps", I), ("time", S),
                              ("milliseconds", L), ("fastestLap", I), ("fastestLapTime", S),
                              ("statusId", I), ("rank", I)),
    "status": _schema(("statusId", I), ("status", S)),
}

# The six files the original dashboard needs. All other files are optional: pages that depend on
# them show a message instead of failing.
REQUIRED = ["drivers", "constructors", "races", "results", "lap_times", "pit_stops"]
OPTIONAL = [n for n in COLUMNS if n not in REQUIRED]

# Logical key of each file (used for duplicate detection).
KEYS = {
    "circuits": ["circuitId"], "constructor_results": ["constructorResultsId"],
    "constructor_standings": ["constructorStandingsId"], "constructors": ["constructorId"],
    "driver_standings": ["driverStandingsId"], "drivers": ["driverId"],
    "lap_times": ["raceId", "driverId", "lap"], "pit_stops": ["raceId", "driverId", "stop"],
    "qualifying": ["qualifyId"], "races": ["raceId"], "results": ["resultId"], "seasons": ["year"],
    "sprint_results": ["resultId"], "status": ["statusId"],
}


def csv_path(name, directory=None):
    return Path(directory or DATA_DIR) / f"{name}.csv"


def missing_files(data_dir_=None, names=REQUIRED):
    """Names of the required CSV files that do not exist."""
    return [f"{n}.csv" for n in names if not csv_path(n, data_dir_).exists()]


def available_files(data_dir_=None):
    return [n for n in COLUMNS if csv_path(n, data_dir_).exists()]


# ---------------------------------------------------------------- validation (pandas, no Spark)
def column_names(name):
    return [c for c, _ in COLUMNS[name]]


def _numeric_cols(name):
    return [c for c, t in COLUMNS[name] if t in (I, L, D)]


def spark_schema(name, only=None):
    """Spark StructType for a file (imported lazily so validation works without PySpark).
    `only` restricts the schema to a subset of columns (handy in unit tests)."""
    from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                                   StructField, StructType)
    types = {I: IntegerType(), L: LongType(), D: DoubleType(), S: StringType()}
    return StructType([StructField(c, types[t], True) for c, t in COLUMNS[name] if only is None or c in only])


def validate_file(name, data_dir_=None):
    """Check one CSV against its schema. Never raises for bad data; returns a report dict."""
    path = csv_path(name, data_dir_)
    rep = {"file": f"{name}.csv", "required": name in REQUIRED, "exists": path.exists(), "rows": 0,
           "header_ok": False, "missing_columns": "", "extra_columns": "", "null_cells": 0,
           "duplicate_keys": 0, "bad_numeric_cells": 0, "status": "missing"}
    if not rep["exists"]:
        return rep
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[NULL_TOKEN, ""],
                         encoding="utf-8")
    except Exception as exc:  # noqa: BLE001 - corrupt file must not crash the dashboard
        rep["status"] = f"unreadable: {exc}"
        return rep
    expected = column_names(name)
    rep["rows"] = len(df)
    rep["header_ok"] = list(df.columns) == expected
    rep["missing_columns"] = ", ".join(c for c in expected if c not in df.columns)
    rep["extra_columns"] = ", ".join(c for c in df.columns if c not in expected)
    rep["null_cells"] = int(df.isna().sum().sum())
    keys = [k for k in KEYS[name] if k in df.columns]
    rep["duplicate_keys"] = int(df.duplicated(keys).sum()) if len(keys) == len(KEYS[name]) else 0
    bad = 0
    for col in _numeric_cols(name):
        if col in df.columns:
            bad += int((pd.to_numeric(df[col], errors="coerce").isna() & df[col].notna()).sum())
    rep["bad_numeric_cells"] = bad
    rep["status"] = "ok" if rep["header_ok"] and bad == 0 else "check"
    return rep


def validate_dataset(data_dir_=None):
    """Validation table for every known CSV (a pandas DataFrame, safe to show in the UI)."""
    return pd.DataFrame([validate_file(n, data_dir_) for n in COLUMNS])


# ---------------------------------------------------------------- Spark loading
def load_csvs(spark, data_dir_=None):
    """Load every available CSV into a Spark DataFrame using the explicit schemas."""
    directory = data_dir_ or DATA_DIR
    missing = missing_files(directory)
    if missing:
        raise FileNotFoundError(f"Missing data files in {directory}: {', '.join(missing)}")
    return {
        name: spark.read.csv(str(csv_path(name, directory).resolve()), header=True, schema=spark_schema(name),
                             nullValue=NULL_TOKEN, mode="PERMISSIVE")
        for name in available_files(directory)
    }
