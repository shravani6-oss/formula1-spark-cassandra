"""Spark tests. They need Java 17 + PySpark (see README). Small in-memory fixtures check the rules;
`test_real_dataset_matches_pandas` recomputes key numbers from the real CSV files with pandas and
compares them with the Spark results."""
import pandas as pd
import pytest

pytest.importorskip("pyspark")

import data_loader as dl  # noqa: E402
import spark_processor as sp  # noqa: E402

DRIVER_COLS = ["driverId", "driverRef", "forename", "surname"]
CONSTRUCTOR_COLS = ["constructorId", "name"]
RACE_COLS = ["raceId", "year", "round", "name", "date"]
RESULT_COLS = ["resultId", "raceId", "driverId", "constructorId", "grid", "position", "positionOrder", "points",
               "laps", "milliseconds", "fastestLapTime", "statusId"]
LAP_COLS = ["raceId", "driverId", "lap", "position", "milliseconds"]
PIT_COLS = ["raceId", "driverId", "stop", "lap", "milliseconds"]

DRIVERS = [(1, "a", "Ann", "Able"), (2, "b", "Bob", "Baker")]
CONSTRUCTORS = [(1, "Red"), (2, "Blue")]
RACES = [(1, 2023, 1, "R1", "2023-03-01"), (2, 2023, 2, "R2", "2023-03-08"), (3, 2023, 3, "R3", "2023-03-15")]
RESULTS = [
    (1, 1, 1, 1, 1, 1, 1, 25.0, 10, 900000, "1:30.000", 1),
    (2, 1, 2, 2, 2, 2, 2, 18.0, 10, 905000, "1:30.500", 1),
    (3, 2, 1, 1, 2, 2, 2, 18.0, 10, 910000, None, 1),
    (4, 2, 2, 2, 1, 1, 1, 25.0, 10, 908000, "1:29.800", 1),
    (5, 3, 1, 1, 1, 1, 1, 25.0, 10, 901000, "1:30.100", 1),
    (6, 3, 2, 2, 2, 2, 2, 18.0, 10, 903000, "1:30.200", 1),
]
BAD_ROWS = [
    (7, 1, 1, 1, 3, None, 3, -5.0, 5, None, None, 2),   # disqualified, invalid negative points
    (8, 1, 99, 1, 3, 3, 3, 0.0, 10, 1000, None, 1),     # orphan driver id
    (9, 1, 2, 2, 3, 3, None, 0.0, 10, 1000, None, 1),   # missing positionOrder
]


@pytest.fixture(scope="session")
def spark():
    import os
    os.environ.setdefault("SPARK_CASSANDRA_ENABLED", "false")   # unit tests need no connector download
    s = sp.get_spark("f1-tests")
    yield s
    s.stop()


def frame(spark, name, cols, rows):
    return spark.createDataFrame(list(rows), dl.spark_schema(name, only=cols))


def build(spark, results=RESULTS, laps=(), pits=()):
    raw = {"drivers": frame(spark, "drivers", DRIVER_COLS, DRIVERS),
           "constructors": frame(spark, "constructors", CONSTRUCTOR_COLS, CONSTRUCTORS),
           "races": frame(spark, "races", RACE_COLS, RACES),
           "results": frame(spark, "results", RESULT_COLS, results),
           "lap_times": frame(spark, "lap_times", LAP_COLS, laps),
           "pit_stops": frame(spark, "pit_stops", PIT_COLS, pits)}
    clean = sp.clean_data(raw)
    return clean, sp.join_results(clean)


def test_driver_win_counts(spark):
    _, full = build(spark)
    wins = {r["driverName"]: r["wins"] for r in sp.driver_wins(full).collect()}
    assert wins == {"Ann Able": 2, "Bob Baker": 1}


def test_constructor_point_aggregation(spark):
    _, full = build(spark)
    pts = {r["constructorName"]: r["points"] for r in sp.constructor_stats(full).collect()}
    assert pts == {"Red": 68.0, "Blue": 61.0}


def test_average_finishing_position(spark):
    _, full = build(spark)
    avg = {r["driverName"]: r["avg_finish"] for r in sp.avg_finishing_position(full).collect()}
    assert avg["Ann Able"] == 1.33 and avg["Bob Baker"] == 1.67


def test_missing_value_handling(spark):
    _, full = build(spark, RESULTS + BAD_ROWS)
    assert full.count() == 7                      # orphan + missing positionOrder dropped
    dsq = full.filter("resultId = 7").first()
    assert dsq["points"] is None and dsq["classified"] is False
    wins = {r["driverName"]: r["wins"] for r in sp.driver_wins(full).collect()}
    assert wins == {"Ann Able": 2, "Bob Baker": 1}


def test_empty_datasets(spark):
    clean, full = build(spark, results=[])
    assert full.count() == 0
    assert sp.driver_stats(full).count() == 0
    assert sp.constructor_stats(full).count() == 0
    assert sp.avg_finishing_position(full).count() == 0
    k = sp.overview_kpis(clean, full)
    assert k["races"] == 0 and k["calendar_races"] == 3 and k["top_driver"] is None and k["fastest_lap"] is None


def test_filter_results(spark):
    _, full = build(spark)
    assert sp.filter_results(full, race_ids=(1,)).count() == 2
    assert sp.filter_results(full, driver_ids=(1,), constructor_ids=(1,)).count() == 3
    assert sp.filter_results(full, years=(1999,)).count() == 0


def test_slow_lap_detection(spark):
    laps = [(1, 1, n, 1, 90000) for n in range(1, 10)] + [(1, 1, 10, 1, 120000)]
    clean, _ = build(spark, laps=laps)
    flagged = sp.flag_slow_laps(clean["lap_times"], clean["drivers"])
    assert [r["lap"] for r in flagged.filter("is_slow").collect()] == [10]
    best = sp.lap_summary(clean["lap_times"], clean["drivers"]).first()
    assert best["best_sec"] == 90.0


def test_duplicate_lap_keys_keep_first_row(spark):
    laps = [(1, 1, 1, 1, 90000), (1, 1, 1, 1, 80000), (1, 1, 2, 1, 91000)]
    clean, _ = build(spark, laps=laps)
    rows = {r["lap"]: r["milliseconds"] for r in clean["lap_times"].collect()}
    assert rows == {1: 90000, 2: 91000}


def test_pit_durations_use_milliseconds_and_exclude_outliers(spark):
    pits = [(1, 1, 1, 10, 24000), (1, 1, 2, 30, 26000), (1, 1, 3, 40, 600000)]   # last = red flag
    clean, _ = build(spark, pits=pits)
    row = sp.pit_summary(clean["pit_stops"], clean["drivers"]).first()
    assert row["stops"] == 3 and row["avg_duration"] == 25.0
    assert row["fastest_stop"] == 24.0 and row["slowest_stop"] == 26.0


# ------------------------------------------------------------------ real dataset vs pandas
@pytest.mark.skipif(dl.missing_files(), reason="dataset not found")
def test_real_dataset_matches_pandas(spark):
    d = dl.DATA_DIR
    rd = lambda n: pd.read_csv(d / f"{n}.csv", na_values=["\\N"], low_memory=False)  # noqa: E731
    res, drv, con = rd("results"), rd("drivers"), rd("constructors")
    laps, pits = rd("lap_times"), rd("pit_stops")

    ctx = sp.prepare(spark)
    full = ctx["full"]

    classified = res["position"].notna() & (res["statusId"] != 2)
    win_rows = res[classified & (res["positionOrder"] == 1)].merge(drv, on="driverId")
    exp_wins = (win_rows["forename"] + " " + win_rows["surname"]).value_counts().head(5).to_dict()
    got = {r["driverName"]: r["wins"] for r in sp.driver_wins(full).limit(5).collect()}
    assert got == exp_wins

    exp_points = res.merge(con, on="constructorId").groupby("name")["points"].sum().to_dict()
    got_points = {r["constructorName"]: r["points"] for r in sp.constructor_stats(full).collect()}
    for name in ("Ferrari", "McLaren", "Mercedes"):
        assert got_points[name] == pytest.approx(exp_points[name])

    assert sp.overview_kpis(ctx["clean"], full)["races"] == res["raceId"].nunique()
    assert ctx["clean"]["lap_times"].count() == len(laps.drop_duplicates(["raceId", "driverId", "lap"]))
    assert ctx["clean"]["pit_stops"].count() == int(pits["milliseconds"].notna().sum())
