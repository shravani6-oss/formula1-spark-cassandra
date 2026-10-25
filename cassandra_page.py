"""Streamlit page 7: "Cassandra Database".

Everything shown here comes from a live connection test or from real queries. Nothing is reported as
connected / ingested / analysed unless that actually happened. Slow calls are cached (ttl) and the
"Refresh" button clears all of those caches.
"""
import subprocess
import sys
from dataclasses import asdict

import pandas as pd
import streamlit as st

import cassandra_db as cdb
import cassandra_spark as cs
import spark_processor as sp
import visualizations as viz
from cassandra_schema import TABLES, TABLES_BY_NAME
from config import BASE_DIR, get_cassandra_settings


# ------------------------------------------------------------------ cached access (ttl = auto-refresh)
@st.cache_data(ttl=30, show_spinner=False)
def _status():
    return asdict(cdb.check_connection())


@st.cache_data(ttl=120, show_spinner="Counting rows in Cassandra...")
def _table_overview():
    return cdb.table_overview()          # exceptions are not cached by Streamlit


@st.cache_data(ttl=60, show_spinner=False)
def _preview(table, limit):
    s = get_cassandra_settings()
    with cdb.open_session(s) as session:
        return cdb.preview_table(session, s.keyspace, table, limit)


@st.cache_data(ttl=60, show_spinner=False)
def _ingestion_log():
    s = get_cassandra_settings()
    with cdb.open_session(s) as session:
        return cdb.read_ingestion_log(session, s.keyspace)


@st.cache_data(ttl=600, show_spinner="Spark is reading the Cassandra tables and aggregating...")
def _spark_analytics(_spark):
    # `_spark` starts with an underscore so Streamlit does not try to hash the SparkSession.
    return cs.run_analytics(_spark)


@st.cache_data(ttl=600, show_spinner="Spark is counting rows...")
def _spark_count(_spark, table):
    return cs.count_table(_spark, table)


def _refresh_all():
    for fn in (_status, _table_overview, _preview, _ingestion_log, _spark_analytics, _spark_count):
        fn.clear()
    st.session_state.pop("cass_run_spark", None)


# ------------------------------------------------------------------ small helpers
def _table(df, key, filename):
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.download_button("⬇ Download CSV", df.to_csv(index=False).encode("utf-8"), f"{filename}.csv", "text/csv", key=key)


START_HELP = """\
# 1. start Cassandra (needs Docker Desktop on Windows)
docker compose up -d
# 2. create keyspace + tables
python create_schema.py
# 3. load the CSV files
python ingest_to_cassandra.py"""


# ------------------------------------------------------------------ sections
def _connection_block(settings):
    st_ = _status()
    c = st.columns(4)
    c[0].metric("Connection", "Connected ✅" if st_["connected"] else "Not connected ❌")
    c[1].metric("Cassandra version", st_["release_version"] or "n/a")
    c[2].metric("Keyspace", settings.keyspace, "exists" if st_["keyspace_exists"] else "not found",
                delta_color="off")
    c[3].metric("Host", st_["host"])
    if st_["connected"] and st_["keyspace_exists"]:
        st.success(f'Live connection test succeeded (cluster "{st_["cluster_name"]}"). '
                   f'{len(st_["tables"])} tables found in keyspace `{settings.keyspace}`.')
    elif st_["connected"]:
        st.warning(st_["message"])
    else:
        st.error(st_["message"])
    return st_


def _spark_connector_block():
    cst = sp.CONNECTOR_STATUS
    if not cst["requested"]:
        st.info("Spark Cassandra Connector is disabled (SPARK_CASSANDRA_ENABLED=false).")
    elif cst["loaded"]:
        st.caption(f'Spark was started with the connector `{cst["package"]}` (it must still reach Cassandra to read data).')
    else:
        st.warning(cst["error"] or "The Spark Cassandra Connector is not loaded.")


def _tables_tab(spark):
    try:
        ov = _table_overview()
    except cdb.CassandraError as exc:
        st.error(str(exc))
        return
    st.dataframe(ov, use_container_width=True, hide_index=True)
    st.caption("`rows` = live COUNT(*). The lap-times table is too large for a CQL COUNT(*), so use the ingestion log "
               "or the Spark count below.")
    with st.expander("Why these partition keys and clustering columns?"):
        for t in TABLES:
            st.markdown(f"**{t.name}** — partition key `({', '.join(t.partition_key)})`"
                        + (f", clustering `{', '.join(t.clustering_key)}`" if t.clustering_key else "")
                        + f"  \n{t.purpose} Query served: *{t.query}*")
    if st.button("Count lap_times_by_race_driver with Spark"):
        try:
            n = _spark_count(spark, "lap_times_by_race_driver")
            st.success(f"Spark read {n:,} rows from `lap_times_by_race_driver` in Cassandra.")
        except cs.CassandraSparkError as exc:
            st.error(str(exc))


def _preview_tab():
    name = st.selectbox("Table", list(TABLES_BY_NAME), key="cass_prev_table")
    n = st.slider("Rows", 5, 200, 20, key="cass_prev_n")
    try:
        df = _preview(name, n)
    except cdb.CassandraError as exc:
        st.error(str(exc))
        return
    if df.empty:
        st.info(f"`{name}` exists but has no rows yet - run the ingestion first.")
    else:
        st.caption(f"First {len(df)} rows returned by `SELECT * FROM {name} LIMIT {n}` (Cassandra order, not sorted).")
        st.dataframe(df, use_container_width=True, hide_index=True)


def _ingestion_tab(settings, status):
    st.markdown("**Ingestion log** (written by the ingestion script after each table load)")
    log_df = pd.DataFrame()
    if status["connected"] and status["keyspace_exists"]:
        try:
            log_df = _ingestion_log()
        except cdb.CassandraError as exc:
            st.error(str(exc))
    if log_df.empty:
        st.info("No ingestion has been recorded yet.")
    else:
        st.dataframe(log_df.sort_values("table_name"), use_container_width=True, hide_index=True)
    st.markdown("**Run from a terminal (recommended for the large lap-times table):**")
    st.code(START_HELP, "powershell")
    with st.expander("Or run the ingestion from this page"):
        names = list(TABLES_BY_NAME)
        default = [n for n in names if not TABLES_BY_NAME[n].large]
        picked = st.multiselect("Tables to load", names, default=default, key="cass_ing_tables")
        create = st.checkbox("Create keyspace and tables first (idempotent)", value=not status["keyspace_exists"])
        st.caption("Loading is an upsert on the primary key, so repeating it does not create duplicates. "
                   "`lap_times_by_race_driver` has ~885k rows and takes several minutes.")
        if st.button("Run ingestion now", disabled=not (status["connected"] and picked)):
            cmd = [sys.executable, "ingest_to_cassandra.py", "--tables", *picked]
            if create:
                cmd.append("--create-schema")
            with st.spinner("Ingesting... this blocks the page until it finishes."):
                try:
                    proc = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True, timeout=3600)
                except subprocess.TimeoutExpired:
                    st.error("Ingestion timed out after 60 minutes. Run it from a terminal instead.")
                    return
            out = (proc.stdout + proc.stderr).strip()
            (st.success if proc.returncode == 0 else st.error)(
                "Ingestion finished: all selected tables loaded." if proc.returncode == 0
                else f"Ingestion reported problems (exit code {proc.returncode}). See the output below.")
            st.code("\n".join(out.splitlines()[-40:]) or "(no output)")
            _refresh_all()
    if not status["connected"]:
        st.warning("Start Cassandra first: the ingestion needs a live connection.")


def _analytics_tab(spark, csv_ctx):
    st.markdown("These analyses are computed by **Spark** on DataFrames read **from Cassandra** through the "
                "Spark Cassandra Connector (tables `results_by_race`, `drivers`, `constructors`, `races`, "
                "`pit_stops_by_race`). The CSV files are not used for the results below.")
    if st.button("▶ Run Spark analytics on Cassandra data"):
        st.session_state["cass_run_spark"] = True
    if not st.session_state.get("cass_run_spark"):
        st.info("Press the button to start the Spark job (the result is cached for 10 minutes).")
        return
    try:
        res = _spark_analytics(spark)
    except cs.CassandraSparkError as exc:
        st.error(str(exc))
        return
    st.success("Spark read the Cassandra tables and finished the aggregations.")

    st.subheader("Control check: Cassandra vs CSV pipeline")
    st.dataframe(cs.compare_with_csv(res["totals"], csv_ctx["full"]), use_container_width=True, hide_index=True)
    st.caption("Both pipelines use the same cleaning and aggregation code; matching totals confirm that the ingestion "
               "preserved the data.")

    st.subheader("1 · Driver performance (aggregated from Cassandra race results)")
    dp = res["driver_performance"]
    c1, c2 = st.columns(2)
    metric = c1.radio("Rank drivers by", ["wins", "podiums", "points", "races"], horizontal=True, key="cass_dp_m")
    top = c2.slider("Number of drivers", 5, 30, 15, key="cass_dp_n")
    st.plotly_chart(viz.bar(dp.nlargest(top, metric), "driverName", metric, f"Top {top} drivers by {metric}"),
                    use_container_width=True)
    _table(dp.drop(columns=["driverId"]), "cass_dl_dp", "cassandra_driver_performance")

    st.subheader("2 · Constructor performance by season (from Cassandra)")
    leaders, pts = res["constructor_season_leaders"], res["constructor_season_points"]
    lead = leaders.copy()
    lead["year"] = lead["year"].astype(str)
    st.plotly_chart(viz.bar(lead, "year", "points", "Top-scoring constructor of each season", "constructorName", "stack"),
                    use_container_width=True)
    year = st.selectbox("Season", sorted(pts["year"].unique().tolist(), reverse=True), key="cass_cs_y")
    sel = pts[pts["year"] == year].nlargest(12, "points")
    st.plotly_chart(viz.bar(sel, "constructorName", "points", f"{year}: constructor points"), use_container_width=True)
    _table(res["season_summary"], "cass_dl_ss", "cassandra_season_summary")

    st.subheader("3 · Pit stops by season (from Cassandra)")
    pit = res["pit_by_season"]
    if pit.empty:
        st.info("No pit-stop rows in Cassandra yet.")
    else:
        long = pit.melt(id_vars="year", value_vars=["avg_duration", "median_duration"])
        st.plotly_chart(viz.line(long, "year", "value", "variable",
                                 f"Pit-stop duration by season (stops over {sp.PIT_MAX_SEC:.0f} s excluded)"),
                        use_container_width=True)
        _table(pit, "cass_dl_pit", "cassandra_pit_by_season")


# ------------------------------------------------------------------ entry point
def render(get_csv_ctx):
    """Draw the page. `get_csv_ctx` returns the cached Spark context (also used for the control check)."""
    st.title("Cassandra Database")
    settings = get_cassandra_settings()
    st.caption(f"Keyspace `{settings.keyspace}` on {settings.host_string}:{settings.port} · "
               "values are queried live and cached briefly.")
    st.markdown(viz.architecture_html(), unsafe_allow_html=True)
    if st.button("🔄 Refresh Cassandra data"):
        _refresh_all()
        st.rerun()

    status = _connection_block(settings)
    _spark_connector_block()
    if not status["connected"]:
        st.markdown("**How to start Cassandra and load the data**")
        st.code(START_HELP, "powershell")

    ready = status["connected"] and status["keyspace_exists"]
    spark_ctx = get_csv_ctx()
    spark = spark_ctx["spark"]
    t1, t2, t3, t4 = st.tabs(["Tables & record counts", "Record preview", "Ingestion", "Spark analytics on Cassandra"])
    with t1:
        if ready:
            _tables_tab(spark)
        else:
            st.info("Connect to Cassandra and create the schema first.")
    with t2:
        if ready:
            _preview_tab()
        else:
            st.info("Connect to Cassandra and create the schema first.")
    with t3:
        _ingestion_tab(settings, status)
    with t4:
        if ready:
            _analytics_tab(spark, spark_ctx)
        else:
            st.info("Connect to Cassandra and ingest the data first.")
