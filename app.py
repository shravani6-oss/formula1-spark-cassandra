"""Streamlit dashboard. Run with: streamlit run app.py"""
import pandas as pd
import streamlit as st

import cassandra_page
import data_loader as dl
import spark_processor as sp
import visualizations as viz

st.set_page_config(page_title="F1 Spark + Cassandra Analytics", page_icon="🏁", layout="wide")
st.markdown("""<style>
.stApp{background:#121214;color:#F5F5F5}
[data-testid=stSidebar]{background:#18181c;border-right:2px solid #E10600}
h1,h2,h3{color:#fff;letter-spacing:.5px}
h1{border-bottom:3px solid #E10600;padding-bottom:6px}
[data-testid=stMetric]{background:#1d1d23;border-left:4px solid #E10600;border-radius:8px;padding:8px 14px}
[data-testid=stMetricValue]{font-size:1.45rem}
.stButton>button,.stDownloadButton>button{background:#E10600;color:#fff;border:0;border-radius:6px}
.flow{display:flex;flex-wrap:wrap;align-items:center;gap:6px;margin:8px 0}
.chip{background:#1d1d23;border:1px solid #E10600;border-radius:16px;padding:4px 12px;font-size:.85rem}
.arrow{color:#E10600;font-weight:700}
.banner{background:#2a1214;border:1px solid #E10600;border-radius:8px;padding:6px 12px;font-size:.85rem;margin-bottom:8px}
</style>""", unsafe_allow_html=True)


# ------------------------------------------------------------ cached Spark access
@st.cache_resource(show_spinner="Starting Spark and loading data…")
def get_ctx():
    return sp.prepare(sp.get_spark())


@st.cache_data(show_spinner=False)
def lookups():
    c = get_ctx()["clean"]
    return (c["drivers"].select("driverId", "driverName").orderBy("driverName").toPandas(),
            c["constructors"].select("constructorId", "name").withColumnRenamed("name", "constructorName")
            .orderBy("constructorName").toPandas(),
            c["races"].select("raceId", "year", "round", "name").orderBy("year", "round").toPandas())


@st.cache_data(show_spinner=False)
def kpis():
    c = get_ctx()
    return sp.overview_kpis(c["clean"], c["full"])


@st.cache_data(show_spinner=False)
def spark_info():
    return sp.spark_overview(get_ctx())


@st.cache_data(show_spinner="Running Spark job…")
def compute(name, years=(), drivers=(), cons=(), races=()):
    c = get_ctx()
    cl = c["clean"]
    f = sp.filter_results(c["full"], years, drivers, cons, races)
    jobs = {
        "results": lambda: f.select(*sp.RESULT_COLS).orderBy("year", "round", "positionOrder"),
        "driver_stats": lambda: sp.driver_stats(f),
        "constructor_stats": lambda: sp.constructor_stats(f),
        "season_driver": lambda: sp.season_points(f, "driver"),
        "season_constructor": lambda: sp.season_points(f, "constructor"),
        "driver_by_constructor": lambda: sp.driver_contributions(f),
        "winners": lambda: sp.race_winners(f),
        "lap_races": lambda: cl["lap_times"].select("raceId").distinct(),
        "lap_summary": lambda: sp.lap_summary(cl["lap_times"], cl["drivers"], races, drivers),
        "laps": lambda: sp.flag_slow_laps(cl["lap_times"], cl["drivers"], races, drivers),
        "pit_summary": lambda: sp.pit_summary(cl["pit_stops"], cl["drivers"], races, drivers),
        "pit_vs_pos": lambda: sp.pit_vs_position(cl["pit_stops"], c["full"], races, drivers),
        "pit_by_season": lambda: sp.pit_by_season(cl["pit_stops"], cl["races"]),
        "quali_vs_race": lambda: sp.qualifying_vs_race(cl, f),
        "champ_driver": lambda: sp.season_champions(cl, "driver"),
        "champ_constructor": lambda: sp.season_champions(cl, "constructor"),
    }
    df = jobs[name]()
    return None if df is None else df.toPandas()


@st.cache_data(show_spinner=False)
def validation_report():
    return dl.validate_dataset()


# ------------------------------------------------------------ helpers
def need(df, msg="No data for the selected filters."):
    if df is None or df.empty:
        st.info(msg)
        return False
    return True


def download(df, name, key):
    st.download_button("⬇ Download CSV", df.to_csv(index=False).encode("utf-8"), f"{name}.csv", "text/csv", key=key)


def table(df, name, key):
    st.dataframe(df, use_container_width=True, hide_index=True)
    download(df, name, key)


def _pick(col, label, mapping, key):
    sel = col.multiselect(label, list(mapping), key=key, placeholder="All")
    return tuple(int(mapping[s]) for s in sel if s in mapping)


def filters(key, drivers=True, cons=False, races=True):
    d, k, r = lookups()
    out = {"years": (), "drivers": (), "cons": (), "races": ()}
    cols = st.columns(4)
    ys = cols[0].multiselect("Season", sorted(r["year"].unique().tolist()), key=key + "y", placeholder="All")
    out["years"] = tuple(int(y) for y in ys)
    if drivers:
        out["drivers"] = _pick(cols[1], "Driver", dict(zip(d["driverName"], d["driverId"])), key + "d")
    if cons:
        out["cons"] = _pick(cols[2], "Constructor", dict(zip(k["constructorName"], k["constructorId"])), key + "c")
    if races:
        rr = r[r["year"].isin(ys)] if ys else r
        m = {f"{y} R{rd} {n}": i for y, rd, n, i in zip(rr["year"], rr["round"], rr["name"], rr["raceId"])}
        out["races"] = _pick(cols[3], "Race", m, key + "r")
    return out


def stack_top(df, key, value="points", n=10):
    """Keep the n biggest `key` values (by total `value`), merge the rest into 'Other' (readable stacked charts)."""
    keep = df.groupby(key)[value].sum().nlargest(n).index
    out = df.copy()
    out[key] = out[key].where(out[key].isin(keep), "Other")
    return out.groupby(["year", key], as_index=False)[value].sum()


def f3(x, unit="s"):
    return f"{x:.3f}{unit}" if pd.notna(x) else "n/a"


# ------------------------------------------------------------ pages
def page_overview():
    st.title("Overview")
    k = kpis()
    c = st.columns(5)
    c[0].metric("Races with results", f'{k["races"]:,}')
    c[1].metric("Drivers", f'{k["drivers"]:,}')
    c[2].metric("Constructors", f'{k["constructors"]:,}')
    c[3].metric("Circuits", f'{k["circuits"]:,}' if k["circuits"] is not None else "n/a")
    c[4].metric("Seasons", f'{k["seasons"]} ({k["first_year"]}–{k["last_year"]})')
    if k["calendar_races"] > k["races"]:
        st.caption(f'{k["calendar_races"] - k["races"]} race(s) in races.csv have no results yet '
                   "(scheduled/future) and are not counted.")
    c = st.columns(3)
    fl = k["fastest_lap"]
    c[0].metric("Fastest recorded lap", f3(fl["seconds"]) if fl else "n/a",
                f'{fl["driver"]} · {fl["race"]}' if fl else None, delta_color="off")
    td, tc = k["top_driver"], k["top_constructor"]
    c[1].metric("Most successful driver", td["name"] if td else "n/a", f'{td["wins"]} wins' if td else None, delta_color="off")
    c[2].metric("Most successful constructor", tc["name"] if tc else "n/a", f'{tc["wins"]} wins' if tc else None, delta_color="off")
    ds, cs = compute("driver_stats"), compute("constructor_stats")
    if not need(ds, "The results dataset is empty."):
        return
    a, b = st.columns(2)
    a.plotly_chart(viz.bar(ds.head(15), "driverName", "wins", "Driver wins (top 15)"), use_container_width=True)
    b.plotly_chart(viz.bar(cs.nlargest(15, "wins"), "constructorName", "wins", "Constructor wins (top 15)"), use_container_width=True)
    t1, t2, t3, t4 = st.tabs(["Driver points per season", "Constructor points per season",
                              "Season champions", "Race results by season"])
    with t1:
        sd = stack_top(compute("season_driver"), "driverName", "points", 10)
        sd["year"] = sd["year"].astype(str)
        st.plotly_chart(viz.bar(sd, "year", "points", "Championship points by season (top 10 drivers overall, rest = Other)",
                                "driverName", "stack"), use_container_width=True)
    with t2:
        sc = stack_top(compute("season_constructor"), "constructorName", "points", 10)
        sc["year"] = sc["year"].astype(str)
        st.plotly_chart(viz.bar(sc, "year", "points", "Constructor points by season (top 10 overall, rest = Other)",
                                "constructorName", "stack"), use_container_width=True)
    with t3:
        cd, cc = compute("champ_driver"), compute("champ_constructor")
        if cd is None or cd.empty:
            st.info("driver_standings.csv is not available, so season champions cannot be shown.")
        else:
            x, y = st.columns(2)
            x.caption("Drivers' champion = position 1 in the standings after the last race of the season")
            x.dataframe(cd.sort_values("year", ascending=False), use_container_width=True, hide_index=True)
            y.caption("Constructors' champion (standings exist from 1958)")
            if cc is None or cc.empty:
                y.info("constructor_standings.csv is not available.")
            else:
                y.dataframe(cc.sort_values("year", ascending=False), use_container_width=True, hide_index=True)
            st.caption("Computed from the standings files as supplied; the final standings of the current season "
                       "are provisional while it is still running.")
    with t4:
        table(compute("winners"), "race_winners", "dl_ov")
    with st.expander("About this dataset and its validation"):
        st.write(dl.DATASET_NOTE)
        st.caption("Each CSV is checked against the verified schema: header names/order, unreadable numbers, "
                   "null cells (`\\N`) and duplicate keys. Duplicate lap keys are resolved by keeping the first row.")
        st.dataframe(validation_report(), use_container_width=True, hide_index=True)


def page_driver():
    st.title("Driver Performance")
    flt = filters("drv")
    res = compute("results", **flt)
    if not need(res):
        return
    stats = compute("driver_stats", **flt)
    rid = tuple(sorted(int(x) for x in res["raceId"].unique()))
    laps = compute("lap_summary", races=rid, drivers=flt["drivers"])
    cl = res[res["classified"]]
    c = st.columns(6)
    c[0].metric("Wins", int(stats["wins"].sum()))
    c[1].metric("Podiums", int(stats["podiums"].sum()))
    c[2].metric("Points", f'{stats["points"].sum():.0f}')
    c[3].metric("Avg finish", f'{cl["positionOrder"].mean():.2f}' if len(cl) else "n/a")
    c[4].metric("Fastest lap", f3(stats["best_lap_sec"].min()))
    c[5].metric("Avg lap time", f3(laps["avg_sec"].mean()) if not laps.empty else "n/a")
    n_sel = len(flt["drivers"])
    if cl.empty:
        st.info("No classified finishes for this selection.")
    elif not 1 <= n_sel <= 8:
        st.info("Select between 1 and 8 drivers (and optionally seasons) to see the race-by-race finishing positions.")
    else:
        st.plotly_chart(viz.line(cl, "race_label", "positionOrder", "driverName",
                                 "Race-by-race finishing position (lower is better)", reverse_y=True), use_container_width=True)
    metric = st.radio("Compare drivers by", ["points", "wins", "podiums", "avg_finish"], horizontal=True)
    if metric == "avg_finish":   # lower is better; ignore drivers with very few races
        shown = stats[stats["races"] >= 10].nsmallest(20, "avg_finish")
        title = "Driver comparison: average finish (best 20, at least 10 races)"
    else:
        shown, title = stats.nlargest(20, metric), f"Driver comparison: {metric} (top 20)"
    st.plotly_chart(viz.bar(shown, "driverName", metric, title), use_container_width=True)
    qv = compute("quali_vs_race", **flt)
    st.subheader("Qualifying vs race performance")
    if qv is None:
        st.info("qualifying.csv is not available.")
    elif qv.empty:
        st.info("No qualifying data for this selection (qualifying.csv starts in 1994).")
    else:
        a2, b2 = st.columns(2)
        a2.metric("Average places gained (quali → finish)", f'{qv["places_gained"].mean():+.2f}')
        b2.metric("Races compared", f"{len(qv):,}")
        st.plotly_chart(viz.scatter(qv, "quali_position", "positionOrder", None, None, "driverName",
                                    "Qualifying position vs finishing position (classified finishes)"), use_container_width=True)
    st.subheader("Driver statistics")
    table(stats.drop(columns=["driverId"]), "driver_stats", "dl_ds")
    st.subheader("Race results")
    table(res[["race_label", "driverName", "constructorName", "grid", "position", "positionOrder",
               "points", "laps", "fastestLapTime"]], "filtered_race_results", "dl_dr")


def page_constructor():
    st.title("Constructor Analysis")
    flt = filters("con", drivers=False, cons=True, races=False)
    res = compute("results", **flt)
    if not need(res):
        return
    cs = compute("constructor_stats", **flt)
    cl = res[res["classified"]]
    c = st.columns(4)
    c[0].metric("Points", f'{cs["points"].sum():.0f}')
    c[1].metric("Wins", int(cs["wins"].sum()))
    c[2].metric("Podiums", int(cs["podiums"].sum()))
    c[3].metric("Avg finish", f'{cl["positionOrder"].mean():.2f}' if len(cl) else "n/a")
    a, b = st.columns(2)
    top = cs.nlargest(15, "points")
    a.plotly_chart(viz.bar(top, "constructorName", "points", "Constructor points (top 15)"), use_container_width=True)
    wp = top.melt(id_vars="constructorName", value_vars=["wins", "podiums"])
    b.plotly_chart(viz.bar(wp, "constructorName", "value", "Wins and podiums (top 15 by points)", "variable"), use_container_width=True)
    contrib = compute("driver_by_constructor", **flt)
    cc = contrib[contrib["constructorName"].isin(top["constructorName"])]
    best = cc.groupby("driverName")["points"].sum().nlargest(12).index
    cc = cc.assign(driverName=cc["driverName"].where(cc["driverName"].isin(best), "Other"))
    cc = cc.groupby(["constructorName", "driverName"], as_index=False)["points"].sum()
    st.plotly_chart(viz.bar(cc, "constructorName", "points", "Driver contributions to constructor points (top 15 constructors)",
                            "driverName", "stack"), use_container_width=True)
    season = stack_top(compute("season_constructor", **flt), "constructorName", "points", 8)
    season["year"] = season["year"].astype(str)
    st.plotly_chart(viz.bar(season, "year", "points", "Season-wise comparison (top 8 constructors in selection)",
                            "constructorName", "stack"), use_container_width=True)
    table(cs.drop(columns=["constructorId"]), "constructor_stats", "dl_cs")
    st.subheader("Driver contributions")
    table(contrib, "driver_contributions", "dl_cc")


def page_laps():
    st.title("Lap Time Analysis")
    lr = compute("lap_races")
    if lr.empty:
        st.warning("No lap-time data available (lap_times.csv is empty or invalid).")
        return
    _, _, r = lookups()
    r = r[r["raceId"].isin(lr["raceId"])]
    m = {f"{y} R{rd} {n}": int(i) for y, rd, n, i in zip(r["year"], r["round"], r["name"], r["raceId"])}
    if not m:
        st.warning("Lap data does not match any race in races.csv.")
        return
    c1, c2 = st.columns(2)
    rid = m[c1.selectbox("Race (only races with lap data)", list(m))]
    base = compute("laps", races=(rid,))
    names = sorted(base["driverName"].unique())
    sel = c2.multiselect("Drivers", names, default=names)
    laps = base[base["driverName"].isin(sel)]
    if not need(laps, "Select at least one driver."):
        return
    summ = compute("lap_summary", races=(rid,))
    summ = summ[summ["driverName"].isin(sel)].drop(columns=["raceId", "driverId"])
    fast = laps.loc[laps["seconds"].idxmin()]
    c = st.columns(3)
    c[0].metric("Fastest lap", f3(fast["seconds"]), fast["driverName"], delta_color="off")
    c[1].metric("Average lap", f3(laps["seconds"].mean()))
    c[2].metric("Unusually slow laps", int(laps["is_slow"].sum()))
    st.plotly_chart(viz.lap_pace(laps), use_container_width=True)
    st.caption("Slow lap = lap time above the driver's mean + 2 standard deviations in this race "
               "(pit in/out laps can be flagged too).")
    a, b = st.columns(2)
    a.plotly_chart(viz.bar(summ, "driverName", "avg_sec", "Average lap time (s)"), use_container_width=True)
    b.plotly_chart(viz.bar(summ, "driverName", "best_sec", "Fastest lap (s)"), use_container_width=True)
    table(summ, "lap_summary", "dl_ls")
    slow = laps[laps["is_slow"]][["driverName", "lap", "seconds", "mean_sec", "threshold_sec"]]
    st.subheader("Unusually slow laps")
    if slow.empty:
        st.success("No unusually slow laps detected.")
    else:
        table(slow, "slow_laps", "dl_sl")


def page_pits():
    st.title("Pit Stop Analysis")
    flt = filters("pit", cons=False)
    ps = compute("pit_summary", races=flt["races"], drivers=flt["drivers"])
    if not need(ps, "No pit-stop data for this selection (pit_stops.csv may be empty)."):
        return
    c = st.columns(3)
    c[0].metric("Total stops", int(ps["stops"].sum()))
    c[1].metric("Average duration", f3(ps["avg_duration"].mean()))
    c[2].metric("Fastest stop", f3(ps["fastest_stop"].min()))
    a, b = st.columns(2)
    a.plotly_chart(viz.bar(ps.nlargest(20, "stops"), "driverName", "stops", "Pit stops per driver (top 20)"), use_container_width=True)
    dur = ps.nlargest(20, "stops").melt(id_vars="driverName", value_vars=["avg_duration", "fastest_stop", "slowest_stop"])
    b.plotly_chart(viz.bar(dur, "driverName", "value", "Pit stop duration comparison (s, 20 drivers with most stops)", "variable"),
                   use_container_width=True)
    st.caption(f"Stops are counted in full; durations ignore stops longer than {sp.PIT_MAX_SEC:.0f} s "
               "(red flags, repairs). Duration is computed from the `milliseconds` column.")
    season = compute("pit_by_season")
    if not season.empty:
        sl = season.melt(id_vars="year", value_vars=["avg_duration", "median_duration"])
        st.plotly_chart(viz.line(sl, "year", "value", "variable", "Average and median pit-stop duration by season (s)"),
                        use_container_width=True)
    pv = compute("pit_vs_pos", races=flt["races"], drivers=flt["drivers"])
    st.subheader("Pit stops vs finishing position")
    if len(pv) < 3:
        st.info("Not enough classified pit-stop data to show a relationship.")
    else:
        few = pv["driverName"].nunique() <= 10
        st.plotly_chart(viz.scatter(pv, "avg_duration", "positionOrder", "driverName" if few else None, "stops",
                                    "race_label", "Average pit duration (s) vs finishing position"), use_container_width=True)
        st.caption("Marker size = number of stops. Races without pit-stop data are excluded, not estimated.")
    table(ps.drop(columns=["driverId"]), "pit_summary", "dl_ps")


def page_spark():
    st.title("Spark Analytics")
    m, info = ctx["meta"], spark_info()
    c = st.columns(4)
    c[0].metric("Spark status", "Active ✅")
    c[1].metric("Spark version", m["spark_version"])
    c[2].metric("Load + process time", f'{m["seconds"]} s')
    c[3].metric("Master", f'{m["master"]} ({m["cores"]} cores)')
    st.markdown(viz.workflow_html(), unsafe_allow_html=True)
    t1, t2, t3 = st.tabs(["Datasets & schemas", "DataFrame operations", "Spark SQL"])
    with t1:
        st.dataframe(info["counts"], use_container_width=True, hide_index=True)
        name = st.selectbox("Show schema for", list(info["schemas"]))
        st.dataframe(info["schemas"][name], use_container_width=True, hide_index=True)
        st.caption("Preview of the joined Spark DataFrame (results ⋈ drivers ⋈ constructors ⋈ races)")
        st.dataframe(info["preview"], use_container_width=True, hide_index=True)
    with t2:
        years = sorted(lookups()[2]["year"].unique().tolist())
        ys = st.multiselect("Season filter", years, key="spk_y", placeholder="All")
        flt = {"years": tuple(int(y) for y in ys)}
        a, b, c3 = st.columns(3)
        ds, cs = compute("driver_stats", **flt), compute("constructor_stats", **flt)
        if ds.empty:
            st.info("No results for this season filter.")
        else:
            a.caption("Driver win counts")
            a.dataframe(ds[["driverName", "wins", "podiums"]], hide_index=True, use_container_width=True)
            b.caption("Constructor points")
            b.dataframe(cs[["constructorName", "points"]], hide_index=True, use_container_width=True)
            c3.caption("Average finishing position")
            c3.dataframe(ds[["driverName", "avg_finish"]].sort_values("avg_finish"), hide_index=True, use_container_width=True)
            st.subheader("Season-filtered race results")
            table(compute("results", **flt)[["race_label", "driverName", "constructorName", "grid", "position",
                                              "positionOrder", "points", "laps"]], "spark_filtered_results", "dl_sp")
        for n, (code, df) in info["ops"].items():
            with st.expander(n):
                st.code(code, "python")
                st.dataframe(df, use_container_width=True, hide_index=True)
    with t3:
        for i, (n, (q, df)) in enumerate(info["sql"].items()):
            st.markdown(f"**{n}**")
            st.code(q, "sql")
            table(df, f"sql_{i}", f"dl_sql{i}")


# ------------------------------------------------------------ startup & navigation
missing = dl.missing_files()
if missing:
    st.error(f"Missing data files in {dl.DATA_DIR}: {', '.join(missing)}")
    st.stop()
try:
    ctx = get_ctx()
except Exception as exc:  # noqa: BLE001
    st.error(f"Spark failed to start or load data: {exc}")
    st.stop()

PAGES = {"🏠 Overview": page_overview, "🧑‍✈️ Driver Performance": page_driver,
         "🏎️ Constructor Analysis": page_constructor, "⏱️ Lap Time Analysis": page_laps,
         "🛞 Pit Stop Analysis": page_pits, "⚡ Spark Analytics": page_spark,
         "🗄️ Cassandra Database": lambda: cassandra_page.render(get_ctx)}

with st.sidebar:
    st.markdown("## 🏁 F1 Spark + Cassandra")
    page = st.radio("Navigate", list(PAGES), label_visibility="collapsed")
    if st.button("🔄 Reload data"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()
    st.caption(f'Spark {ctx["meta"]["spark_version"]} · {ctx["meta"]["joined_rows"]} joined result rows')

_k = kpis()
st.markdown(f'<div class="banner">📊 Analysis based on the supplied historical Formula 1 CSV dataset '
            f'({_k["first_year"]}–{_k["last_year"]}). Its original source and licence are not stated in the files.</div>',
            unsafe_allow_html=True)
if ctx["meta"]["joined_rows"] == 0:
    st.warning("results.csv has no valid rows after cleaning, so most analytics will be empty.")
PAGES[page]()
