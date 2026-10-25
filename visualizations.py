"""Plotly figure builders with a charcoal / racing-red theme."""
import plotly.express as px
import plotly.graph_objects as go

RED = "#E10600"
PALETTE = [RED, "#F5F5F5", "#FF8A80", "#9E9E9E", "#FFB300", "#26C6DA", "#7E57C2", "#66BB6A"]


def _style(fig, title="", height=340):
    fig.update_layout(template="plotly_dark", title=dict(text=title, font=dict(size=15)), height=height,
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", colorway=PALETTE,
                      margin=dict(l=10, r=10, t=45, b=10), legend=dict(orientation="h", y=-0.25),
                      font=dict(color="#F5F5F5"))
    fig.update_xaxes(gridcolor="#2a2a32")
    fig.update_yaxes(gridcolor="#2a2a32")
    return fig


def bar(df, x, y, title="", color=None, barmode="group", height=340):
    return _style(px.bar(df, x=x, y=y, color=color, barmode=barmode, text_auto=True), title, height)


def line(df, x, y, color=None, title="", reverse_y=False, height=340):
    fig = px.line(df, x=x, y=y, color=color, markers=True)
    if reverse_y:
        fig.update_yaxes(autorange="reversed")
    return _style(fig, title, height)


def scatter(df, x, y, color=None, size=None, hover=None, title="", height=340):
    return _style(px.scatter(df, x=x, y=y, color=color, size=size, hover_name=hover, size_max=22), title, height)


def lap_pace(df, title="Lap-by-lap pace (seconds)"):
    fig = px.line(df, x="lap", y="seconds", color="driverName", markers=True)
    slow = df[df["is_slow"]]
    if not slow.empty:
        fig.add_trace(go.Scatter(x=slow["lap"], y=slow["seconds"], mode="markers", name="Unusually slow",
                                 hovertext=slow["driverName"],
                                 marker=dict(symbol="x", size=13, color="#FF1744", line=dict(width=2))))
    return _style(fig, title, 420)


def workflow_html():
    steps = ["CSV", "Spark DataFrames", "Cleaning", "Spark SQL / Transformations", "Aggregations", "Plotly Dashboard"]
    chips = '<span class="arrow">→</span>'.join(f'<span class="chip">{s}</span>' for s in steps)
    return f'<div class="flow">{chips}</div>'


def architecture_html():
    steps = ["Kaggle CSV dataset", "Validation + ingestion", "Apache Cassandra", "Apache Spark",
             "Streamlit dashboard", "Plotly charts"]
    chips = '<span class="arrow">→</span>'.join(f'<span class="chip">{s}</span>' for s in steps)
    return f'<div class="flow">{chips}</div>'
