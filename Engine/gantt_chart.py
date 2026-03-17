import pandas as pd
import plotly.graph_objects as go

# =========================
# LOAD DATA
# =========================
schedule = pd.read_csv("database/schedule_final.csv")
downtime = pd.read_csv("database/planned_downtime_schedule.csv")

schedule["start_time"] = pd.to_datetime(schedule["start_time"])
schedule["end_time"] = pd.to_datetime(schedule["end_time"])

downtime["start_time"] = pd.to_datetime(downtime["start_time"])
downtime["end_time"] = pd.to_datetime(downtime["end_time"])

# normalize
schedule["process"] = schedule["process_name"].str.lower()
schedule["type"] = "JOB"
schedule["label"] = schedule["job_id"]

downtime["process"] = downtime["process"].str.lower()
downtime["type"] = "DOWNTIME"
downtime["label"] = "PD"

df = pd.concat([
    schedule[["line_id","process","start_time","end_time","type","label"]],
    downtime[["line_id","process","start_time","end_time","type","label"]]
])

# =========================
# UNIQUE VALUES
# =========================
dates = sorted(df["start_time"].dt.date.unique())
processes = sorted(df["process"].unique())
types = ["ALL", "JOB", "DOWNTIME"]

# =========================
# BUILD TRACES
# =========================
fig = go.Figure()

trace_meta = []

for d in dates:
    day_start = pd.Timestamp(d)
    day_end = day_start + pd.Timedelta(days=1)

    for p in processes:
        for t in types:

            temp = df[
                (df["end_time"] > day_start) &
                (df["start_time"] < day_end)
            ].copy()

            temp["plot_start"] = temp["start_time"].clip(lower=day_start, upper=day_end)
            temp["plot_end"] = temp["end_time"].clip(lower=day_start, upper=day_end)

            temp["start_hour"] = (temp["plot_start"] - day_start).dt.total_seconds()/3600
            temp["end_hour"] = (temp["plot_end"] - day_start).dt.total_seconds()/3600

            temp = temp[temp["end_hour"] > temp["start_hour"]]

            temp = temp[temp["process"] == p]

            if t != "ALL":
                temp = temp[temp["type"] == t]

            if temp.empty:
                continue

            fig.add_trace(
                go.Bar(
                    x=temp["end_hour"] - temp["start_hour"],
                    y=temp["line_id"],
                    base=temp["start_hour"],
                    orientation="h",
                    text=temp["label"],
                    name=f"{d}-{p}-{t}",
                    marker=dict(color="blue" if t=="JOB" else "red"),
                    visible=False
                )
            )

            trace_meta.append((d,p,t))

# show first trace
if fig.data:
    fig.data[0].visible = True

# =========================
# DROPDOWN
# =========================
def get_visibility(sel_d=None, sel_p=None, sel_t=None):
    vis = []
    for (d,p,t) in trace_meta:
        if sel_d and d != sel_d:
            vis.append(False)
        elif sel_p and p != sel_p:
            vis.append(False)
        elif sel_t and t != sel_t:
            vis.append(False)
        else:
            vis.append(True)
    return vis

# Day dropdown
buttons_day = [
    dict(label=str(d),
         method="update",
         args=[{"visible": get_visibility(sel_d=d)}])
    for d in dates
]

# Process dropdown
buttons_proc = [
    dict(label=p,
         method="update",
         args=[{"visible": get_visibility(sel_p=p)}])
    for p in processes
]

# Type dropdown
buttons_type = [
    dict(label=t,
         method="update",
         args=[{"visible": get_visibility(sel_t=t)}])
    for t in types
]

fig.update_layout(
    updatemenus=[
        dict(buttons=buttons_day, x=0, y=1.2),
        dict(buttons=buttons_proc, x=0.3, y=1.2),
        dict(buttons=buttons_type, x=0.6, y=1.2),
    ]
)

fig.update_layout(
    title="Gantt Chart Interactive",
    xaxis_title="Hour of Day",
    yaxis_title="Line",
    barmode="overlay",
    height=700
)

fig.update_xaxes(
    range=[0,24],
    tickvals=list(range(0,25,2)),
    ticktext=[f"{h:02d}:00" for h in range(0,25,2)]
)

fig.update_yaxes(autorange="reversed")

fig.write_html("gantt_chart.html")
print("DONE → gantt_chart.html")