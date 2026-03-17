import pandas as pd
import plotly.express as px
import os


def main():
    print("[INFO] Loading schedule data...")

    schedule = pd.read_csv("database/schedule_final.csv")

    # ✅ FIX: tránh crash nếu file không tồn tại
    downtime_path = "database/planned_downtime_schedule.csv"

    if os.path.exists(downtime_path):
        downtime = pd.read_csv(downtime_path)
    else:
        print("[WARNING] No downtime file found → using empty dataframe")
        downtime = pd.DataFrame(
            columns=["line_id", "process", "start_time", "end_time"]
        )

    # =============================
    # FORMAT DATA
    # =============================
    schedule["start_time"] = pd.to_datetime(schedule["start_time"])
    schedule["end_time"] = pd.to_datetime(schedule["end_time"])

    # =============================
    # GANTT CHART
    # =============================
    fig = px.timeline(
        schedule,
        x_start="start_time",
        x_end="end_time",
        y="line_id",
        color="job_id",
        title="Production Gantt Chart"
    )

    fig.update_yaxes(autorange="reversed")

    # =============================
    # ADD DOWNTIME (nếu có)
    # =============================
    if not downtime.empty:
        downtime["start_time"] = pd.to_datetime(downtime["start_time"])
        downtime["end_time"] = pd.to_datetime(downtime["end_time"])

        for _, row in downtime.iterrows():
            fig.add_shape(
                type="rect",
                x0=row["start_time"],
                x1=row["end_time"],
                y0=row["line_id"],
                y1=row["line_id"],
                fillcolor="red",
                opacity=0.3,
                layer="below",
                line_width=0,
            )

    # =============================
    # SAVE FILE
    # =============================
    fig.write_html("gantt_chart.html")

    print("[INFO] Gantt chart saved → gantt_chart.html")


if __name__ == "__main__":
    main()