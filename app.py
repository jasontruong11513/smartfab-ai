from fastapi import FastAPI
import subprocess
import sys
import os
import pandas as pd

from Engine import scheduler

app = FastAPI()


@app.get("/")
def root():
    return {"message": "SmartFab API is running"}


@app.get("/run")
def run_all():
    try:
        print("[INFO] Running scheduler...")
        scheduler.main()

        # ✅ FIX 1: đảm bảo file downtime tồn tại
        downtime_path = "database/planned_downtime_schedule.csv"

        if not os.path.exists(downtime_path):
            print("[WARNING] Downtime file missing → creating empty file")
            pd.DataFrame(
                columns=["line_id", "process", "start_time", "end_time"]
            ).to_csv(downtime_path, index=False)

        # ✅ FIX 2: chạy gantt chart + log
        print("[INFO] Running Gantt chart...")

        result = subprocess.run(
            [sys.executable, "Engine/gantt_chart.py"],
            capture_output=True,
            text=True
        )

        print(result.stdout)
        print(result.stderr)

        return {
            "status": "success",
            "message": "Scheduler + Gantt chart generated"
        }

    except Exception as e:
        print("[ERROR]", str(e))
        return {
            "status": "error",
            "message": str(e)
        }