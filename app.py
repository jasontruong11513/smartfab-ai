from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import shutil
import subprocess
import sys
import os

app = FastAPI()

# CORS fix cho web gọi API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


@app.get("/")
def root():
    return {"message": "SmartFab API is running"}


# =========================
# Helper chạy script
# =========================
def run_script(script):
    full_path = os.path.join(BASE_DIR, script)

    result = subprocess.run(
        [sys.executable, full_path],
        capture_output=True,
        text=True
    )

    print("=== RUN:", script, "===")
    print(result.stdout)
    print(result.stderr)

    if result.returncode != 0:
        raise Exception(result.stderr)


# =========================
# MAIN PIPELINE
# =========================
@app.post("/run")
async def run_pipeline(file: UploadFile = File(...)):

    try:
        os.makedirs("data", exist_ok=True)
        os.makedirs("database", exist_ok=True)

        # 🔥 1. save demand file
        demand_path = os.path.join("data", "demand.csv")

        with open(demand_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        print("[INFO] Demand uploaded")

        # 🔥 2. run pipeline đúng thứ tự
        run_script("Engine/job_generator.py")
        run_script("Engine/routing_generator.py")   # ❗ quan trọng
        run_script("Engine/factory_assignment.py")
        run_script("Engine/scheduler.py")
        run_script("Engine/gantt_chart.py")

        return {
            "status": "success",
            "message": "Pipeline completed",
            "outputs": [
                "database/generated_jobs.csv",
                "database/job_process_flow.csv",
                "database/schedule_final.csv",
                "gantt_chart.html"
            ]
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

from fastapi.responses import FileResponse

@app.get("/ui")
def ui():
    return FileResponse("index.html")