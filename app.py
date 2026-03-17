from fastapi import FastAPI, UploadFile
import shutil
import pandas as pd
import os

# ✅ import từ Engine
from Engine.job_generator import generate_jobs
from Engine.routing_generator import generate_routing
from Engine.factory_assignment import generate_product_factory_assignment
from Engine import scheduler  # gọi scheduler.main()

app = FastAPI()


@app.post("/run-planning")
async def run_planning(file: UploadFile):

    # =========================
    # 1. đảm bảo folder tồn tại
    # =========================
    os.makedirs("data", exist_ok=True)
    os.makedirs("database", exist_ok=True)

    # =========================
    # 2. lưu demand file
    # =========================
    file_path = f"data/{file.filename}"

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    print(f"[INFO] Demand file saved: {file_path}")

    # =========================
    # 3. generate jobs
    # =========================
    jobs = generate_jobs(
        file_path,
        "data/Produc_Master_file.csv"
    )

    jobs.to_csv("database/generated_jobs.csv", index=False)
    print("[INFO] Jobs generated")

    # =========================
    # 4. routing
    # =========================
    routing = generate_routing(
        "database/generated_jobs.csv",
        "data/production_matrix.csv"
    )

    routing.to_csv("database/job_process_flow.csv", index=False)
    print("[INFO] Routing generated")

    # =========================
    # 5. factory assignment
    # =========================
    factory = generate_product_factory_assignment(
        "data/production_matrix.csv"
    )

    factory.to_csv("database/product_factory_assignment.csv", index=False)
    print("[INFO] Factory assigned")

    # =========================
    # 6. scheduler
    # =========================
    print("[INFO] Running scheduler...")
    scheduler.main()

    
    #6.1 gantt chart
    import sys
    import subprocess

    subprocess.run([sys.executable, "Engine/gantt_chart.py"])
    
    # =========================
    # 7. đọc kết quả
    # =========================
    schedule = pd.read_csv("database/schedule_final.csv")

    total_jobs = schedule["job_id"].nunique()
    total_ops = len(schedule)

    try:
        print("[DEBUG] Running scheduler...")
        scheduler.main()

        print("[DEBUG] Loading schedule...")
        schedule = pd.read_csv("database/schedule_final.csv")

        total_jobs = schedule["job_id"].nunique()
        total_ops = len(schedule)

        return {
            "status": "success",
            "total_jobs": int(total_jobs),
            "total_operations": int(total_ops)
        }

    except Exception as e:
        print("[ERROR]", str(e))
        return {
            "status": "error",
            "message": str(e)
        }

    return {
        "status": "success",
        "total_jobs": int(total_jobs),
        "total_operations": int(total_ops),
        "insight": "Bottleneck detected at plating and coating processes"
        }


