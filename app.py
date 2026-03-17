from fastapi import FastAPI, UploadFile, File
import shutil
import os
import subprocess

app = FastAPI()


@app.get("/")
def root():
    return {"message": "SmartFab API is running"}


# 👉 upload demand + chạy pipeline
@app.post("/run")
async def run_pipeline(file: UploadFile = File(...)):

    # lưu file demand user upload
    file_location = f"data/{file.filename}"

    with open(file_location, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        # chạy job generator
        subprocess.run(
            ["python", "Engine/job_generator.py"],
            check=True
        )

        # chạy scheduler
        subprocess.run(
            ["python", "Engine/scheduler.py"],
            check=True
        )

        return {
            "status": "success",
            "message": "Schedule generated"
        }

    except subprocess.CalledProcessError as e:
        return {
            "status": "error",
            "message": str(e)
        }

from fastapi.responses import FileResponse

@app.get("/ui")
def ui():
    return FileResponse("index.html")