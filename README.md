# 🏭 SmartFab AI

AI-Powered Smart Manufacturing Planning & Scheduling Platform

SmartFab AI transforms production demand forecasts into optimized manufacturing schedules through an automated planning pipeline that generates jobs, assigns factories, schedules production activities, and visualizes results using interactive Gantt charts.

---

## 🚀 Overview

Manufacturing operations often require multiple manual planning steps before production can begin.

SmartFab AI automates this process by converting uploaded demand forecasts into executable production schedules.

The platform performs:

* Demand Processing
* Job Generation
* Process Routing
* Factory Assignment
* Production Scheduling
* Gantt Chart Visualization

The result is a streamlined workflow that helps planners reduce manual effort and improve production visibility.

---

## 🎯 Key Features

### 📁 Demand Forecast Upload

Upload production demand files in CSV format.

### ⚙️ Automated Job Generation

Convert demand forecasts into manufacturing jobs.

### 🔄 Process Routing

Automatically generate process flows for each product.

### 🏭 Factory Assignment

Assign production jobs to appropriate factories or production units.

### 📅 Production Scheduling

Generate executable manufacturing schedules.

### 📊 Interactive Gantt Charts

Visualize schedules and production timelines using Plotly.

### 🌐 Web API

FastAPI-powered backend for integration with dashboards and enterprise systems.

---

## 🏗 System Architecture

```text
Demand Forecast CSV
         │
         ▼
 Job Generator
         │
         ▼
 Routing Generator
         │
         ▼
 Factory Assignment
         │
         ▼
 Production Scheduler
         │
         ▼
 Gantt Chart Generator
         │
         ▼
 Production Plan Dashboard
```

---

## ⚡ Technology Stack

### Backend

* FastAPI
* Python

### Data Processing

* Pandas

### Visualization

* Plotly

### Deployment

* Uvicorn

### API Features

* REST API
* File Upload Support
* Automated Workflow Execution

---

## 📂 Project Structure

```text
smartfab-ai/
│
├── app.py
├── requirements.txt
├── index.html
│
├── Engine/
│   ├── job_generator.py
│   ├── routing_generator.py
│   ├── factory_assignment.py
│   ├── scheduler.py
│   └── gantt_chart.py
│
├── data/
│   └── demand.csv
│
├── database/
│   ├── generated_jobs.csv
│   ├── job_process_flow.csv
│   └── schedule_final.csv
│
└── gantt_chart.html
```

---

## 🔄 Workflow

### Step 1

Upload demand forecast data.

### Step 2

Generate manufacturing jobs.

### Step 3

Create routing process flows.

### Step 4

Assign jobs to production facilities.

### Step 5

Generate optimized schedules.

### Step 6

Visualize production timelines through Gantt charts.

---

## 📡 API Endpoints

### Health Check

```http
GET /
```

Response:

```json
{
  "message": "SmartFab API is running"
}
```

### Run Planning Pipeline

```http
POST /run
```

Upload:

```text
multipart/form-data
```

Input:

```text
demand.csv
```

Output:

```json
{
  "status": "success",
  "message": "Pipeline completed"
}
```

### Web Interface

```http
GET /ui
```

Launches the SmartFab dashboard interface.

---

## 🛠 Installation

### Clone Repository

```bash
git clone https://github.com/jasontruong11513/smartfab-ai.git

cd smartfab-ai
```

### Create Virtual Environment

```bash
python -m venv venv
```

Activate:

Windows

```bash
venv\Scripts\activate
```

Mac/Linux

```bash
source venv/bin/activate
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Start Application

```bash
uvicorn app:app --reload
```

Application:

```text
http://127.0.0.1:8000
```

Swagger API:

```text
http://127.0.0.1:8000/docs
```

---

## 📈 Example Outputs

Generated files:

```text
generated_jobs.csv
job_process_flow.csv
schedule_final.csv
gantt_chart.html
```

These outputs provide complete visibility into production planning and scheduling activities.

---

## 🎓 Use Cases

* Smart Manufacturing
* Factory Operations Planning
* Production Scheduling
* Supply Chain Optimization
* Manufacturing Analytics
* Capacity Planning
* Digital Factory Initiatives

---

## 🔮 Future Improvements

Potential enhancements:

* Machine Learning Demand Forecasting
* Reinforcement Learning Scheduling
* Multi-Factory Optimization
* Capacity Constraints Modeling
* ERP Integration
* Real-Time Shop Floor Monitoring
* Predictive Maintenance Integration
* AI Copilot for Production Planning

---

## 👨‍💻 Author

Jason Truong

MS Information Systems Candidate

California State University, Long Beach

---

## 📄 License

This project is provided for educational and portfolio purposes.
