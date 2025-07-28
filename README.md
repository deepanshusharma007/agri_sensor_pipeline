# 🚜 Agricultural Sensor Data Pipeline

**A robust data pipeline for agricultural sensor data ingestion, processing, validation, and analytics-ready storage.**

---

## 📚 Table of Contents

- [Project Overview](#project-overview)
- [Directory Structure](#directory-structure)
- [Installation & Environment Setup](#installation--environment-setup)
    - [Local Python Environment](#local-python-environment)
    - [Docker Setup (Recommended)](#docker-setup-recommended)
- [Pipeline Stages](#pipeline-stages)
    - [1. Ingestion](#1-ingestion)
    - [2. Transformation](#2-transformation)
    - [3. Validation & Reporting](#3-validation--reporting)
- [Sample Data Generation](#sample-data-generation)
- [Development & Testing](#development--testing)
- [Typical Workflow (Step by Step)](#typical-workflow-step-by-step)
- [Usage Examples](#usage-examples)
- [Extending the Pipeline](#extending-the-pipeline)
- [Troubleshooting](#troubleshooting)
- [Acknowledgments](#acknowledgments)

---

## 📝 Project Overview

This project implements a **production-grade, modular data pipeline** for agricultural sensor data. The system ingests Parquet files containing readings like soil moisture, temperature, humidity, and battery levels, validates and transforms the data, and stores clean, query-optimized, analytics-ready output.

**Key features:**
- Handles large-scale, time-partitioned sensor data.
- Performs incremental ingestion (only new files processed).
- Robust error logging (in DuckDB database).
- Cleans and enriches data, flags anomalies, applies calibration.
- Generates comprehensive data quality reports.
- Fully containerized with Docker for easy deployment.

---

## 🗂️ Directory Structure
---
agri_sensor_pipeline/
├── data/
│ ├── raw/ # Place incoming Parquet data files here
│ ├── processed/ # Output: cleaned/partitioned data by date
│ └── ingestion_log.duckdb # DuckDB log of ingestion events
├── scripts/
│ ├── ingest.py # Ingests new raw files, logs to DuckDB
│ ├── transform.py # Transforms new/unprocessed files
│ └── validate.py # Validates/transforms processed data
├── tests/
│ └── test_transform.py# Example unit test
├── Dockerfile # Docker environment definition
├── README.md # Project documentation (this file)
---


---

## ⚙️ Installation & Environment Setup

### 🐍 Local Python Environment

1. **Create & Activate Virtual Environment**  
python -m venv venv

Windows:
venv\Scripts\activate

Ubuntu/Mac:
source venv/bin/activate


2. **Install Requirements**  
pip install pandas duckdb pyarrow numpy python-snappy pytest
OR
pip install -r requirements.txt


### 🐳 Docker Setup (Recommended for Consistency)

1. **[Install Docker Desktop](https://www.docker.com/products/docker-desktop/)** (if not already installed).
2. **Build Docker Image**  
docker build -t agri-pipeline .
3. **Run Docker Container (with data volume mounted)**
docker run -it --rm -v "C:\Full Path till project directory(agri_senor_pipeline)\data:/app/data" agri-pipeline

> On Windows, use the absolute path if `%cd%/data` does not work.

4. **Inside Docker Bash, run pipeline scripts (see below).**

---

## ⚡ Pipeline Stages

### 1. **Ingestion**

- **Script:** `python scripts/ingest.py`
- **Purpose:**  
- Reads daily `.parquet` files in `data/raw/`
- Logs schema, row count, errors in a DuckDB table (`ingestion_log`)
- **Processes only new files** (files not yet in the log)

**DuckDB log:**
- Location: `data/ingestion_log.duckdb`
- Columns: filename, success, record count, error messages

### 2. **Transformation**

- **Script:** `python scripts/transform.py`
- **Purpose:**  
- Processes only untransformed files from `data/raw/`
- Cleans data (deduplication, missing values, z-score outlier detection)
- Applies calibration for each reading_type
- Adds anomaly flag if value is out of expected range
- Converts timestamps to UTC+5:30 (Asia/Kolkata)
- Produces daily and rolling averages
- Saves output as:  
 `data/processed/<date>/<source_file>_transformed.parquet`

### 3. **Validation & Reporting**

- **Script:** `python scripts/validate.py`
- **Modes:**  
- Validate all data (`VALIDATE_MODE = 'all'`)
- Only today (`VALIDATE_MODE = 'today'`)
- Only newest (`VALIDATE_MODE = 'latest'`)
- **Purpose:**  
- Profiles: missing values, anomaly %, battery level QC by reading type
- Checks hourly gaps for each sensor_id against expected ranges
- Outputs human-readable report:  
 `data/data_quality_report.csv`

---

---

## 🧑‍💻 Development & Testing

**Run unit tests:**
pytest
OR
python -m tests.test_transform (You need to be in the project root directory)

*(Make sure you’re in the project root, and both scripts/ and tests/ folder have an empty `__init__.py` file for package structure.)*

---

## 🔄 Typical Workflow (Step by Step)

1️⃣ Add new .parquet files to data/raw/
2️⃣ Run ingestion:
python scripts/ingest.py
3️⃣ Run transformation:
python scripts/transform.py
4️⃣ Generate validation report:
python scripts/validate.py
5️⃣ Inspect processed files in data/processed/<date>/
6️⃣ Review data_quality_report.csv for statistics


---


## 🛠️ Troubleshooting

- **No files processed?**  
  Ensure your `.parquet` files are in the `data/raw/` folder and have not already been ingested/transformed.

- **Files/folders created inside scripts/?**  
  Always run scripts from the **project root**, not from inside scripts/ or tests/.

- **Cannot import modules?**  
  Make sure you have `__init__.py` files in `scripts/` and `tests/`, and run with:

  python -m tests.test_transform
  or
  pytest



- **Docker data not visible?**  
Map your local `data/` folder using `-v` as shown above.

- **Python 3.12 issues?**  
All dependencies are compatible. If you face native build errors (very rare), try the latest `pip` or stick to Docker.

---

## 🏁 Acknowledgments

- [DuckDB](https://duckdb.org/) — for blazing fast analytics and logging
- Pandas, NumPy, PyArrow — for data munging
- Satsure (assessment provider)

---

**Project maintained by DEEPANSHU SHARMA.**  
For improvement ideas or feedback, open an issue or submit a pull request! or just drop a email at deepanshu2210sharma@gmail.com
