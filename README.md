# ETL: Spark + Airflow + MinIO (Loan ETL)

Small repository that ingests CSVs (from Google Drive or local), runs a Spark ETL job, stores outputs in MinIO (local S3-compatible), and sends a styled HTML run summary by email.

## Contents
- `airflow/dags/loan_spark_etl_dag.py` — DAG: discover raw CSVs, submit Spark job, record processed/compressed artifacts, build & send email.
- `airflow/sparks_jobs/loan_etl_job.py` — Spark ETL application (reads CSV(s), transforms, writes processed data).
- `docker-compose.yml` — local stack (Airflow, Postgres, MinIO, etc).
- `.gitignore` — recommended ignores to avoid committing secrets and runtime artifacts.
- `README.md` — this file.

## Prerequisites
- Docker & Docker Compose
- (Optional) Python + PySpark for local unit tests
- (Optional) `mc` (MinIO client) for inspecting MinIO

## Quickstart (local Docker)
1. Build and start stack:
   - PowerShell:
     ```powershell
     docker compose up -d --build
     ```
2. Verify raw directory inside the Airflow container:
   ```powershell
   docker exec -it etl_airflow_minio-airflow-1 bash -lc "ls -l /opt/airflow/shared/raw"
   ```
3. Place CSV files into the `raw` shared folder (or upload via your Drive sync):
   - Put files in host folder mapped to `/opt/airflow/shared/raw`.
4. Trigger the DAG (UI or CLI):
   ```powershell
   docker exec -it etl_airflow_minio-airflow-1 bash -lc "airflow dags trigger spark_etl_loan_dag"
   ```
5. Inspect outputs and registry:
   ```powershell
   docker exec -it etl_airflow_minio-airflow-1 bash -lc "ls -l /opt/airflow/shared/processed || true"
   docker exec -it etl_airflow_minio-airflow-1 bash -lc "cat /opt/airflow/shared/tmp/processed_registry.json || true"
   ```

## How the DAG decides what to process
- A signature (hash of `name:size:mtime`) is calculated per raw file and stored in:
  `/opt/airflow/shared/tmp/processed_registry.json`.
- Files with a signature already present are skipped (even if renamed).
- To force reprocessing remove the registry file or specific signatures:
  ```powershell
  docker exec -it etl_airflow_minio-airflow-1 bash -lc "rm -f /opt/airflow/shared/tmp/processed_registry.json"
  ```

## Email / SMTP configuration
- Airflow uses `smtp_default` connection and/or `AIRFLOW__SMTP__*` env vars.
- Example for Gmail (use App Password if 2FA enabled):
  - HOST: `smtp.gmail.com`
  - PORT: `587`
  - STARTTLS: `True`
  - USER: your@gmail.com
  - PASSWORD: 16-character app password


## What the email contains
- Per-file stats (original, processed, compressed size, ratio).
- MinIO URIs for raw/processed/compressed objects.
- Top Spark aggregates (pulled via XCom from the Spark job).
- Stylish HTML template — dynamically populated from XCom.

## Spark ETL details
- Reads CSV(s) passed via DAG application args.
- Performs cleaning/transforms; configured to add `<date>` and `<time>` columns for known timestamp fields.
- Writes processed outputs to `OUTPUT_DIR` and compressed files under `OUTPUT_DIR/compressed`.
- Pushes aggregate results to XCom key `loan_aggregates`.

## Useful commands (PowerShell)
- Run tests inside Airflow container:
- Trigger DAG:
  ```powershell
  docker exec -it etl_airflow_minio-airflow-1 bash -lc "airflow dags trigger spark_etl_loan_dag"
  ```
- View task logs:
  ```powershell
  docker exec -it etl_airflow_minio-airflow-1 bash -lc "airflow tasks logs spark_etl_loan_dag spark_etl_job <execution_date>"
  ```

## Troubleshooting
- Tasks unexpectedly skipped: check `fetch_files` output and `processed_registry.json`.
- Compressed/processed file info missing in email: ensure ETL writes to `OUTPUT_DIR` and compressed files to `OUTPUT_DIR/compressed`; the DAG resolves file stems to find matching processed/compressed files.
- Email fails: validate `smtp_default` connection, network reachability, and credentials; use MailHog for local dev if needed.

# etl_airflow_minio

Run:

1. Copy your Google service account JSON to `airflow/service_account.json`.
2. Update `.env` (GDRIVE_FOLDER_ID, SMTP password).
3. docker compose up --build

Airflow UI: http://localhost:8080  (user: admin / password: admin)
MinIO console: http://localhost:9001
MinIO API: http://localhost:9000
Postgres: localhost:5432
