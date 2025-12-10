# etl_airflow_minio

Run:

1. Copy your Google service account JSON to `airflow/service_account.json`.
2. Update `.env` (GDRIVE_FOLDER_ID, SMTP password).
3. docker compose up --build

Airflow UI: http://localhost:8080  (user: admin / password: admin)
MinIO console: http://localhost:9001
MinIO API: http://localhost:9000
Postgres: localhost:5432
