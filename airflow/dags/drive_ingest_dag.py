from __future__ import annotations

import hashlib
import os
import subprocess
import gzip
import shutil
from datetime import datetime
from typing import List
import urllib.parse

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from minio import Minio

# CONFIG
RCLONE_CONFIG_PATH = "/home/airflow/.config/rclone/rclone.conf"
os.environ["RCLONE_CONFIG"] = RCLONE_CONFIG_PATH
RCLONE_REMOTE = "gdrive"
GDRIVE_FOLDER_ID = "1sMG3_NwBNgNfVewfb012Qy_cy1efUzkl"

MINIO_BUCKET = "loan-raw"
MINIO_ENDPOINT_RAW = os.getenv("MINIO_ENDPOINT", "http://minio:9000").rstrip("/")
# Minio SDK expects host:port (optionally scheme). Reject any path.
parsed = urllib.parse.urlparse(MINIO_ENDPOINT_RAW)
if parsed.scheme:
    if parsed.path and parsed.path != "":
        raise ValueError("MINIO_ENDPOINT must not include a path. Use http://minio:9000")
    MINIO_HOSTPORT = f"{parsed.hostname}:{parsed.port or 9000}"
    MINIO_SECURE = parsed.scheme == "https"
else:
    # Endpoint provided as 'host:port'
    if "/" in MINIO_ENDPOINT_RAW:
        raise ValueError("MINIO_ENDPOINT must be host:port without path (e.g., minio:9000)")
    MINIO_HOSTPORT = MINIO_ENDPOINT_RAW
    MINIO_SECURE = False

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_TEMP_DIR = os.getenv("MINIO_TEMP_DIR", "/opt/airflow/shared/tmp")
# Local dirs (top-level under /opt/airflow/shared)
RAW_DIR = os.getenv("RAW_DIR", "/opt/airflow/shared/raw")
PROCESSED_DIR = os.getenv("PROCESSED_DIR", "/opt/airflow/shared/processed")
COMPRESSED_DIR = os.getenv("COMPRESSED_DIR", "/opt/airflow/shared/compressed")

# Ensure dirs exist
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(COMPRESSED_DIR, exist_ok=True)
os.makedirs(MINIO_TEMP_DIR, exist_ok=True)

# Postgres config
PG_HOST = "postgres"
PG_PORT = 5432
PG_DB = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"

DEFAULT_ARGS = {"owner": "airflow", "retries": 1}


def file_hash(path: str) -> str:
    """Return SHA-256 of a file at path."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_tables():
    """Ensure metadata table exists. Uses its own short-lived connection."""
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER,
        password=PG_PASSWORD, dbname=PG_DB
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            id SERIAL PRIMARY KEY,
            filename TEXT NOT NULL,
            stored_filename TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            first_seen TIMESTAMPTZ DEFAULT now(),
            remote_path TEXT
        );
    """)
    conn.commit()
    cur.close()
    conn.close()


def list_remote_files() -> List[str]:
    cmd = [
        "rclone",
        "--config", RCLONE_CONFIG_PATH,
        "lsf",
        f"{RCLONE_REMOTE}:",
        "--drive-root-folder-id", GDRIVE_FOLDER_ID,
        "--files-only",
    ]
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    print(f"rclone stdout: {res.stdout}")
    print(f"rclone stderr: {res.stderr}")
    if res.returncode != 0:
        raise RuntimeError(f"rclone lsf failed: {res.stderr}")
    files = [x.strip() for x in res.stdout.splitlines() if x.strip()]
    files = [x for x in files if x.lower().endswith(".csv")]
    print(f"Discovered {len(files)} CSV file(s): {files}")
    return files


def compress_gzip(in_path: str, out_path: str) -> None:
    with open(in_path, "rb") as fin, gzip.open(out_path, "wb") as fout:
        shutil.copyfileobj(fin, fout)


def download_and_store(**context):
    minio_client = Minio(MINIO_HOSTPORT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=MINIO_SECURE)

    files = list_remote_files()
    if not files:
        print("No files found by rclone, nothing to do.")
        context["ti"].xcom_push(key="new_files", value=[])
        return []

    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(MINIO_TEMP_DIR, exist_ok=True)

    # OPEN DB CONNECTION AND CURSOR HERE
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER,
        password=PG_PASSWORD, dbname=PG_DB
    )
    cur = conn.cursor()

    stored_files = []
    unique_prefix = context["ts_nodash"]

    for fname in files:
        tmp_folder = os.path.join(MINIO_TEMP_DIR, unique_prefix)
        os.makedirs(tmp_folder, exist_ok=True)
        tmp_path = os.path.join(tmp_folder, fname)

        # Download into tmp
        cmd = [
            "rclone", "--config", RCLONE_CONFIG_PATH, "copyto",
            f"{RCLONE_REMOTE}:{fname}", tmp_path,
            "--drive-root-folder-id", GDRIVE_FOLDER_ID
        ]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if res.returncode != 0:
            print(f"rclone copyto failed for {fname}: {res.stderr}")
            continue

        sha256 = file_hash(tmp_path)

        # Check for prior record
        prev_name = None
        try:
            cur.execute("SELECT stored_filename FROM processed_files WHERE sha256=%s", (sha256,))
            row = cur.fetchone()
            if row:
                prev_name = row[0]
        except Exception as e:
            print(f"DB lookup failed: {e}")

        if prev_name:
            prev_local_csv = os.path.join(RAW_DIR, prev_name)
            # Check object in MinIO
            missing_in_minio = False
            try:
                minio_client.stat_object(MINIO_BUCKET, f"raw/{prev_name}")
            except Exception:
                missing_in_minio = True

            if os.path.isfile(prev_local_csv) and not missing_in_minio:
                print(f"Duplicate: {fname}, using existing local {prev_local_csv}")
                stored_files.append(prev_name)
                os.remove(tmp_path)
                continue

            # Restore missing duplicate
            shutil.move(tmp_path, prev_local_csv)  # cross-device safe
            prev_local_gz = os.path.join(COMPRESSED_DIR, prev_name + ".gz")
            compress_gzip(prev_local_csv, prev_local_gz)
            # Upload under partitioned MinIO prefixes
            minio_client.fput_object(MINIO_BUCKET, f"raw/{prev_name}", prev_local_csv)
            minio_client.fput_object(MINIO_BUCKET, f"compressed/{os.path.basename(prev_local_gz)}", prev_local_gz)
            print(f"Restored duplicate to MinIO (raw/, compressed/) and local copy")
            stored_files.append(prev_name)
            continue

        # New content path/name
        unique_filename = f"{unique_prefix}__{fname}"
        raw_csv_path = os.path.join(RAW_DIR, unique_filename)
        gz_path = os.path.join(COMPRESSED_DIR, unique_filename + ".gz")
        shutil.move(tmp_path, raw_csv_path)  # cross-device safe
        compress_gzip(raw_csv_path, gz_path)

        # Upload to MinIO using top-level prefixes
        minio_client.fput_object(MINIO_BUCKET, f"raw/{unique_filename}", raw_csv_path)
        minio_client.fput_object(MINIO_BUCKET, f"compressed/{os.path.basename(gz_path)}", gz_path)

        # Record metadata
        try:
            cur.execute("""
                INSERT INTO processed_files (filename, stored_filename, sha256, remote_path)
                VALUES (%s, %s, %s, %s)
            """, (fname, unique_filename, sha256, f"{RCLONE_REMOTE}:{fname}"))
            conn.commit()
        except Exception as e:
            print(f"DB insert failed: {e}")

        stored_files.append(unique_filename)

    print(f"Ensured local raw CSVs in {RAW_DIR}, compressed in {COMPRESSED_DIR}: {stored_files}")
    context["ti"].xcom_push(key="new_files", value=stored_files)
    return stored_files


with DAG(
    dag_id="drive_ingest_dag",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="* * * * *",
    catchup=False,
) as dag:
    download_task = PythonOperator(
        task_id="download_and_store",
        python_callable=download_and_store,
        provide_context=True
    )

    trigger_etl = TriggerDagRunOperator(
        task_id="trigger_spark_etl",
        trigger_dag_id="spark_etl_loan_dag",
        conf={"xcom_task": "download_and_store", "xcom_key": "new_files"},
        wait_for_completion=False
    )

    download_task >> trigger_etl
