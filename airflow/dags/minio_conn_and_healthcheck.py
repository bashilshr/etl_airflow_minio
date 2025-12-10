from __future__ import annotations
import os
import json
from datetime import datetime
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.models import Connection
from airflow.settings import Session
from airflow.operators.python import PythonOperator

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "loan-raw")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")
AIRFLOW_CONN_ID = os.getenv("AIRFLOW_MINIO_CONN_ID", "minio_s3")

def _ensure_airflow_conn() -> None:
    # Create or update an Airflow connection for MinIO
    conn_extras = {
        "endpoint_url": MINIO_ENDPOINT,
        "aws_access_key_id": MINIO_ACCESS_KEY,
        "aws_secret_access_key": MINIO_SECRET_KEY,
        "region_name": MINIO_REGION,
        "addressing_style": "path",
        "signature_version": "s3v4",
    }

    session = Session()
    try:
        conn = session.query(Connection).filter(Connection.conn_id == AIRFLOW_CONN_ID).one_or_none()
        if conn is None:
            conn = Connection(
                conn_id=AIRFLOW_CONN_ID,
                conn_type="s3",
                extra=json.dumps(conn_extras),
            )
            session.add(conn)
            session.commit()
            print(f"Created Airflow Connection '{AIRFLOW_CONN_ID}'.")
        else:
            conn.conn_type = "s3"
            conn.extra = json.dumps(conn_extras)
            session.commit()
            print(f"Updated Airflow Connection '{AIRFLOW_CONN_ID}'.")
    finally:
        session.close()

def _get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )

def _ensure_bucket() -> None:
    s3 = _get_s3_client()
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
        print(f"Bucket '{MINIO_BUCKET}' exists.")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchBucket"):
            if MINIO_REGION == "us-east-1":
                s3.create_bucket(Bucket=MINIO_BUCKET)
            else:
                s3.create_bucket(
                    Bucket=MINIO_BUCKET,
                    CreateBucketConfiguration={"LocationConstraint": MINIO_REGION},
                )
            print(f"Bucket '{MINIO_BUCKET}' created.")
        else:
            raise

def _list_bucket() -> None:
    s3 = _get_s3_client()
    try:
        resp = s3.list_objects_v2(Bucket=MINIO_BUCKET, MaxKeys=10)
        contents = [o["Key"] for o in resp.get("Contents", [])]
        print(f"Listed {len(contents)} object(s) in '{MINIO_BUCKET}': {contents}")
    except ClientError as e:
        print(f"Failed to list objects in '{MINIO_BUCKET}': {e}")
        raise

with DAG(
    dag_id="minio_conn_and_healthcheck",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["health", "minio", "s3"],
) as dag:
    ensure_conn = PythonOperator(
        task_id="ensure_airflow_connection",
        python_callable=_ensure_airflow_conn,
    )

    ensure_bucket = PythonOperator(
        task_id="ensure_bucket",
        python_callable=_ensure_bucket,
    )

    list_bucket = PythonOperator(
        task_id="list_bucket",
        python_callable=_list_bucket,
    )

    ensure_conn >> ensure_bucket >> list_bucket