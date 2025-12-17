from __future__ import annotations

import hashlib
import json
import logging
import os
import socket
import smtplib
from pathlib import Path
from typing import Iterable

from airflow import DAG
from airflow.configuration import conf
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

LOCAL_SHARED_DIR = Path(os.getenv("LOCAL_SHARED_DIR", "/opt/airflow/shared"))
RAW_DIR = Path(os.getenv("RAW_DIR", LOCAL_SHARED_DIR / "raw"))
OUTPUT_DIR = Path(os.getenv("PROCESSED_DIR", LOCAL_SHARED_DIR / "processed"))
REGISTRY_PATH = Path(os.getenv("PROCESSED_REGISTRY", LOCAL_SHARED_DIR / "tmp" / "processed_registry.json"))


def _load_processed_ids() -> set[str]:
    if REGISTRY_PATH.exists():
        try:
            with REGISTRY_PATH.open(encoding="utf-8") as handle:
                payload = json.load(handle)
            return set(payload.get("processed_ids", []))
        except Exception as err:  # noqa: BLE001
            logging.warning("Failed loading registry %s: %s", REGISTRY_PATH, err)
    return set()


def _persist_ids(ids: Iterable[str]) -> None:
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with REGISTRY_PATH.open("w", encoding="utf-8") as handle:
        json.dump({"processed_ids": sorted(ids)}, handle, indent=2)


def _file_signature(path: Path) -> str:
    try:
        stat = path.stat()
        key = f"{path.name}:{stat.st_size}:{int(stat.st_mtime)}"
    except FileNotFoundError:
        key = path.as_posix()
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def collect_new_files() -> list[str]:
    if not RAW_DIR.exists():
        logging.warning("RAW_DIR %s does not exist; nothing to process.", RAW_DIR)
        return []

    candidates = [item for item in RAW_DIR.iterdir() if item.is_file() and item.suffix.lower() == ".csv"]
    if not candidates:
        logging.info("No CSV files found in %s.", RAW_DIR)
        return []

    processed = _load_processed_ids()
    fresh = [path.as_posix() for path in candidates if _file_signature(path) not in processed]

    logging.info("Found %d candidate CSV(s), %d unprocessed.", len(candidates), len(fresh))
    return fresh


def ensure_inputs(ti, **_):
    files = ti.xcom_pull(task_ids="fetch_files") or []
    if not files:
        raise AirflowSkipException("No new files to process.")
    ti.xcom_push(key="input_files", value=files)
    return True


def _human_size(num: int | None) -> str:
    if not isinstance(num, (int, float)):
        return "–"
    step = 1024.0
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    value = float(num)
    while value >= step and idx < len(units) - 1:
        value /= step
        idx += 1
    return f"{value:.2f} {units[idx]}"


def _resolve_output(base: Path, raw_name: str, *, keywords: Iterable[str] | None = None) -> Path | None:
    """Return the most recent file in *base* that maps to *raw_name*.

    Spark outputs include hashes (e.g. `foo_raw_deadbeef_processed_<ts>.csv.gz`),
    so we match by raw stem and optional keyword markers.
    """

    if not base.exists():
        return None

    direct = base / raw_name
    if direct.exists():
        return direct

    raw_stem = Path(raw_name).stem
    candidate_files: list[Path] = []

    for candidate in base.iterdir():
        if not candidate.is_file():
            continue
        if raw_stem not in candidate.stem:
            continue
        if keywords and not any(marker in candidate.name for marker in keywords):
            continue
        candidate_files.append(candidate)

    if not candidate_files:
        return None

    return max(candidate_files, key=lambda path: path.stat().st_mtime)


def record_processed(ti, **_):
    files = ti.xcom_pull(task_ids="has_inputs", key="input_files") or []
    processed_ids = _load_processed_ids()
    processed_ids.update(_file_signature(Path(path)) for path in files)
    _persist_ids(processed_ids)

    file_stats = []
    for raw_path_str in files:
        raw_path = Path(raw_path_str)

        original_size = raw_path.stat().st_size if raw_path.exists() else None

        processed_obj = _resolve_output(OUTPUT_DIR, raw_path.name, keywords=("_processed_",))
        processed_size = processed_obj.stat().st_size if processed_obj and processed_obj.exists() else None

        compressed_base = OUTPUT_DIR / "compressed"
        compressed_obj = _resolve_output(compressed_base, raw_path.name, keywords=("_compressed_", ".gz"))
        compressed_size = compressed_obj.stat().st_size if compressed_obj and compressed_obj.exists() else None

        ratio = (
            f"{original_size / compressed_size:.2f}"
            if original_size and compressed_size and compressed_size > 0
            else "–"
        )

        file_stats.append(
            {
                "filename": raw_path.name,
                "original_size": _human_size(original_size),
                "processed_size": _human_size(processed_size),
                "compressed_size": _human_size(compressed_size),
                "compression_ratio": ratio,
                "raw_uri": f"s3://uploaded-files/raw/{raw_path.name}",
                "processed_uri": f"s3://uploaded-files/processed/{processed_obj.name if processed_obj else raw_path.name}",
                "compressed_uri": f"s3://uploaded-files/compressed/{compressed_obj.name if compressed_obj else raw_path.name}",
            }
        )

    summary = {
        "raw_files": file_stats,
        "processed_dir": OUTPUT_DIR.as_posix(),
        "compressed_dir": (OUTPUT_DIR / "compressed").as_posix(),
        "file_count": len(file_stats),
    }
    ti.xcom_push(key="run_summary", value=summary)


def smtp_available(conn_id: str = "smtp_default") -> bool:
    try:
        conn = BaseHook.get_connection(conn_id)
        host = conn.host
        port = conn.port
        username = conn.login or conn.extra_dejson.get("smtp_user")
        password = conn.password or conn.extra_dejson.get("smtp_password")
        use_starttls = conn.extra_dejson.get("starttls", True)
        use_ssl = conn.extra_dejson.get("ssl", False)
    except Exception:  # noqa: BLE001
        host = conf.get("smtp", "smtp_host", fallback=None)
        port = conf.getint("smtp", "smtp_port", fallback=None)
        username = conf.get("smtp", "smtp_user", fallback=None)
        password = conf.get("smtp", "smtp_password", fallback=None)
        use_starttls = conf.getboolean("smtp", "smtp_starttls", fallback=True)
        use_ssl = conf.getboolean("smtp", "smtp_ssl", fallback=False)

    if not host or not port:
        logging.warning("SMTP host/port not configured; skipping email.")
        return False

    try:
        smtp_cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
        with smtp_cls(host=host, port=port, timeout=10) as client:
            client.ehlo()
            if use_starttls and not use_ssl:
                client.starttls()
                client.ehlo()
            if username and password:
                client.login(username, password)
            return True
    except Exception as err:  # noqa: BLE001
        logging.warning("SMTP health check failed: %s", err)
        return False


def build_email_html(ti, **_):
    summary = ti.xcom_pull(task_ids="record_processed", key="run_summary") or {}
    aggregates = ti.xcom_pull(task_ids="spark_etl_job", key="loan_aggregates") or []

    file_rows = "".join(
        f"""
            <tr>
                <td>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <span style="width:10px; height:10px; border-radius:50%; background:#38bdf8;"></span>
                        <span>{item.get('filename','–')}</span>
                    </div>
                </td>
                <td>{item.get('original_size','–')}</td>
                <td>{item.get('processed_size','–')}</td>
                <td>{item.get('compressed_size','–')}</td>
                <td><span style="padding:4px 10px; border-radius:999px; background:#fef3c7; color:#b45309;">{item.get('compression_ratio','–')}</span></td>
                <td><a href="{item.get('raw_uri','#')}" style="color:#38bdf8;">Raw</a></td>
                <td><a href="{item.get('processed_uri','#')}" style="color:#f97316;">Processed</a></td>
                <td><a href="{item.get('compressed_uri','#')}" style="color:#22c55e;">Compressed</a></td>
            </tr>
        """
        for item in summary.get("raw_files", [])
    ) or """
            <tr>
                <td colspan="8" style="text-align:center; padding:16px; color:#94a3b8;">
                    No files processed in this run.
                </td>
            </tr>
        """

    spark_rows = "".join(
        f"""
            <tr>
                <td><span style="padding:4px 12px; border-radius:999px; background:#dcfce7; color:#166534;">{row.get('status','–')}</span></td>
                <td>{row.get('product_type','–')}</td>
                <td>{row.get('branch','–')}</td>
                <td style="font-weight:600;">{row.get('loan_count','–')}</td>
                <td style="color:#38bdf8; font-weight:600;">{row.get('total_amount','–')}</td>
            </tr>
        """
        for row in aggregates[:5]
    ) or """
            <tr>
                <td colspan="5" style="text-align:center; padding:16px; color:#94a3b8;">
                    Spark job produced no aggregates.
                </td>
            </tr>
        """

    return f"""
    <html>
      <body style="margin:0; font-family:'Segoe UI',Roboto,Arial,sans-serif; background:#0f172a; padding:32px;">
        <table width="100%" cellpadding="0" cellspacing="0" style="max-width:960px; margin:auto; border-spacing:0;">
          <tr>
            <td style="padding:0;">
              <div style="background:radial-gradient(circle at top left,#2563eb,#0f172a 60%); border-radius:28px; overflow:hidden; box-shadow:0 30px 70px rgba(15,23,42,0.55);">
                <div style="padding:36px; color:#f8fafc;">
                  <h1 style="margin:0; font-size:30px; letter-spacing:0.8px;">Loan ETL — Execution Digest</h1>
                  <p style="margin:12px 0 0; opacity:0.85;">DAG: spark_etl_loan_dag · Run timestamp: {{ ts }}</p>
                </div>
                <div style="padding:0 36px 36px; background:#0f172a;">
                  <table width="100%" cellpadding="0" cellspacing="0" style="border-spacing:24px;">
                    <tr>
                      <td style="background:#1e293b; border-radius:20px; padding:24px; color:#e2e8f0;">
                        <p style="margin:0 0 6px; text-transform:uppercase; font-size:12px; letter-spacing:1.5px; color:#38bdf8;">Processed Files</p>
                        <p style="margin:0; font-size:28px; font-weight:600;">{summary.get('file_count', 0)}</p>
                      </td>
                      <td style="background:#1e293b; border-radius:20px; padding:24px; color:#e2e8f0;">
                        <p style="margin:0 0 6px; text-transform:uppercase; font-size:12px; letter-spacing:1.5px; color:#facc15;">Processed Dir</p>
                        <p style="margin:0; font-size:14px; color:#f8fafc;">{summary.get('processed_dir','–')}</p>
                      </td>
                      <td style="background:#1e293b; border-radius:20px; padding:24px; color:#e2e8f0;">
                        <p style="margin:0 0 6px; text-transform:uppercase; font-size:12px; letter-spacing:1.5px; color:#f472b6;">Compressed Dir</p>
                        <p style="margin:0; font-size:14px; color:#f8fafc;">{summary.get('compressed_dir','–')}</p>
                      </td>
                    </tr>
                  </table>

                  <div style="background:#111827; border-radius:24px; padding:28px;">
                    <h2 style="margin:0 0 12px; color:#e2e8f0;">Compression Summary</h2>
                    <p style="margin:0 0 18px; color:#94a3b8; font-size:14px;">
                      MinIO objects created after processing Google Drive uploads.
                    </p>
                    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse; overflow:hidden; border-radius:18px; background:#0f172a;">
                      <thead style="background:rgba(56,189,248,0.25); color:#e0f2fe; font-size:13px;">
                        <tr>
                          <th style="padding:14px 16px; text-align:left;">Filename</th>
                          <th style="padding:14px 16px; text-align:left;">Original Size</th>
                          <th style="padding:14px 16px; text-align:left;">Processed Size</th>
                          <th style="padding:14px 16px; text-align:left;">Compressed Size</th>
                          <th style="padding:14px 16px; text-align:left;">Ratio</th>
                          <th style="padding:14px 16px; text-align:left;">MinIO Raw</th>
                          <th style="padding:14px 16px; text-align:left;">MinIO Processed</th>
                          <th style="padding:14px 16px; text-align:left;">MinIO Compressed</th>
                        </tr>
                      </thead>
                      <tbody style="color:#cbd5f5; font-size:13px;">
                        {file_rows}
                      </tbody>
                    </table>
                  </div>

                  <div style="background:#111827; border-radius:24px; padding:28px; margin-top:24px;">
                    <h2 style="margin:0 0 12px; color:#e2e8f0;">Spark Loan Aggregates</h2>
                    <p style="margin:0 0 18px; color:#94a3b8; font-size:14px;">
                      Top segments by <strong style="color:#eab308;">loan_count</strong> from the latest Spark run.
                    </p>
                    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse; overflow:hidden; border-radius:18px; background:#0f172a;">
                      <thead style="background:rgba(16,185,129,0.25); color:#d1fae5; font-size:13px;">
                        <tr>
                          <th style="padding:14px 16px; text-align:left;">Status</th>
                          <th style="padding:14px 16px; text-align:left;">Product Type</th>
                          <th style="padding:14px 16px; te
                          xt-align:left;">Branch</th>
                          <th style="padding:14px 16px; text-align:left;">Loan Count</th>
                          <th style="padding:14px 16px; text-align:left;">Total Amount</th>
                        </tr>
                      </thead>
                      <tbody style="color:#cbd5f5; font-size:13px;">
                        {spark_rows}
                      </tbody>
                    </table>
                  </div>

                  <p style="margin:32px 0 0; color:#64748b; font-size:12px; text-align:center;">
                    Automated summary — Airflow · Bishal Shrestha
                  </p>
                </div>
              </div>
            </td>
          </tr>
        </table>
      </body>
    </html>
    """


with DAG(
    dag_id="spark_etl_loan_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "airflow"},
) as dag:
    fetch_files = PythonOperator(
        task_id="fetch_files",
        python_callable=collect_new_files,
    )

    has_inputs = ShortCircuitOperator(
        task_id="has_inputs",
        python_callable=ensure_inputs,
    )

    spark_etl = SparkSubmitOperator(
        task_id="spark_etl_job",
        application="/opt/airflow/sparks_jobs/loan_etl_job.py",
        name="Loan_ETL_Job",
        conn_id="spark_local",
        application_args=[
            "--input_files",
            "{{ ti.xcom_pull(task_ids='has_inputs', key='input_files') | join(',') }}",
            "--output_dir",
            OUTPUT_DIR.as_posix(),
            "--run_ts",
            "{{ ts_nodash }}",
        ],
        verbose=True,
        conf={"spark.master": "local[*]"},
    )

    record_done = PythonOperator(
        task_id="record_processed",
        python_callable=record_processed,
    )

    prepare_email = PythonOperator(
        task_id="prepare_email",
        python_callable=build_email_html,
    )

    smtp_ok = ShortCircuitOperator(
        task_id="smtp_available_check",
        python_callable=lambda: smtp_available("smtp_default"),
    )

    send_summary_email = EmailOperator(
        task_id="send_summary_email",
        to="{{ dag_run.conf.get('notify_to') or var.value.get('EMAIL_RECIPIENTS', '') }}",
        subject="Loan ETL Delivery Summary - {{ ts_nodash }}",
        html_content="{{ ti.xcom_pull(task_ids='prepare_email') }}",
        conn_id="smtp_default",
    )

    fetch_files >> has_inputs >> spark_etl >> record_done
    record_done >> prepare_email >> smtp_ok >> send_summary_email
