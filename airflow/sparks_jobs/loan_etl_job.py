import argparse
import os
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from py4j.java_gateway import java_import
from datetime import datetime, timezone
import hashlib
from functools import reduce  # <-- add this import

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input_files", required=True, help="Comma-separated CSV paths")
    p.add_argument("--output_dir", required=True, help="Output directory")
    p.add_argument("--run_ts", required=False, help="Run timestamp, e.g. 20251209T101500")
    return p.parse_args()

def normalize_name(c: str) -> str:
    return (
        c.strip()
         .lower()
         .replace(" ", "_")
    )

HEADER_MAP = {
    "loanid": "loan_id",
    "borrowername": "borrower_name",
    "amount": "amount",
    "interestrate": "interest_rate",
    "termmonths": "tenure_months",
    "status": "status",
    "created_at": "created_at",
    "createdon": "created_at",
    "created_on": "created_at",
}

DATE_PATTERNS = [
    "yyyy-MM-dd",
    "yyyy/MM/dd",
    "MM/dd/yyyy",
    "dd/MM/yyyy",
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd HH:mm:ss",
    "MM/dd/yyyy HH:mm",
    "MM/dd/yyyy HH:mm:ss",
    "M/d/yyyy",
    "M/d/yyyy HH:mm",
    "M/d/yyyy HH:mm:ss",
    "d/M/yyyy",
    "d/M/yyyy HH:mm",
    "d/M/yyyy HH:mm:ss",
]

DATE_SPLIT_COLUMNS = {"created_at", "created_on", "updated_at", "application_date"}


def _timestamp_from_patterns(column_name: str):
    expressions = [F.to_timestamp(F.col(column_name), fmt) for fmt in DATE_PATTERNS]
    return F.coalesce(*expressions) if expressions else F.lit(None)


def clean_df(df):
    # Trim all string columns
    str_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in str_cols:
        df = df.withColumn(c, F.trim(F.col(c)))

    # Normalize column names and map to canonical names
    for old in df.columns:
        norm = normalize_name(old)
        canonical = HEADER_MAP.get(norm, norm)
        if old != canonical:
            df = df.withColumnRenamed(old, canonical)

    # Safe casts
    if "amount" in df.columns:
        df = df.withColumn("amount", F.col("amount").cast("double"))
    if "interest_rate" in df.columns:
        df = df.withColumn("interest_rate", F.col("interest_rate").cast("double"))
    if "tenure_months" in df.columns:
        df = df.withColumn("tenure_months", F.col("tenure_months").cast("int"))
    if "status" in df.columns:
        df = df.withColumn("status", F.lower(F.col("status")))

    # Drop fully empty rows safely
    non_empty_exprs = [(F.col(c).isNotNull()) & (F.col(c) != "") for c in df.columns]
    if non_empty_exprs:
        df = df.filter(reduce(lambda a, b: a | b, non_empty_exprs))

    # Basic validations
    if "amount" in df.columns:
        df = df.filter((F.col("amount").isNull()) | (F.col("amount") >= 0))
    if "interest_rate" in df.columns:
        df = df.filter((F.col("interest_rate").isNull()) | (F.col("interest_rate").between(0, 100)))
    if "tenure_months" in df.columns:
        df = df.filter((F.col("tenure_months").isNull()) | (F.col("tenure_months") > 0))

    for date_col in DATE_SPLIT_COLUMNS.intersection(df.columns):
        ts_alias = f"__{date_col}_ts"
        df = df.withColumn(ts_alias, _timestamp_from_patterns(date_col))
        df = df.withColumn(f"{date_col}_date", F.to_date(F.col(ts_alias)))
        df = df.withColumn(f"{date_col}_time", F.date_format(F.col(ts_alias), "HH:mm:ss"))
        df = df.drop(ts_alias)

    return df

def main():
    args = parse_args()
    run_ts = args.run_ts or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    inputs = [p.strip() for p in args.input_files.split(",") if p.strip()]

    spark = SparkSession.builder.appName("Loan_ETL_Job").getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    # Hadoop FS helpers
    jsc = spark._jsc
    hconf = jsc.hadoopConfiguration()
    jvm = spark._jvm
    java_import(jvm, "org.apache.hadoop.fs.*")
    fs = jvm.FileSystem.get(hconf)

    # Ensure output_dir exists
    fs.mkdirs(jvm.Path(args.output_dir))

    for path in inputs:
        df = spark.read.option("header", True).option("inferSchema", True).csv(path)
        df = clean_df(df)

        # Build output file name: <basename>_processed_<run_ts>.csv.gz
        parsed = urlparse(path)
        in_name = os.path.basename(parsed.path)
        base, _ext = os.path.splitext(in_name)
        parent = os.path.basename(os.path.dirname(parsed.path)) or "root"
        short = hashlib.sha1(path.encode("utf-8")).hexdigest()[:8]
        out_name = f"{base}_{parent}_{short}_processed_{run_ts}.csv.gz"

        # Write (gzip) to a temp directory, then rename the single part file
        tmp_dir = os.path.join(args.output_dir, f".tmp_{base}")
        df.coalesce(1) \
          .write.mode("overwrite") \
          .option("header", True) \
          .option("compression", "gzip") \
          .csv(tmp_dir)

        tmp_path = jvm.Path(tmp_dir)
        status = fs.listStatus(tmp_path)
        part_file = None
        for s in status:
            p = s.getPath().getName()
            if p.startswith("part-") and p.endswith(".csv.gz"):
                part_file = s.getPath()
                break
        if part_file is None:
            raise RuntimeError(f"Could not find part file in {tmp_dir}")

        final_path = jvm.Path(os.path.join(args.output_dir, out_name))
        if fs.exists(final_path):
            fs.delete(final_path, True)
        fs.rename(part_file, final_path)
        fs.delete(tmp_path, True)

    spark.stop()

if __name__ == "__main__":
    main()
