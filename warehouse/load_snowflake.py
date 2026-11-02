"""
Loads the cleaning outputs (Parquet) into Snowflake RAW tables.

Each run, for every dataset:
  1. uploads the Parquet files to the internal stage (PUT)
  2. recreates the RAW table from the files' own schema (INFER_SCHEMA)
  3. loads the data (COPY INTO)
  4. checks the Snowflake row count equals the local Parquet row count

Usage:
    python warehouse/load_snowflake.py                                   # data/processed
    python warehouse/load_snowflake.py --processed-dir data/sample/processed

Required environment variables (see set_env.ps1, never commit them):
    SF_ACCOUNT, SF_USER, SF_PRIVATE_KEY_PATH
"""
import argparse
import os
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
import snowflake.connector

TABLES = ["encounters", "diagnoses", "medications", "lookups", "rejected", "dq_summary"]


def parse_args():
    parser = argparse.ArgumentParser(description="Load Parquet outputs into Snowflake RAW")
    parser.add_argument("--processed-dir",
                        default=os.environ.get("PROCESSED_DIR", "data/processed"),
                        help="folder containing the six Parquet output folders")
    return parser.parse_args()


def require_env(name):
    value = os.environ.get(name)
    if not value:
        sys.exit(f"Missing environment variable {name}. Run  . .\\set_env.ps1  first.")
    return value


def main():
    args = parse_args()
    base = Path(args.processed_dir).resolve()
    if not base.exists():
        sys.exit(f"Folder not found: {base}. Run the cleaning pipeline first.")

    conn = snowflake.connector.connect(
        account=require_env("SF_ACCOUNT"),
        user=require_env("SF_USER"),
        private_key_file=require_env("SF_PRIVATE_KEY_PATH"),
        role="LOADER",
        warehouse="READMISSIONS_WH",
        database="READMISSIONS_DW",
        schema="RAW",
    )
    cur = conn.cursor()
    mismatches = []

    try:
        print(f"Loading from {base}\n")
        for t in TABLES:
            folder = base / t
            if not folder.exists():
                sys.exit(f"Missing output folder: {folder}")

            local_rows = pq.read_table(folder).num_rows
            posix_folder = folder.as_posix()

            # 1. Upload: clear old files first so the stage holds only this run's files
            cur.execute(f"REMOVE @PARQUET_STAGE/{t}/")
            for attempt in range(1, 4):
                try:
                    cur.execute(
                        f"PUT 'file://{posix_folder}/*.parquet' @PARQUET_STAGE/{t}/ "
                        f"AUTO_COMPRESS = FALSE OVERWRITE = TRUE PARALLEL = 2"
                    )
                    break
                except snowflake.connector.errors.OperationalError as err:
                    if attempt == 3:
                        raise
                    print(f"  {t}: upload attempt {attempt} failed ({err.msg[:60]}...), retrying")
                    time.sleep(10)

            # 2. Recreate the table from the Parquet schema (uppercase, unquoted column names)
            cur.execute(f"""
                CREATE OR REPLACE TABLE {t} USING TEMPLATE (
                  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                  FROM TABLE(INFER_SCHEMA(
                    LOCATION => '@PARQUET_STAGE/{t}/',
                    FILE_FORMAT => 'PARQUET_FMT',
                    IGNORE_CASE => TRUE)))
            """)

            # 3. Load
            cur.execute(f"""
                COPY INTO {t}
                FROM @PARQUET_STAGE/{t}/
                FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FMT')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            """)

            # 4. Verify
            sf_rows = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            status = "OK" if sf_rows == local_rows else "MISMATCH"
            print(f"  {t:<12} local={local_rows:>10,}  snowflake={sf_rows:>10,}  {status}")
            if sf_rows != local_rows:
                mismatches.append(t)
    finally:
        cur.close()
        conn.close()

    if mismatches:
        sys.exit(f"\nFAILED: row counts differ for {', '.join(mismatches)}")
    print("\nSUCCESS: all tables loaded and row counts match.")


if __name__ == "__main__":
    main()