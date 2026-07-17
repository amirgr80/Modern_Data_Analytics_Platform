"""Server-side smoke test for Spark, Lakekeeper, MinIO and Behavioral DDL."""

from __future__ import annotations

import argparse
from datetime import datetime
import os
import sys

CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(CURRENT_FILE_DIR)
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from common.silver_behavioral_bronze_reader import read_bronze_behavioral_partition
from common.silver_behavioral_config import BehavioralRuntimeConfig
from common.silver_behavioral_pipeline_state import run_key_for
from common.silver_behavioral_schema import (
    BEHAVIORAL_TABLES,
    TABLE_QUALITY,
    ensure_behavioral_tables,
)
from common.silver_behavioral_spark_session import create_silver_behavioral_spark_session


def main(execution_date: str | None) -> int:
    config = BehavioralRuntimeConfig.from_env()
    spark = create_silver_behavioral_spark_session("verify-silver-behavioral", config)
    try:
        ensure_behavioral_tables(spark, config)
        for table in BEHAVIORAL_TABLES:
            spark.table(config.qualified_table(table)).limit(0).collect()
        spark.table(
            config.qualified_table(TABLE_QUALITY, config.quality_namespace)
        ).limit(0).collect()
        if execution_date:
            parsed = datetime.strptime(execution_date, "%Y-%m-%d").date()
            df = read_bronze_behavioral_partition(
                spark,
                parsed,
                run_key_for(parsed),
                config,
            )
            print(f"Bronze rows for {parsed}: {df.count()}")
        print("Silver Behavioral runtime smoke test: PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-date")
    args = parser.parse_args()
    raise SystemExit(main(args.execution_date))
