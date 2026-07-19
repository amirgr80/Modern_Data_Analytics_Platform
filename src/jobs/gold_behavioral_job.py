"""Silver Behavioral Iceberg -> ClickHouse Gold batch ETL."""

from __future__ import annotations

import argparse
from datetime import date, datetime
import logging
import os
import sys
from typing import Optional

CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(CURRENT_FILE_DIR)
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.silver_behavioral_spark_session import create_silver_behavioral_spark_session
from gold.behavioral_gold_clickhouse import replace_behavioral_gold_partition
from gold.behavioral_gold_config import GoldBehavioralConfig
from gold.behavioral_gold_transform import build_behavioral_gold_obt


logging.basicConfig(
    level=os.getenv("GOLD_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _safe_unpersist(dataframe: Optional[DataFrame]) -> None:
    if dataframe is not None:
        try:
            dataframe.unpersist()
        except Exception:
            logger.debug("Unable to unpersist DataFrame", exc_info=True)


def _assert_gold_contract(df: DataFrame, expected_columns: tuple[str, ...]) -> None:
    if tuple(df.columns) != expected_columns:
        raise RuntimeError(
            "Behavioral Gold schema mismatch. "
            f"Expected={list(expected_columns)}; actual={df.columns}."
        )
    if df.filter(F.col("event_key").isNull()).limit(1).count():
        raise RuntimeError("Behavioral Gold OBT produced a null event_key.")
    if df.filter(F.col("event_timestamp").isNull()).limit(1).count():
        raise RuntimeError("Behavioral Gold OBT produced a null event_timestamp.")
    duplicates = (
        df.groupBy("event_key")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicates:
        raise RuntimeError("Behavioral Gold OBT produced duplicate event_key rows.")


def run_gold_behavioral_job(execution_date: date) -> int:
    config = GoldBehavioralConfig.from_env()
    spark = create_silver_behavioral_spark_session(
        app_name=f"gold-behavioral-{execution_date.isoformat()}",
        config=config.silver,
    )
    gold_df = None
    try:
        gold_df = build_behavioral_gold_obt(spark, config, execution_date).cache()
        _assert_gold_contract(gold_df, config.target_columns)
        inserted = replace_behavioral_gold_partition(gold_df, config, execution_date)
        logger.info(
            "Gold Behavioral completed for %s with %s inserted rows",
            execution_date,
            inserted,
        )
        return inserted
    finally:
        _safe_unpersist(gold_df)
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Behavioral Silver -> Gold ETL")
    parser.add_argument(
        "--execution-date",
        required=True,
        help="Silver processing date in YYYY-MM-DD format.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_gold_behavioral_job(
        datetime.strptime(args.execution_date, "%Y-%m-%d").date()
    )
