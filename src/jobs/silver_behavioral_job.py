"""Independent Bronze -> Silver Behavioral batch ETL."""

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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.silver_behavioral_bronze_reader import read_bronze_behavioral_partition
from common.silver_behavioral_cleaning import clean_behavioral_data
from common.silver_behavioral_config import BehavioralRuntimeConfig
from common.silver_behavioral_iceberg_writer import MergeStrategy, merge_dataframe
from common.silver_behavioral_pipeline_state import run_key_for, write_pipeline_state
from common.silver_behavioral_quality_writer import (
    write_behavioral_quality_issues,
    write_behavioral_quarantine,
)
from common.silver_behavioral_schema import (
    TABLE_DIM_DEVICE,
    TABLE_DIM_EVENT_TYPE,
    TABLE_DIM_SESSION,
    TABLE_FACT_EVENTS,
    ensure_behavioral_tables,
)
from common.silver_behavioral_spark_session import (
    create_silver_behavioral_spark_session,
)
from common.silver_behavioral_transform import (
    add_dimension_keys,
    build_dim_device_updates,
    build_dim_event_type_updates,
    build_fact_behavioral_events,
    recompute_dim_session,
    resolve_shared_user_keys,
)
from common.silver_behavioral_validation import validate_behavioral_data


logging.basicConfig(
    level=os.getenv("SILVER_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _safe_unpersist(dataframe: Optional[DataFrame]) -> None:
    if dataframe is not None:
        try:
            dataframe.unpersist()
        except Exception:
            logger.debug("Unable to unpersist DataFrame", exc_info=True)


def _assert_batch_fact_contract(fact_df: DataFrame) -> None:
    if fact_df.filter(F.col("event_key").isNull()).limit(1).count():
        raise RuntimeError("Fact builder produced a null event_key.")
    if fact_df.filter(F.col("event_timestamp").isNull()).limit(1).count():
        raise RuntimeError("Fact builder produced a null event_timestamp.")
    duplicate = (
        fact_df.groupBy("event_key")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate:
        raise RuntimeError("Fact builder produced duplicate event_key values.")


def _assert_fact_write_contract(
    spark: SparkSession,
    fact_table: str,
    source_fact_df: DataFrame,
) -> None:
    """Verify the keys written by this batch, without scanning unrelated facts."""

    source_keys = source_fact_df.select("event_key").distinct()
    target_keys = spark.table(fact_table).select("event_key")

    missing = source_keys.join(target_keys, "event_key", "left_anti").limit(1).count()
    if missing:
        raise RuntimeError("At least one source event_key is missing after Iceberg MERGE.")

    duplicate = (
        target_keys.join(source_keys, "event_key", "inner")
        .groupBy("event_key")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate:
        raise RuntimeError(
            "Iceberg fact contains duplicate rows for a key touched by this batch."
        )


def run_silver_behavioral_job(execution_date: date) -> None:
    config = BehavioralRuntimeConfig.from_env()
    spark = create_silver_behavioral_spark_session(
        app_name=f"silver-behavioral-{execution_date.isoformat()}",
        config=config,
    )

    processable_df = None
    rejected_df = None
    clean_df = None
    enriched_df = None
    quality_df = None
    fact_df = None

    try:
        ensure_behavioral_tables(spark, config)
        write_pipeline_state(spark, config, execution_date, "RUNNING")
        pipeline_run_id = run_key_for(execution_date)

        bronze_df = read_bronze_behavioral_partition(
            spark=spark,
            execution_date=execution_date,
            pipeline_run_id=pipeline_run_id,
            config=config,
        )
        raw_count = bronze_df.count()
        if raw_count == 0:
            logger.warning("Bronze partition is empty for %s", execution_date)
            write_pipeline_state(
                spark,
                config,
                execution_date,
                "EMPTY",
                raw_count=0,
                valid_count=0,
                warning_count=0,
                processable_count=0,
                rejected_count=0,
                fact_rows_merged=0,
            )
            return

        validation = validate_behavioral_data(bronze_df)
        processable_df = validation.processable_df.cache()
        rejected_df = validation.rejected_df.cache()
        quality_df = validation.quality_issues_df.cache()

        processable_count = processable_df.count()
        valid_count = processable_df.filter(
            F.size("validation_warnings") == 0
        ).count()
        warning_count = processable_count - valid_count
        rejected_count = rejected_df.count()

        if processable_count + rejected_count != raw_count:
            raise RuntimeError(
                "Validation split changed the input grain: "
                f"raw={raw_count}, processable={processable_count}, "
                f"rejected={rejected_count}."
            )

        quality_written = write_behavioral_quality_issues(
            spark,
            quality_df,
            config,
        )
        quarantine_written = write_behavioral_quarantine(
            spark,
            rejected_df,
            config,
        )
        _safe_unpersist(quality_df)
        quality_df = None
        logger.info(
            "Validation raw=%s valid=%s warning_records=%s processable=%s "
            "rejected=%s quality_issues=%s quarantine=%s",
            raw_count,
            valid_count,
            warning_count,
            processable_count,
            rejected_count,
            quality_written,
            quarantine_written,
        )

        if processable_count == 0:
            write_pipeline_state(
                spark,
                config,
                execution_date,
                "SUCCEEDED",
                raw_count=raw_count,
                valid_count=valid_count,
                warning_count=warning_count,
                processable_count=0,
                rejected_count=rejected_count,
                fact_rows_merged=0,
            )
            return

        clean_df = clean_behavioral_data(processable_df).cache()
        enriched_df = add_dimension_keys(clean_df)
        enriched_df = resolve_shared_user_keys(spark, enriched_df, config).cache()
        enriched_count = enriched_df.count()
        if enriched_count != processable_count:
            raise RuntimeError(
                "Shared-dimension enrichment changed the event grain: "
                f"processable={processable_count}, enriched={enriched_count}."
            )

        devices = merge_dataframe(
            spark,
            config.qualified_table(TABLE_DIM_DEVICE),
            build_dim_device_updates(enriched_df),
            merge_keys=("device_key",),
            strategy=MergeStrategy.UPSERT_PRESERVE_BOUNDS,
            protected_columns=("device_key",),
            min_columns=("first_seen_at",),
            max_columns=("last_seen_at",),
        )
        event_types = merge_dataframe(
            spark,
            config.qualified_table(TABLE_DIM_EVENT_TYPE),
            build_dim_event_type_updates(enriched_df),
            merge_keys=("event_type_key",),
            strategy=MergeStrategy.UPSERT_PRESERVE_BOUNDS,
            protected_columns=("event_type_key",),
            min_columns=("first_seen_at",),
            max_columns=("last_seen_at",),
        )

        fact_df = build_fact_behavioral_events(enriched_df).cache()
        _assert_batch_fact_contract(fact_df)
        fact_table = config.qualified_table(TABLE_FACT_EVENTS)
        facts = merge_dataframe(
            spark,
            fact_table,
            fact_df,
            merge_keys=("event_key",),
            strategy=MergeStrategy.FACT_DEDUPLICATE_INSERT,
        )
        _assert_fact_write_contract(spark, fact_table, fact_df)

        sessions = merge_dataframe(
            spark,
            config.qualified_table(TABLE_DIM_SESSION),
            recompute_dim_session(
                spark,
                fact_table,
                fact_df.select("session_key").distinct(),
            ),
            merge_keys=("session_key",),
            strategy=MergeStrategy.UPSERT_ALL,
            protected_columns=("session_key",),
        )
        _safe_unpersist(fact_df)
        fact_df = None

        logger.info(
            "Behavioral writes: devices=%s event_types=%s facts=%s sessions=%s",
            devices,
            event_types,
            facts,
            sessions,
        )

        write_pipeline_state(
            spark,
            config,
            execution_date,
            "SUCCEEDED",
            raw_count=raw_count,
            valid_count=valid_count,
            warning_count=warning_count,
            processable_count=processable_count,
            rejected_count=rejected_count,
            fact_rows_merged=facts,
        )
        logger.info("Silver Behavioral completed successfully for %s", execution_date)

    except Exception as exc:
        logger.exception("Silver Behavioral failed for %s", execution_date)
        try:
            write_pipeline_state(
                spark,
                config,
                execution_date,
                "FAILED",
                error_message=str(exc)[:4000],
            )
        except Exception:
            logger.exception("Could not persist FAILED pipeline state")
        raise
    finally:
        _safe_unpersist(fact_df)
        _safe_unpersist(quality_df)
        _safe_unpersist(enriched_df)
        _safe_unpersist(clean_df)
        _safe_unpersist(processable_df)
        _safe_unpersist(rejected_df)
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Silver Behavioral batch ETL")
    parser.add_argument(
        "--execution-date",
        required=True,
        help="Bronze partition date in YYYY-MM-DD format.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_silver_behavioral_job(
        datetime.strptime(args.execution_date, "%Y-%m-%d").date()
    )
