"""Safe migration of legacy Behavioral event keys.

The migration never assumes that partition+offset is globally unique.  It
uses topic+partition+offset whenever topic is present.  For older target rows
with a null topic it first proves that the corresponding Bronze coordinates
map to exactly one topic; otherwise it fails without writing.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(CURRENT_FILE_DIR)
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from pyspark.sql import functions as F

from common.silver_behavioral_config import BehavioralRuntimeConfig
from common.silver_behavioral_iceberg_writer import ensure_columns, table_exists
from common.silver_behavioral_keys import (
    event_identity_source_expression,
    event_key_expression,
)
from common.silver_behavioral_schema import TABLE_FACT_EVENTS, ensure_behavioral_tables
from common.silver_behavioral_spark_session import create_silver_behavioral_spark_session


logging.basicConfig(
    level=os.getenv("SILVER_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

LEGACY_EVENT_KEY_PATTERN = r"^[0-9]+_[0-9]+$"


def run_migration(dry_run: bool, target_table: str | None = None) -> int:
    config = BehavioralRuntimeConfig.from_env()
    spark = create_silver_behavioral_spark_session(
        "silver-behavioral-key-migration",
        config,
    )
    try:
        ensure_behavioral_tables(spark, config)
        target = target_table or os.getenv(
            "BEHAVIORAL_MIGRATION_TARGET_TABLE",
            config.qualified_table(TABLE_FACT_EVENTS),
        )
        if not table_exists(spark, target):
            raise RuntimeError(f"Migration target does not exist: {target}")

        ensure_columns(
            spark,
            target,
            {
                "event_id": "STRING",
                "kafka_topic": "STRING",
                "event_identity_source": "STRING",
            },
        )
        target_df = spark.table(target)
        required_target = {"event_key", "kafka_partition", "kafka_offset"}
        missing = required_target - set(target_df.columns)
        if missing:
            raise RuntimeError(f"Migration target is missing columns: {sorted(missing)}")

        stale = target_df.filter(F.col("event_key").rlike(LEGACY_EVENT_KEY_PATTERN))
        stale_count = stale.count()
        logger.info("Legacy event-key rows in %s: %s", target, stale_count)
        if stale_count == 0:
            return 0

        bronze = (
            spark.read.format("parquet")
            .load(config.bronze_path)
            .select(
                F.col("event_id").cast("string").alias("event_id"),
                F.col("kafka_topic").cast("string").alias("kafka_topic"),
                F.col("kafka_partition").cast("int").alias("kafka_partition"),
                F.col("kafka_offset").cast("bigint").alias("kafka_offset"),
            )
            .dropDuplicates(["kafka_topic", "kafka_partition", "kafka_offset"])
        )

        stale_coordinates = stale.select("kafka_partition", "kafka_offset").distinct()
        ambiguous = (
            bronze.join(stale_coordinates, ["kafka_partition", "kafka_offset"], "inner")
            .groupBy("kafka_partition", "kafka_offset")
            .agg(F.countDistinct("kafka_topic").alias("topic_count"))
            .filter(F.col("topic_count") > 1)
        )
        null_topic_stale = stale.filter(F.col("kafka_topic").isNull()).limit(1).count() > 0
        if null_topic_stale and ambiguous.limit(1).count() > 0:
            raise RuntimeError(
                "Legacy rows have null kafka_topic and Bronze contains the same "
                "partition+offset in more than one topic. Refusing an ambiguous migration."
            )

        stale_keys = stale.select(
            F.col("event_key").alias("legacy_event_key"),
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
        )
        join_condition = (
            (stale_keys.kafka_partition == bronze.kafka_partition)
            & (stale_keys.kafka_offset == bronze.kafka_offset)
            & (
                stale_keys.kafka_topic.eqNullSafe(bronze.kafka_topic)
                | stale_keys.kafka_topic.isNull()
            )
        )
        mapping = (
            stale_keys.join(bronze, join_condition, "inner")
            .select(
                "legacy_event_key",
                bronze.event_id.alias("event_id"),
                bronze.kafka_topic.alias("kafka_topic"),
                bronze.kafka_partition.alias("kafka_partition"),
                bronze.kafka_offset.alias("kafka_offset"),
            )
            .dropDuplicates(["legacy_event_key"])
        )
        mapping = (
            mapping
            .withColumn("new_event_key", event_key_expression(mapping))
            .withColumn(
                "new_event_identity_source",
                event_identity_source_expression(mapping),
            )
        )
        mappable = mapping.count()
        if mappable != stale_count:
            raise RuntimeError(
                f"Only {mappable} of {stale_count} legacy rows can be mapped. "
                "No changes were written."
            )

        duplicate_new_keys = (
            mapping.groupBy("new_event_key")
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )
        if duplicate_new_keys:
            raise RuntimeError(
                "More than one legacy row maps to the same new event key. "
                "No changes were written."
            )

        existing_new_keys = (
            target_df.filter(~F.col("event_key").rlike(LEGACY_EVENT_KEY_PATTERN))
            .select(F.col("event_key").alias("existing_event_key"))
            .filter(F.col("existing_event_key").isNotNull())
            .distinct()
        )
        collision = (
            mapping.join(
                existing_new_keys,
                mapping.new_event_key == existing_new_keys.existing_event_key,
                "inner",
            )
            .limit(1)
            .count()
        )
        if collision:
            raise RuntimeError(
                "A generated event key already exists on a non-legacy target row. "
                "No changes were written."
            )

        if dry_run:
            mapping.show(20, truncate=False)
            return 0

        mapping.createOrReplaceTempView("_behavioral_key_migration")
        spark.sql(
            f"""
            MERGE INTO {target} AS target
            USING _behavioral_key_migration AS source
            ON target.event_key = source.legacy_event_key
            WHEN MATCHED AND target.event_key RLIKE '{LEGACY_EVENT_KEY_PATTERN}'
              THEN UPDATE SET
                target.event_key = source.new_event_key,
                target.event_id = source.event_id,
                target.event_identity_source = source.new_event_identity_source,
                target.kafka_topic = source.kafka_topic
            """
        )

        migrated = spark.table(target)
        remaining = migrated.filter(F.col("event_key").rlike(LEGACY_EVENT_KEY_PATTERN)).count()
        null_keys = migrated.filter(F.col("event_key").isNull()).count()
        duplicate_keys = (
            migrated.groupBy("event_key")
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )
        if remaining or null_keys or duplicate_keys:
            raise RuntimeError(
                "Post-migration verification failed: "
                f"legacy={remaining}, null={null_keys}, duplicate_groups={duplicate_keys}."
            )
        logger.info("Behavioral event-key migration completed successfully.")
        return 0
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate legacy Behavioral event keys")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--target-table", default=None)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    sys.exit(run_migration(args.dry_run, args.target_table))
