"""Iceberg DDL and table ownership for the isolated Behavioral namespace."""

from __future__ import annotations

import logging
from typing import Mapping, Sequence

from pyspark.sql import SparkSession

from common.silver_behavioral_config import BehavioralRuntimeConfig
from common.silver_behavioral_iceberg_writer import ensure_columns


logger = logging.getLogger(__name__)

TABLE_DIM_DEVICE = "dim_behavioral_device"
TABLE_DIM_EVENT_TYPE = "dim_behavioral_event_type"
TABLE_DIM_SESSION = "dim_behavioral_session"
TABLE_FACT_EVENTS = "fact_behavioral_events"
TABLE_QUARANTINE = "behavioral_events_quarantine"
TABLE_PIPELINE_STATE = "behavioral_pipeline_state"
TABLE_QUALITY = "behavioral_validation_issues"

BEHAVIORAL_TABLES: Sequence[str] = (
    TABLE_DIM_DEVICE,
    TABLE_DIM_EVENT_TYPE,
    TABLE_DIM_SESSION,
    TABLE_FACT_EVENTS,
    TABLE_QUARANTINE,
    TABLE_PIPELINE_STATE,
)

TABLE_DDL: Mapping[str, str] = {
    TABLE_DIM_DEVICE: """
        CREATE TABLE IF NOT EXISTS {table} (
            device_key STRING,
            device_name STRING,
            first_seen_at TIMESTAMP,
            last_seen_at TIMESTAMP,
            silver_updated_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2')
    """,
    TABLE_DIM_EVENT_TYPE: """
        CREATE TABLE IF NOT EXISTS {table} (
            event_type_key STRING,
            event_type STRING,
            event_category STRING,
            first_seen_at TIMESTAMP,
            last_seen_at TIMESTAMP,
            silver_updated_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2')
    """,
    TABLE_DIM_SESSION: """
        CREATE TABLE IF NOT EXISTS {table} (
            session_key STRING,
            session_id STRING,
            user_key STRING,
            user_id STRING,
            session_start_at TIMESTAMP,
            session_end_at TIMESTAMP,
            session_duration_sec BIGINT,
            primary_device_key STRING,
            event_count BIGINT,
            silver_updated_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2')
    """,
    TABLE_FACT_EVENTS: """
        CREATE TABLE IF NOT EXISTS {table} (
            event_key STRING,
            event_id STRING,
            event_identity_source STRING,
            date_key INT,
            user_key STRING,
            user_id STRING,
            session_key STRING,
            session_id STRING,
            device_key STRING,
            event_type_key STRING,
            event_type STRING,
            event_timestamp TIMESTAMP,
            utm_source STRING,
            ip_address_hash STRING,
            product_id STRING,
            order_id STRING,
            url_path STRING,
            query STRING,
            wishlist_name STRING,
            payment_type STRING,
            shipping_method STRING,
            fulfillment_speed STRING,
            error_code STRING,
            success BOOLEAN,
            http_status INT,
            quantity INT,
            cart_total_items INT,
            cart_value DOUBLE,
            duration_sec INT,
            results_count INT,
            clicked_position INT,
            rating INT,
            text_length INT,
            cart_items ARRAY<STRUCT<product_id: STRING, price: DOUBLE, quantity: INT>>,
            dq_flags ARRAY<STRING>,
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            bronze_ingestion_timestamp TIMESTAMP,
            source_file STRING,
            processing_date DATE,
            pipeline_run_id STRING,
            silver_ingestion_timestamp TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(event_timestamp))
        TBLPROPERTIES (
            'format-version'='2',
            'write.distribution-mode'='hash',
            'write.target-file-size-bytes'='134217728'
        )
    """,
    TABLE_QUARANTINE: """
        CREATE TABLE IF NOT EXISTS {table} (
            event_key STRING,
            event_id STRING,
            event_identity_source STRING,
            validation_errors ARRAY<STRING>,
            validation_warnings ARRAY<STRING>,
            raw_user_id STRING,
            raw_session_id STRING,
            raw_event_type STRING,
            raw_timestamp STRING,
            raw_device STRING,
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            bronze_ingestion_timestamp TIMESTAMP,
            source_file STRING,
            processing_date DATE,
            pipeline_run_id STRING,
            first_quarantined_at TIMESTAMP,
            last_quarantined_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (processing_date)
        TBLPROPERTIES ('format-version'='2')
    """,
    TABLE_PIPELINE_STATE: """
        CREATE TABLE IF NOT EXISTS {table} (
            run_key STRING,
            pipeline_name STRING,
            execution_date DATE,
            status STRING,
            first_started_at TIMESTAMP,
            last_started_at TIMESTAMP,
            completed_at TIMESTAMP,
            raw_count BIGINT,
            valid_count BIGINT,
            warning_count BIGINT,
            processable_count BIGINT,
            rejected_count BIGINT,
            fact_rows_merged BIGINT,
            error_message STRING,
            updated_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (months(execution_date))
        TBLPROPERTIES ('format-version'='2')
    """,
}

QUALITY_DDL = """
    CREATE TABLE IF NOT EXISTS {table} (
        quality_issue_key STRING,
        source_table STRING,
        record_key STRING,
        issue_status STRING,
        issue_code STRING,
        validation_errors ARRAY<STRING>,
        validation_warnings ARRAY<STRING>,
        original_record STRING,
        source_file STRING,
        kafka_topic STRING,
        kafka_partition INT,
        kafka_offset BIGINT,
        kafka_timestamp TIMESTAMP,
        bronze_ingestion_timestamp TIMESTAMP,
        processing_date DATE,
        pipeline_run_id STRING,
        first_detected_at TIMESTAMP,
        last_detected_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (processing_date)
    TBLPROPERTIES ('format-version'='2')
"""

# Safe, additive-only evolution for tables that may already exist from an
# earlier Behavioral deployment. Type changes, renames, and drops remain
# explicit migrations rather than silent runtime actions.
SAFE_ADDITIVE_COLUMNS: Mapping[str, Mapping[str, str]] = {
    TABLE_FACT_EVENTS: {
        "utm_source": "STRING",
        "ip_address_hash": "STRING",
    },
    TABLE_PIPELINE_STATE: {
        "processable_count": "BIGINT",
    },
}

QUALITY_SAFE_ADDITIVE_COLUMNS: Mapping[str, str] = {
    "issue_code": "STRING",
}


def ensure_behavioral_tables(
    spark: SparkSession,
    config: BehavioralRuntimeConfig,
) -> None:
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {config.catalog_name}.{config.namespace}"
    )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {config.catalog_name}.{config.quality_namespace}"
    )

    for table_name, ddl in TABLE_DDL.items():
        qualified = config.qualified_table(table_name)
        spark.sql(ddl.format(table=qualified))
        added = ensure_columns(
            spark,
            qualified,
            dict(SAFE_ADDITIVE_COLUMNS.get(table_name, {})),
        )
        if added:
            logger.info("Added safe columns to %s: %s", qualified, list(added))

    quality_table = config.qualified_table(TABLE_QUALITY, config.quality_namespace)
    spark.sql(QUALITY_DDL.format(table=quality_table))
    added = ensure_columns(
        spark,
        quality_table,
        dict(QUALITY_SAFE_ADDITIVE_COLUMNS),
    )
    if added:
        logger.info("Added safe columns to %s: %s", quality_table, list(added))
