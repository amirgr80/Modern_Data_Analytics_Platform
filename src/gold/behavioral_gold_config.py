"""Configuration for the Behavioral Gold ClickHouse load."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import os
from typing import Mapping, Optional, Tuple

from common.silver_behavioral_config import BehavioralRuntimeConfig


GOLD_BEHAVIORAL_TABLE = "behavioral_obt"


def _get(env: Mapping[str, str], name: str, default: Optional[str] = None) -> Optional[str]:
    value = env.get(name, default)
    if value is None:
        return None
    value = value.strip()
    return value or default


def _get_int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = _get(env, name, str(default))
    try:
        value = int(raw) if raw is not None else default
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}.") from exc
    if value < 1:
        raise ValueError(f"{name} must be positive, got {value}.")
    return value


@dataclass(frozen=True)
class GoldBehavioralConfig:
    silver: BehavioralRuntimeConfig
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_database: str
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_table: str
    insert_batch_size: int

    @property
    def qualified_clickhouse_table(self) -> str:
        return f"{self.clickhouse_database}.{self.clickhouse_table}"

    @property
    def fact_table(self) -> str:
        return self.silver.qualified_table("fact_behavioral_events")

    @property
    def device_table(self) -> str:
        return self.silver.qualified_table("dim_behavioral_device")

    @property
    def event_type_table(self) -> str:
        return self.silver.qualified_table("dim_behavioral_event_type")

    @property
    def session_table(self) -> str:
        return self.silver.qualified_table("dim_behavioral_session")

    @property
    def target_columns(self) -> Tuple[str, ...]:
        return GOLD_BEHAVIORAL_COLUMNS

    @classmethod
    def from_env(
        cls,
        env: Optional[Mapping[str, str]] = None,
    ) -> "GoldBehavioralConfig":
        source = os.environ if env is None else env
        silver_config = BehavioralRuntimeConfig.from_env(source)
        clickhouse_user = _get(source, "CLICKHOUSE_USER", "default")
        clickhouse_password = _get(source, "CLICKHOUSE_PASSWORD", "")
        if clickhouse_user is None:
            raise RuntimeError("CLICKHOUSE_USER is required for Behavioral Gold.")
        if clickhouse_password is None:
            raise RuntimeError("CLICKHOUSE_PASSWORD is required for Behavioral Gold.")
        return cls(
            silver=silver_config,
            clickhouse_host=_get(source, "CLICKHOUSE_HOST", "clickhouse") or "clickhouse",
            clickhouse_port=_get_int(source, "CLICKHOUSE_HTTP_PORT", 8123),
            clickhouse_database=_get(source, "CLICKHOUSE_DB", "lakehouse") or "lakehouse",
            clickhouse_user=clickhouse_user,
            clickhouse_password=clickhouse_password,
            clickhouse_table=(
                _get(source, "GOLD_BEHAVIORAL_TABLE", GOLD_BEHAVIORAL_TABLE)
                or GOLD_BEHAVIORAL_TABLE
            ),
            insert_batch_size=_get_int(source, "GOLD_CLICKHOUSE_INSERT_BATCH_SIZE", 10000),
        )


GOLD_BEHAVIORAL_COLUMNS: Tuple[str, ...] = (
    "event_key",
    "event_id",
    "event_identity_source",
    "event_timestamp",
    "date_key",
    "processing_date",
    "user_key",
    "user_id",
    "session_key",
    "session_id",
    "session_start_at",
    "session_end_at",
    "session_duration_sec",
    "session_event_count",
    "device_key",
    "device_name",
    "primary_device_key",
    "event_type_key",
    "event_type",
    "event_category",
    "utm_source",
    "ip_address_hash",
    "product_id",
    "order_id",
    "url_path",
    "query",
    "wishlist_name",
    "payment_type",
    "shipping_method",
    "fulfillment_speed",
    "error_code",
    "success",
    "http_status",
    "quantity",
    "cart_total_items",
    "cart_value",
    "duration_sec",
    "results_count",
    "clicked_position",
    "rating",
    "text_length",
    "cart_items_json",
    "dq_flags_json",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "bronze_ingestion_timestamp",
    "source_file",
    "pipeline_run_id",
    "silver_ingestion_timestamp",
    "gold_loaded_at",
)


def execution_date_filter(execution_date: date) -> str:
    return execution_date.isoformat()
