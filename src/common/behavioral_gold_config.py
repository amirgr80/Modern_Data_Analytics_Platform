"""Configuration for the Behavioral Gold ClickHouse load.

This module is the single source of truth for two things:

1. **Runtime settings** for the Behavioral Silver → ClickHouse Gold job — where
   to find the Silver Iceberg tables (via the sibling
   :class:`BehavioralRuntimeConfig`) and how to reach ClickHouse.
2. **The canonical column list** (:data:`GOLD_BEHAVIORAL_COLUMNS`) that every
   other Gold Behavioral component must agree on: the DDL
   (``sql/clickhouse/001_behavioral_gold_obt.sql``), the transform
   (:mod:`common.behavioral_gold_transform`), the contract check inside
   :mod:`jobs.gold_behavioral_job`, and the writer's ``column_names`` argument
   in :mod:`common.behavioral_gold_clickhouse`.

Keeping this list in one place is what makes the schema contract enforceable:
if any component drifts, the ``_assert_gold_contract`` step at job start-up
fails loudly instead of silently producing a wrong OBT.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping, Optional, Tuple

from common.silver_behavioral_config import BehavioralRuntimeConfig



GOLD_BEHAVIORAL_TABLE = "behavioral_obt"
"""Default ClickHouse table name inside the target database.

Overridable via the ``GOLD_BEHAVIORAL_TABLE`` environment variable. The value
here must match the table created by
``sql/clickhouse/001_behavioral_gold_obt.sql`` and by the embedded DDL in
``behavioral_gold_clickhouse.CREATE_BEHAVIORAL_GOLD_TABLE_SQL``.
"""

ENV_CLICKHOUSE_HOST = "CLICKHOUSE_HOST"
ENV_CLICKHOUSE_HTTP_PORT = "CLICKHOUSE_HTTP_PORT"
ENV_CLICKHOUSE_DB = "CLICKHOUSE_DB"
ENV_CLICKHOUSE_USER = "CLICKHOUSE_USER"
ENV_CLICKHOUSE_PASSWORD = "CLICKHOUSE_PASSWORD"  # noqa: S105 — name, not a secret
ENV_GOLD_BEHAVIORAL_TABLE = "GOLD_BEHAVIORAL_TABLE"

DEFAULT_CLICKHOUSE_HOST = "clickhouse"
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123
DEFAULT_CLICKHOUSE_DB = "lakehouse"
DEFAULT_CLICKHOUSE_USER = "default"




def _get(
    env: Mapping[str, str],
    name: str,
    default: Optional[str] = None,
) -> Optional[str]:
    """Read a string env var, treating whitespace-only values as unset.

    Returns the value stripped of surrounding whitespace, or ``default`` if the
    variable is missing or contains only whitespace. Returning ``default`` for
    blank strings is deliberate: it prevents an accidentally-blank
    ``export CLICKHOUSE_HOST=`` from silently overriding a good default.
    """

    value = env.get(name, default)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or default


def _get_int(env: Mapping[str, str], name: str, default: int) -> int:
    """Read a positive integer env var, or return ``default`` if unset.

    Raises ``ValueError`` with a message that names the offending env var if
    the value is not an integer or is not strictly positive. Failing fast at
    configuration load time gives a much clearer error than a downstream
    connection failure with a garbage port number.
    """

    raw = _get(env, name, str(default))
    try:
        value = int(raw) if raw is not None else default
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}.") from exc
    if value < 1:
        raise ValueError(f"{name} must be positive, got {value}.")
    return value


def _get_password(env: Mapping[str, str], name: str) -> str:
    """Read a password env var without stripping (passwords may have spaces).

    Unlike :func:`_get`, we preserve whatever the operator set. Missing
    variables map to the empty string, which matches ClickHouse's default
    ``default`` account behavior on a fresh install.
    """

    return env.get(name, "") or ""




@dataclass(frozen=True)
class GoldBehavioralConfig:
    """Immutable configuration for one Gold Behavioral job invocation.

    Instances are typically built via :meth:`from_env` at the top of
    :func:`jobs.gold_behavioral_job.run_gold_behavioral_job`. The
    ``frozen=True`` guarantee means downstream code (transform + writer) can
    safely capture the instance without worrying about mid-run mutation.

    Attributes:
        silver: Fully-resolved Silver Behavioral runtime config. Provides the
            Iceberg REST URI, catalog name, warehouse, and namespace used to
            resolve fully-qualified table names via :meth:`qualified_table`.
        clickhouse_host: Hostname reachable from the Spark driver /
            Airflow worker. Inside the docker-compose stack this is the
            service name ``clickhouse``.
        clickhouse_port: HTTP interface port (``8123`` by default). The
            native protocol on 9000 is not used by ``clickhouse_connect``.
        clickhouse_database: Target database (``lakehouse``). Auto-created
            by the writer if it does not exist.
        clickhouse_user: Auth user; ``default`` on a fresh install.
        clickhouse_password: Auth password; empty string by default.
        clickhouse_table: Target table name inside ``clickhouse_database``.
    """

    silver: BehavioralRuntimeConfig
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_database: str
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_table: str


    def __post_init__(self) -> None:
        """Reject obviously-bogus config at construction time.

        We validate the ClickHouse settings here rather than in the writer
        so misconfiguration surfaces at DAG parse time (or the first job
        line) instead of after a Spark session has already spun up.
        """

        if not self.clickhouse_host:
            raise ValueError("clickhouse_host must be a non-empty string.")
        if self.clickhouse_port < 1:
            raise ValueError(
                f"clickhouse_port must be positive, got {self.clickhouse_port}."
            )
        if not self.clickhouse_database:
            raise ValueError("clickhouse_database must be a non-empty string.")
        if not self.clickhouse_user:
            raise ValueError("clickhouse_user must be a non-empty string.")
        if not self.clickhouse_table:
            raise ValueError("clickhouse_table must be a non-empty string.")


    @property
    def qualified_clickhouse_table(self) -> str:
        """Fully-qualified target table, e.g. ``lakehouse.behavioral_obt``."""
        return f"{self.clickhouse_database}.{self.clickhouse_table}"


    @property
    def fact_table(self) -> str:
        """Fully-qualified path of the Silver fact table."""
        return self.silver.qualified_table("fact_behavioral_events")

    @property
    def device_table(self) -> str:
        """Fully-qualified path of the Silver device dimension."""
        return self.silver.qualified_table("dim_behavioral_device")

    @property
    def event_type_table(self) -> str:
        """Fully-qualified path of the Silver event-type dimension."""
        return self.silver.qualified_table("dim_behavioral_event_type")

    @property
    def session_table(self) -> str:
        """Fully-qualified path of the Silver session dimension."""
        return self.silver.qualified_table("dim_behavioral_session")


    @property
    def target_columns(self) -> Tuple[str, ...]:
        """Canonical Gold column list, in exactly the order the DDL expects.

        Used by:

        - :func:`common.behavioral_gold_transform.build_behavioral_gold_obt`
          to project the final DataFrame.
        - :func:`jobs.gold_behavioral_job._assert_gold_contract` to compare
          against ``df.columns``.
        - :func:`common.behavioral_gold_clickhouse.replace_behavioral_gold_partition`
          to pass as ``column_names`` to ``clickhouse_connect.insert_df``.
        """
        return GOLD_BEHAVIORAL_COLUMNS


    def describe(self) -> str:
        """Return a single-line, secret-free summary suitable for logs."""
        return (
            "GoldBehavioralConfig("
            f"clickhouse={self.clickhouse_user}@{self.clickhouse_host}:"
            f"{self.clickhouse_port}/{self.clickhouse_database}, "
            f"target={self.clickhouse_table}, "
            f"fact={self.fact_table}"
            ")"
        )


    @classmethod
    def from_env(
        cls,
        env: Optional[Mapping[str, str]] = None,
    ) -> "GoldBehavioralConfig":
        """Build a :class:`GoldBehavioralConfig` from environment variables.

        Args:
            env: Optional mapping to read from. Defaults to :data:`os.environ`.
                Passing an explicit mapping is useful in tests so we do not
                have to mutate the real process environment.

        Reads:
            - ``CLICKHOUSE_HOST`` (default ``clickhouse``)
            - ``CLICKHOUSE_HTTP_PORT`` (default ``8123``)
            - ``CLICKHOUSE_DB`` (default ``lakehouse``)
            - ``CLICKHOUSE_USER`` (default ``default``)
            - ``CLICKHOUSE_PASSWORD`` (default empty)
            - ``GOLD_BEHAVIORAL_TABLE`` (default :data:`GOLD_BEHAVIORAL_TABLE`)
            - plus everything read by :meth:`BehavioralRuntimeConfig.from_env`
              (``BEHAVIORAL_ICEBERG_REST_URI``, ``BEHAVIORAL_ICEBERG_WAREHOUSE``,
              ``BEHAVIORAL_ICEBERG_NAMESPACE`` etc.).

        Raises:
            ValueError: If any int-valued env var is not a positive integer,
                or if a required field ends up blank after stripping.
        """

        source = os.environ if env is None else env
        silver_config = BehavioralRuntimeConfig.from_env(source)

        return cls(
            silver=silver_config,
            clickhouse_host=(
                _get(source, ENV_CLICKHOUSE_HOST, DEFAULT_CLICKHOUSE_HOST)
                or DEFAULT_CLICKHOUSE_HOST
            ),
            clickhouse_port=_get_int(
                source, ENV_CLICKHOUSE_HTTP_PORT, DEFAULT_CLICKHOUSE_HTTP_PORT
            ),
            clickhouse_database=(
                _get(source, ENV_CLICKHOUSE_DB, DEFAULT_CLICKHOUSE_DB)
                or DEFAULT_CLICKHOUSE_DB
            ),
            clickhouse_user=(
                _get(source, ENV_CLICKHOUSE_USER, DEFAULT_CLICKHOUSE_USER)
                or DEFAULT_CLICKHOUSE_USER
            ),
            clickhouse_password=_get_password(source, ENV_CLICKHOUSE_PASSWORD),
            clickhouse_table=(
                _get(source, ENV_GOLD_BEHAVIORAL_TABLE, GOLD_BEHAVIORAL_TABLE)
                or GOLD_BEHAVIORAL_TABLE
            ),
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


__all__ = [
    "GOLD_BEHAVIORAL_TABLE",
    "GOLD_BEHAVIORAL_COLUMNS",
    "GoldBehavioralConfig",
]
