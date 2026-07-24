"""Runtime configuration for the independent Silver Behavioral pipeline.

This module intentionally has no PySpark or Airflow imports.  The DAG, jobs,
and tests can all import the same package/version and environment contract
without starting a JVM.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
import re
from typing import Mapping, Optional, Tuple


SPARK_VERSION = "3.5.3"
SCALA_BINARY_VERSION = "2.12"
ICEBERG_VERSION = "1.6.1"
HADOOP_AWS_VERSION = "3.3.4"


_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, value: str) -> None:
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(
            f"{name} must be a simple Spark identifier, got {value!r}."
        )

DEFAULT_SPARK_PACKAGES: Tuple[str, ...] = (
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_{SCALA_BINARY_VERSION}:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
)


def _get(
    env: Mapping[str, str],
    name: str,
    default: Optional[str] = None,
) -> Optional[str]:
    value = env.get(name, default)
    if value is None:
        return None
    value = value.strip()
    return value or default


def _get_bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = _get(env, name)
    if raw is None:
        return default
    normalized = raw.lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Environment variable {name!r} must be a boolean, got {raw!r}.")


def resolve_spark_packages(env: Optional[Mapping[str, str]] = None) -> Tuple[str, ...]:
    """Return one canonical package set for Spark, Airflow and migrations."""

    source = os.environ if env is None else env
    override = _get(source, "BEHAVIORAL_SPARK_PACKAGES") or _get(source, "SPARK_PACKAGES")
    if not override:
        return DEFAULT_SPARK_PACKAGES

    packages = tuple(item.strip() for item in override.split(",") if item.strip())
    if not packages:
        raise ValueError("Configured Spark package list is empty.")
    return packages


def spark_packages_csv(env: Optional[Mapping[str, str]] = None) -> str:
    return ",".join(resolve_spark_packages(env))


def _resolve_minio_credential(
    env: Mapping[str, str],
    primary: str,
    fallback: str,
) -> str:
    value = _get(env, primary) or _get(env, fallback)
    if not value:
        raise RuntimeError(
            f"Missing MinIO credential. Configure {primary} or {fallback}."
        )
    return value


def _normalize_lakekeeper_warehouse(
    rest_uri: str,
    warehouse: str,
    env: Mapping[str, str],
) -> str:
    """Lakekeeper resolves a warehouse by name, not by an ``s3://`` URI.

    The repository's shared compose anchor currently supplies
    ``ICEBERG_WAREHOUSE=s3://warehouse`` while the Lakekeeper bootstrap creates
    a warehouse named ``warehouse``.  Behavioral uses a domain-prefixed
    override first and safely normalizes the shared value as a fallback.
    """

    if "lakekeeper" not in rest_uri.lower() or not warehouse.startswith("s3://"):
        return warehouse

    configured_name = _get(env, "ICEBERG_WAREHOUSE_NAME")
    if configured_name:
        return configured_name

    path = warehouse[len("s3://") :].strip("/")
    return path.split("/", 1)[0] or "warehouse"


@dataclass(frozen=True)
class BehavioralRuntimeConfig:
    catalog_name: str
    rest_uri: str
    warehouse: str
    namespace: str
    quality_namespace: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    aws_region: str
    spark_master: str
    spark_packages: Tuple[str, ...]
    bronze_path: str
    timezone: str
    shuffle_partitions: int
    enable_shared_user_lookup: bool
    shared_user_table: str

    @property
    def packages_csv(self) -> str:
        return ",".join(self.spark_packages)

    def qualified_table(self, table_name: str, namespace: Optional[str] = None) -> str:
        resolved_namespace = namespace or self.namespace
        return f"{self.catalog_name}.{resolved_namespace}.{table_name}"

    def validate(self) -> None:
        required = {
            "catalog_name": self.catalog_name,
            "rest_uri": self.rest_uri,
            "warehouse": self.warehouse,
            "namespace": self.namespace,
            "quality_namespace": self.quality_namespace,
            "minio_endpoint": self.minio_endpoint,
            "minio_access_key": self.minio_access_key,
            "minio_secret_key": self.minio_secret_key,
            "spark_master": self.spark_master,
            "bronze_path": self.bronze_path,
        }
        blanks = [name for name, value in required.items() if not value or not value.strip()]
        if blanks:
            raise RuntimeError(f"Blank Silver Behavioral configuration values: {blanks}")
        for name, value in (
            ("catalog_name", self.catalog_name),
            ("namespace", self.namespace),
            ("quality_namespace", self.quality_namespace),
        ):
            _validate_identifier(name, value)
        if self.shuffle_partitions < 1:
            raise ValueError("SPARK_SQL_SHUFFLE_PARTITIONS must be positive.")
        if not self.spark_packages:
            raise ValueError("At least one Spark package is required.")

    @classmethod
    def from_env(
        cls,
        env: Optional[Mapping[str, str]] = None,
    ) -> "BehavioralRuntimeConfig":
        source = os.environ if env is None else env

        catalog_name = _get(source, "BEHAVIORAL_ICEBERG_CATALOG_NAME") or _get(
            source, "ICEBERG_CATALOG_NAME", "lakekeeper"
        )
        rest_uri = _get(source, "BEHAVIORAL_ICEBERG_REST_URI") or _get(
            source, "ICEBERG_REST_URI", "http://lakekeeper:8181/catalog"
        )
        raw_warehouse = _get(source, "BEHAVIORAL_ICEBERG_WAREHOUSE") or _get(
            source, "ICEBERG_WAREHOUSE", "silver"
        )
        warehouse = _normalize_lakekeeper_warehouse(rest_uri, raw_warehouse, source)

        namespace = _get(
            source,
            "BEHAVIORAL_ICEBERG_NAMESPACE",
            "behavioral",
        )
        quality_namespace = _get(
            source,
            "BEHAVIORAL_QUALITY_NAMESPACE",
            "behavioral_quality",
        )

        minio_endpoint = _get(source, "MINIO_ENDPOINT", "http://minio:9000")
        access_key = _resolve_minio_credential(
            source, "MINIO_ACCESS_KEY", "MINIO_ROOT_USER"
        )
        secret_key = _resolve_minio_credential(
            source, "MINIO_SECRET_KEY", "MINIO_ROOT_PASSWORD"
        )

        shuffle_raw = _get(source, "SPARK_SQL_SHUFFLE_PARTITIONS", "8")
        try:
            shuffle_partitions = int(shuffle_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "SPARK_SQL_SHUFFLE_PARTITIONS must be an integer."
            ) from exc

        config = cls(
            catalog_name=catalog_name,
            rest_uri=rest_uri,
            warehouse=warehouse,
            namespace=namespace,
            quality_namespace=quality_namespace,
            minio_endpoint=minio_endpoint,
            minio_access_key=access_key,
            minio_secret_key=secret_key,
            aws_region=_get(source, "AWS_REGION", "us-east-1"),
            spark_master=_get(
                source,
                "BEHAVIORAL_SPARK_MASTER_URL",
                _get(source, "SPARK_MASTER_URL", "spark://spark-master:7077"),
            ),
            spark_packages=resolve_spark_packages(source),
            bronze_path=_get(
                source,
                "BEHAVIORAL_BRONZE_OUTPUT_PATH",
                "s3a://bronze/behavioral/events",
            ),
            timezone=_get(source, "SILVER_TIMEZONE", "Asia/Tehran"),
            shuffle_partitions=shuffle_partitions,
            enable_shared_user_lookup=_get_bool(
                source,
                "BEHAVIORAL_ENABLE_SHARED_USER_LOOKUP",
                False,
            ),
            shared_user_table=_get(
                source,
                "BEHAVIORAL_SHARED_USER_TABLE",
                f"{catalog_name}.silver.dim_user",
            ),
        )
        config.validate()
        return config
