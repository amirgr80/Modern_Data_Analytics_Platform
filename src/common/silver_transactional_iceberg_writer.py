from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Mapping, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from common.silver_transactional_kimball import KimballTables


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IcebergTableSpec:
    """Physical Iceberg table configuration."""

    table_name: str
    merge_keys: tuple[str, ...]
    partition_sql: str | None = None


DEFAULT_TABLE_SPECS: Mapping[str, IcebergTableSpec] = {
    "dim_date": IcebergTableSpec(
        table_name="dim_date",
        merge_keys=("date_key",),
    ),
    "dim_user": IcebergTableSpec(
        table_name="dim_user",
        merge_keys=("user_id",),
    ),
    "dim_category": IcebergTableSpec(
        table_name="dim_category",
        merge_keys=("category_id",),
    ),
    "dim_product": IcebergTableSpec(
        table_name="dim_product",
        merge_keys=("product_id",),
    ),
    "dim_product_price_scd": IcebergTableSpec(
        table_name="dim_product_price_scd",
        merge_keys=("price_history_id",),
    ),
    "fact_order": IcebergTableSpec(
        table_name="fact_order",
        merge_keys=("order_id",),
        partition_sql="PARTITIONED BY (months(order_timestamp))",
    ),
    "fact_order_item": IcebergTableSpec(
        table_name="fact_order_item",
        merge_keys=("order_item_id",),
        partition_sql="PARTITIONED BY (bucket(16, order_id))",
    ),
}


class SilverTransactionalIcebergWriter:
    """
    Creates and incrementally upserts Silver Kimball tables in Iceberg.

    Responsibilities:
      1. Create the target namespace if it does not exist.
      2. Create each Iceberg table from the incoming DataFrame schema.
      3. MERGE incoming rows by the configured business key.
      4. Preserve silver_created_at for existing rows.
      5. Update silver_updated_at and all mutable business columns.

    The writer expects a SparkSession already configured with the Lakekeeper
    Iceberg REST catalog.
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "lakekeeper",
        namespace: str = "transactional",
        table_specs: Mapping[str, IcebergTableSpec] | None = None,
    ) -> None:
        self.spark = spark
        self.catalog = catalog
        self.namespace = namespace
        self.table_specs = dict(table_specs or DEFAULT_TABLE_SPECS)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write_all(self, tables: KimballTables) -> None:
        """Write all Kimball tables in dependency order."""
        self.ensure_namespace()

        ordered_tables = (
            ("dim_date", tables.dim_date),
            ("dim_user", tables.dim_user),
            ("dim_category", tables.dim_category),
            ("dim_product", tables.dim_product),
            ("dim_product_price_scd", tables.dim_product_price_scd),
            ("fact_order", tables.fact_order),
            ("fact_order_item", tables.fact_order_item),
        )

        for logical_name, dataframe in ordered_tables:
            self.write_table(logical_name, dataframe)

    def write_table(self, logical_name: str, dataframe: DataFrame) -> None:
        """
        Create the target table when missing, then MERGE the DataFrame into it.
        """
        if logical_name not in self.table_specs:
            raise ValueError(
                f"Unknown Silver table '{logical_name}'. "
                f"Supported tables: {sorted(self.table_specs)}"
            )

        spec = self.table_specs[logical_name]
        self._validate_dataframe(dataframe, spec)

        full_table_name = self._full_table_name(spec.table_name)
        self.ensure_table(dataframe, spec)

        if self._is_dataframe_empty(dataframe):
            logger.info("Skipping empty DataFrame for %s.", full_table_name)
            return

        self._merge_dataframe(
            dataframe=dataframe,
            full_table_name=full_table_name,
            merge_keys=spec.merge_keys,
        )

        logger.info("Successfully merged data into %s.", full_table_name)

    def ensure_namespace(self) -> None:
        self.spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS "
            f"{self._quote(self.catalog)}.{self._quote(self.namespace)}"
        )

    def ensure_table(
        self,
        dataframe: DataFrame,
        spec: IcebergTableSpec,
    ) -> None:
        """
        Create an Iceberg table using the exact incoming Spark schema.

        Schema evolution is intentionally not automatic here. A schema mismatch
        should be reviewed instead of silently changing a production table.
        """
        full_table_name = self._full_table_name(spec.table_name)

        if self._table_exists(full_table_name):
            self._validate_target_schema(dataframe, full_table_name)
            return

        columns_ddl = ",\n    ".join(
            f"{self._quote(field.name)} {field.dataType.simpleString()}"
            + ("" if field.nullable else " NOT NULL")
            for field in dataframe.schema.fields
        )

        partition_clause = (
            f"\n{spec.partition_sql}" if spec.partition_sql else ""
        )

        create_sql = f"""
CREATE TABLE IF NOT EXISTS {full_table_name} (
    {columns_ddl}
)
USING iceberg
{partition_clause}
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.distribution-mode' = 'hash'
)
""".strip()

        logger.info("Creating Iceberg table %s.", full_table_name)
        self.spark.sql(create_sql)

    # ------------------------------------------------------------------
    # Merge implementation
    # ------------------------------------------------------------------

    def _merge_dataframe(
        self,
        dataframe: DataFrame,
        full_table_name: str,
        merge_keys: Sequence[str],
    ) -> None:
        # Iceberg validates MERGE source determinism before Spark can
        # prune unused non-deterministic Bronze lineage. Materializing
        # here creates a deterministic LogicalRDD boundary.
        materialized_source = dataframe.localCheckpoint(
            eager=True
        )

        source_view = (
            f"_silver_source_{uuid.uuid4().hex}"
        )

        try:
            analyzed_plan = (
                materialized_source
                ._jdf
                .queryExecution()
                .analyzed()
            )

            if not bool(
                analyzed_plan.deterministic()
            ):
                raise RuntimeError(
                    "Materialized MERGE source for "
                    f"{full_table_name} is still "
                    "non-deterministic."
                )

            materialized_source.createOrReplaceTempView(
                source_view
            )

            logger.info(
                "Materialized deterministic MERGE source "
                "for %s.",
                full_table_name,
            )
            merge_condition = " AND ".join(
                f"target.{self._quote(key)} = source.{self._quote(key)}"
                for key in merge_keys
            )

            update_columns = [
                column
                for column in materialized_source.columns
                if column not in set(merge_keys)
                and column != "silver_created_at"
            ]

            update_assignments = ",\n        ".join(
                f"target.{self._quote(column)} = source.{self._quote(column)}"
                for column in update_columns
            )

            insert_columns = ", ".join(
                self._quote(column) for column in materialized_source.columns
            )
            insert_values = ", ".join(
                f"source.{self._quote(column)}" for column in materialized_source.columns
            )

            matched_clause = ""
            if update_assignments:
                matched_clause = f"""
WHEN MATCHED THEN UPDATE SET
        {update_assignments}
"""

            merge_sql = f"""
MERGE INTO {full_table_name} AS target
USING {self._quote(source_view)} AS source
ON {merge_condition}
{matched_clause}
WHEN NOT MATCHED THEN INSERT (
    {insert_columns}
)
VALUES (
    {insert_values}
)
""".strip()

            self.spark.sql(merge_sql)
        finally:
            self.spark.catalog.dropTempView(
                source_view
            )

            materialized_source.unpersist(
                blocking=False
            )

    # ------------------------------------------------------------------
    # Validation and helpers
    # ------------------------------------------------------------------

    def _validate_dataframe(
        self,
        dataframe: DataFrame,
        spec: IcebergTableSpec,
    ) -> None:
        if not dataframe.columns:
            raise ValueError(
                f"DataFrame for '{spec.table_name}' has no columns."
            )

        missing_keys = sorted(set(spec.merge_keys) - set(dataframe.columns))
        if missing_keys:
            raise ValueError(
                f"DataFrame for '{spec.table_name}' is missing merge keys "
                f"{missing_keys}. Available columns: {sorted(dataframe.columns)}"
            )

        null_key_condition = None
        for key in spec.merge_keys:
            current_condition = dataframe[key].isNull()
            null_key_condition = (
                current_condition
                if null_key_condition is None
                else null_key_condition | current_condition
            )

        if null_key_condition is not None and dataframe.where(
            null_key_condition
        ).limit(1).count():
            raise ValueError(
                f"DataFrame for '{spec.table_name}' contains NULL values in "
                f"merge key(s): {list(spec.merge_keys)}. These rows must be "
                "rejected during validation before writing to Iceberg."
            )

        duplicate_condition = (
            dataframe.groupBy(*spec.merge_keys)
            .count()
            .where("count > 1")
            .limit(1)
            .count()
        )
        if duplicate_condition:
            raise ValueError(
                f"DataFrame for '{spec.table_name}' contains duplicate merge "
                f"keys: {list(spec.merge_keys)}."
            )

    def _validate_target_schema(
        self,
        dataframe: DataFrame,
        full_table_name: str,
    ) -> None:
        target_schema = self.spark.table(full_table_name).schema

        source_fields = {
            field.name: field.dataType.simpleString()
            for field in dataframe.schema.fields
        }
        target_fields = {
            field.name: field.dataType.simpleString()
            for field in target_schema.fields
        }

        missing_in_target = sorted(set(source_fields) - set(target_fields))
        missing_in_source = sorted(set(target_fields) - set(source_fields))
        incompatible_types = sorted(
            column
            for column in set(source_fields) & set(target_fields)
            if source_fields[column] != target_fields[column]
        )

        if missing_in_target or missing_in_source or incompatible_types:
            details: list[str] = []

            if missing_in_target:
                details.append(
                    f"columns missing in target: {missing_in_target}"
                )
            if missing_in_source:
                details.append(
                    f"columns missing in source: {missing_in_source}"
                )
            if incompatible_types:
                type_details = {
                    column: {
                        "source": source_fields[column],
                        "target": target_fields[column],
                    }
                    for column in incompatible_types
                }
                details.append(f"incompatible types: {type_details}")

            raise ValueError(
                f"Schema mismatch for {full_table_name}: "
                + "; ".join(details)
            )

    def _table_exists(self, full_table_name: str) -> bool:
        try:
            self.spark.table(full_table_name).limit(0)
            return True
        except AnalysisException:
            return False

    @staticmethod
    def _is_dataframe_empty(dataframe: DataFrame) -> bool:
        return not dataframe.take(1)

    def _full_table_name(self, table_name: str) -> str:
        return (
            f"{self._quote(self.catalog)}."
            f"{self._quote(self.namespace)}."
            f"{self._quote(table_name)}"
        )

    @staticmethod
    def _quote(identifier: str) -> str:
        return f"`{identifier.replace('`', '``')}`"