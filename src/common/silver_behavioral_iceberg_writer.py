"""Explicit, table-safe Iceberg MERGE helpers for Silver Behavioral.

No wildcard column update is used.  Every update and insert column is rendered
explicitly, source keys are checked for uniqueness, and callers select the
merge behavior appropriate for the table.
"""

from __future__ import annotations

from enum import Enum
import logging
from typing import Dict, Iterable, Mapping, Optional, Sequence
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


logger = logging.getLogger(__name__)


class MergeStrategy(str, Enum):
    INSERT_ONLY = "insert_only"
    UPSERT_ALL = "upsert_all"
    UPSERT_PRESERVE_BOUNDS = "upsert_preserve_bounds"
    FACT_DEDUPLICATE_INSERT = "fact_deduplicate_insert"


def _quote(identifier: str) -> str:
    if not identifier or "`" in identifier:
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
    return f"`{identifier}`"


def _column_ref(alias: str, column: str) -> str:
    return f"{alias}.{_quote(column)}"


def _assert_keys(merge_keys: Sequence[str]) -> None:
    if not merge_keys:
        raise ValueError("At least one merge key is required.")
    if len(set(merge_keys)) != len(merge_keys):
        raise ValueError(f"Duplicate merge keys: {merge_keys}")


def align_to_target_schema(
    spark: SparkSession,
    target_table: str,
    source_df: DataFrame,
) -> DataFrame:
    target_df = spark.table(target_table)
    target_columns = target_df.columns
    source_columns = source_df.columns
    missing = [column for column in target_columns if column not in source_columns]
    extra = [column for column in source_columns if column not in target_columns]
    if missing or extra:
        raise ValueError(
            f"Source schema does not match {target_table}. "
            f"Missing={missing}; unexpected={extra}."
        )

    target_types = {
        field.name: field.dataType.simpleString().lower()
        for field in target_df.schema.fields
    }
    source_types = {
        field.name: field.dataType.simpleString().lower()
        for field in source_df.schema.fields
    }
    type_mismatches = {
        column: (source_types[column], target_types[column])
        for column in target_columns
        if source_types[column] != target_types[column]
    }
    if type_mismatches:
        raise ValueError(
            f"Source types do not match {target_table}: {type_mismatches}."
        )
    return source_df.select(*target_columns)


def assert_unique_source_keys(source_df: DataFrame, merge_keys: Sequence[str]) -> None:
    _assert_keys(merge_keys)
    null_condition = None
    for key in merge_keys:
        condition = F.col(key).isNull()
        null_condition = condition if null_condition is None else null_condition | condition
    if source_df.filter(null_condition).limit(1).count() > 0:
        raise ValueError(f"Null value found in merge keys {list(merge_keys)}.")

    duplicates = (
        source_df.groupBy(*merge_keys)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicates:
        raise ValueError(
            f"Source contains duplicate rows for merge keys {list(merge_keys)}."
        )


def _render_insert(columns: Sequence[str]) -> str:
    names = ", ".join(_quote(column) for column in columns)
    values = ", ".join(_column_ref("source", column) for column in columns)
    return f"INSERT ({names}) VALUES ({values})"


def _render_update_assignments(
    columns: Sequence[str],
    merge_keys: Sequence[str],
    protected_columns: Iterable[str],
    min_columns: Iterable[str],
    max_columns: Iterable[str],
    custom_updates: Optional[Mapping[str, str]],
) -> str:
    protected = set(protected_columns) | set(merge_keys)
    min_set = set(min_columns)
    max_set = set(max_columns)
    custom = dict(custom_updates or {})

    assignments = []
    for column in columns:
        if column in protected:
            continue
        target_ref = _column_ref("target", column)
        source_ref = _column_ref("source", column)
        if column in custom:
            expression = custom[column]
        elif column in min_set:
            expression = (
                f"CASE WHEN {target_ref} IS NULL THEN {source_ref} "
                f"WHEN {source_ref} IS NULL THEN {target_ref} "
                f"ELSE LEAST({target_ref}, {source_ref}) END"
            )
        elif column in max_set:
            expression = (
                f"CASE WHEN {target_ref} IS NULL THEN {source_ref} "
                f"WHEN {source_ref} IS NULL THEN {target_ref} "
                f"ELSE GREATEST({target_ref}, {source_ref}) END"
            )
        else:
            expression = source_ref
        assignments.append(f"{target_ref} = {expression}")
    return ",\n                    ".join(assignments)


def merge_dataframe(
    spark: SparkSession,
    target_table: str,
    source_df: DataFrame,
    merge_keys: Sequence[str],
    strategy: MergeStrategy,
    *,
    protected_columns: Iterable[str] = (),
    min_columns: Iterable[str] = (),
    max_columns: Iterable[str] = (),
    custom_updates: Optional[Mapping[str, str]] = None,
) -> int:
    """Merge one DataFrame into an Iceberg table and return source row count."""

    aligned_df = align_to_target_schema(spark, target_table, source_df)
    source_count = aligned_df.count()
    if source_count == 0:
        return 0

    assert_unique_source_keys(aligned_df, merge_keys)

    view_name = f"_behavioral_merge_{uuid4().hex}"
    aligned_df.createOrReplaceTempView(view_name)
    columns = aligned_df.columns
    on_clause = " AND ".join(
        f"{_column_ref('target', key)} = {_column_ref('source', key)}"
        for key in merge_keys
    )
    insert_clause = _render_insert(columns)

    if strategy in {
        MergeStrategy.INSERT_ONLY,
        MergeStrategy.FACT_DEDUPLICATE_INSERT,
    }:
        matched_clause = ""
    elif strategy in {
        MergeStrategy.UPSERT_ALL,
        MergeStrategy.UPSERT_PRESERVE_BOUNDS,
    }:
        assignments = _render_update_assignments(
            columns=columns,
            merge_keys=merge_keys,
            protected_columns=protected_columns,
            min_columns=min_columns if strategy == MergeStrategy.UPSERT_PRESERVE_BOUNDS else (),
            max_columns=max_columns if strategy == MergeStrategy.UPSERT_PRESERVE_BOUNDS else (),
            custom_updates=custom_updates,
        )
        if not assignments:
            matched_clause = ""
        else:
            matched_clause = f"WHEN MATCHED THEN UPDATE SET\n                    {assignments}"
    else:  # pragma: no cover - defensive for future enum values
        raise ValueError(f"Unsupported merge strategy: {strategy}")

    sql = f"""
        MERGE INTO {target_table} AS target
        USING {view_name} AS source
        ON {on_clause}
        {matched_clause}
        WHEN NOT MATCHED THEN {insert_clause}
    """
    logger.debug("Iceberg merge SQL for %s:\n%s", target_table, sql)
    try:
        spark.sql(sql)
    finally:
        spark.catalog.dropTempView(view_name)
    return source_count


def table_exists(spark: SparkSession, qualified_name: str) -> bool:
    try:
        spark.table(qualified_name).limit(0).collect()
        return True
    except Exception:  # Spark raises different catalog exceptions by version
        return False


def ensure_columns(
    spark: SparkSession,
    target_table: str,
    columns: Dict[str, str],
) -> Sequence[str]:
    existing = {column.lower() for column in spark.table(target_table).columns}
    missing = [
        (name, sql_type)
        for name, sql_type in columns.items()
        if name.lower() not in existing
    ]
    if not missing:
        return ()
    ddl = ", ".join(f"{_quote(name)} {sql_type}" for name, sql_type in missing)
    spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS ({ddl})")
    return tuple(name for name, _ in missing)
