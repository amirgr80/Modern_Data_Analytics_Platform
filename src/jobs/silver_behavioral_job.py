from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime

CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(CURRENT_FILE_DIR)

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


from common.iceberg_catalog import (
    create_iceberg_spark_session,
    ensure_silver_behavioral_tables,
    qualified_table,
)
from common.silver_behavioral_transform import (
    apply_silver_data_quality,
    build_dim_date_seed,
    build_dim_device_updates,
    build_dim_event_type_updates,
    build_fact_behavioral_events,
    build_quarantine_rows,
    deduplicate_events,
    read_bronze_behavioral_partition,
    recompute_dim_session,
    recompute_dim_user,
    split_valid_invalid,
)


# NOTE: Step-3 version. Full Bronze -> Silver star schema:
#   provision tables -> seed dim_date -> read + dedupe Bronze
#   -> split valid/invalid -> quarantine invalid
#   -> DQ-flag valid -> upsert dim_device / dim_event_type
#   -> upsert fact_behavioral_events
#   -> recompute dim_user / dim_session for touched entities.
# All writes are idempotent MERGEs, so reruns/backfills don't duplicate.

DEFAULT_BRONZE_BEHAVIORAL_PATH = "s3a://bronze/behavioral/events"

DIM_DATE_SEED_START = date(2023, 1, 1)
DIM_DATE_SEED_END = date(2030, 12, 31)


def get_bronze_behavioral_path() -> str:
    return os.getenv("BEHAVIORAL_BRONZE_OUTPUT_PATH", DEFAULT_BRONZE_BEHAVIORAL_PATH)


def seed_dim_date_if_needed(spark) -> None:
    dim_date_table = qualified_table("dim_date")
    if spark.table(dim_date_table).count() > 0:
        return
    seed_df = build_dim_date_seed(spark, DIM_DATE_SEED_START, DIM_DATE_SEED_END)
    seed_df.writeTo(dim_date_table).append()


def merge_into(spark, target_table: str, updates_df, merge_keys: list) -> int:
    """
    Idempotent Iceberg MERGE INTO. Returns source row count. Skips if
    empty. updates_df column names must match the target table schema.
    """
    source_count = updates_df.count()
    if source_count == 0:
        return 0

    temp_view = "_silver_merge_source"
    updates_df.createOrReplaceTempView(temp_view)

    on_clause = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)
    spark.sql(
        f"""
        MERGE INTO {target_table} AS target
        USING {temp_view} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    return source_count


def run_silver_behavioral_job(execution_date: date) -> None:
    spark = create_iceberg_spark_session(app_name="silver-behavioral-etl")

    print(f"\n=== Silver behavioral job (Step 3) for {execution_date} ===\n")

    ensure_silver_behavioral_tables(spark)
    seed_dim_date_if_needed(spark)
    print("Tables ensured; dim_date seeded.")

    bronze_df = read_bronze_behavioral_partition(
        spark=spark,
        bronze_path=get_bronze_behavioral_path(),
        execution_date=execution_date,
    )
    raw_count = bronze_df.count()
    print(f"Bronze rows read: {raw_count}")

    if raw_count == 0:
        print("No Bronze data for this date. Nothing to do.")
        spark.stop()
        return

    deduped_df = deduplicate_events(bronze_df)
    valid_df, invalid_df = split_valid_invalid(deduped_df)

    # DQ-flagged valid rows are reused by the fact build + dim recompute,
    # so cache to avoid recomputing the Bronze read/split three times.
    valid_flagged_df = apply_silver_data_quality(valid_df).cache()
    valid_count = valid_flagged_df.count()  # materializes the cache
    print(f"Valid rows: {valid_count}   Invalid rows: {invalid_df.count()}")

    # --- quarantine (invalid rows) ---
    q_written = merge_into(
        spark,
        target_table=qualified_table("behavioral_events_quarantine"),
        updates_df=build_quarantine_rows(invalid_df),
        merge_keys=["kafka_partition", "kafka_offset"],
    )
    print(f"Quarantine rows merged: {q_written}")

    # --- lookup dimensions ---
    d_dev = merge_into(
        spark,
        target_table=qualified_table("dim_device"),
        updates_df=build_dim_device_updates(valid_flagged_df),
        merge_keys=["device_key"],
    )
    d_evt = merge_into(
        spark,
        target_table=qualified_table("dim_event_type"),
        updates_df=build_dim_event_type_updates(valid_flagged_df),
        merge_keys=["event_type_key"],
    )
    print(f"dim_device upserted: {d_dev}   dim_event_type upserted: {d_evt}")

    # --- fact table (idempotent upsert by natural event_key) ---
    fact_table = qualified_table("fact_behavioral_events")
    f_written = merge_into(
        spark,
        target_table=fact_table,
        updates_df=build_fact_behavioral_events(valid_flagged_df),
        merge_keys=["event_key"],
    )
    print(f"fact_behavioral_events upserted: {f_written}")

    # --- dim_user / dim_session recompute (from fact, touched entities) ---
    touched_user_ids = valid_flagged_df.select("user_id").distinct()
    touched_session_ids = valid_flagged_df.select("session_id").distinct()

    u_written = merge_into(
        spark,
        target_table=qualified_table("dim_user"),
        updates_df=recompute_dim_user(spark, fact_table, touched_user_ids),
        merge_keys=["user_key"],
    )
    s_written = merge_into(
        spark,
        target_table=qualified_table("dim_session"),
        updates_df=recompute_dim_session(spark, fact_table, touched_session_ids),
        merge_keys=["session_key"],
    )
    print(f"dim_user upserted: {u_written}   dim_session upserted: {s_written}")

    valid_flagged_df.unpersist()

    # --- final table counts ---
    print("\nCurrent Silver table row counts:")
    for table in [
        "dim_date", "dim_user", "dim_device", "dim_event_type",
        "dim_session", "fact_behavioral_events", "behavioral_events_quarantine",
    ]:
        full = qualified_table(table)
        print(f"  {full}: {spark.table(full).count()}")

    print("\n=== Step 3 finished OK ===")
    spark.stop()


def parse_args():
    parser = argparse.ArgumentParser(description="Run Silver behavioral batch ETL job")
    parser.add_argument(
        "--execution-date",
        type=str,
        required=True,
        help="Date (YYYY-MM-DD) of the Bronze partition to process.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    exec_date = datetime.strptime(args.execution_date, "%Y-%m-%d").date()
    run_silver_behavioral_job(execution_date=exec_date)
