from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column
from pyspark.sql import functions as F


_RUN_TIMESTAMP: datetime | None = None


def initialize_run_timestamp(
    run_timestamp: datetime,
) -> datetime:
    """Initialize the single fixed timestamp for one Silver run."""

    global _RUN_TIMESTAMP

    if run_timestamp is None:
        raise ValueError(
            "run_timestamp must not be None."
        )

    if (
        _RUN_TIMESTAMP is not None
        and _RUN_TIMESTAMP != run_timestamp
    ):
        raise RuntimeError(
            "Transactional run timestamp was already "
            "initialized with a different value."
        )

    _RUN_TIMESTAMP = run_timestamp

    return _RUN_TIMESTAMP


def get_run_timestamp() -> datetime:
    if _RUN_TIMESTAMP is None:
        raise RuntimeError(
            "Transactional run timestamp has not been initialized."
        )

    return _RUN_TIMESTAMP


def run_timestamp_column() -> Column:
    """
    Return a deterministic Spark timestamp literal.

    This must be used instead of current_timestamp() inside
    DataFrames that become Iceberg MERGE sources.
    """

    return F.lit(
        get_run_timestamp()
    ).cast("timestamp")
