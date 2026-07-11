import logging
import sys

from common.kafka_reader import read_kafka_topic
from common.minio_writer import write_transactional_stream
from common.spark_session import create_spark_session
from schema.transactional_schemas import TRANSACTIONAL_SCHEMAS
from transformations.bronze_transform_transactional import (
    transform_bronze_transactional,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("bronze_transactional_job")


def main() -> None:
    spark = None
    queries = []

    try:
        spark = create_spark_session()

        logger.info(
            "Bronze Transactional job started."
        )

        for table_name, table_schema in TRANSACTIONAL_SCHEMAS.items():
            logger.info(
                "Configuring transactional topic '%s'.",
                table_name,
            )

            kafka_df = read_kafka_topic(
                spark=spark,
                topic_name=table_name,
            )

            transformed_df = transform_bronze_transactional(
                kafka_df=kafka_df,
                schema=table_schema,
                table_name=table_name,
            )

            query = write_transactional_stream(
                dataframe=transformed_df,
                table_name=table_name,
            )

            queries.append(query)

        logger.info(
            "%s transactional streaming queries started: %s",
            len(queries),
            ", ".join(TRANSACTIONAL_SCHEMAS.keys()),
        )

        spark.streams.awaitAnyTermination()

    except Exception:
        logger.exception(
            "Bronze Transactional job failed."
        )
        sys.exit(1)

    finally:
        for query in queries:
            if query.isActive:
                query.stop()

        if spark is not None:
            spark.stop()

        logger.info(
            "Bronze Transactional job stopped."
        )


if __name__ == "__main__":
    main()