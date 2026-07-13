import logging
import sys

from common.bronze_transactional_kafka_reader import read_kafka_topic
from common.bronze_transactional_minio_writer import write_transactional_stream
from common.bronze_transactional_spark_session import create_spark_session
from schemas.bronze_transactional_schemas import (
    TRANSACTIONAL_SCHEMAS,
    TRANSACTIONAL_TOPICS,
    validate_transactional_configuration,
)
from common.bronze_transactional_transform import (
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
        validate_transactional_configuration()

        spark = create_spark_session()

        logger.info(
            "Bronze Transactional job started."
        )

        logger.info(
            "Configured logical tables: %s",
            ", ".join(TRANSACTIONAL_SCHEMAS.keys()),
        )

        for table_name, table_schema in (
            TRANSACTIONAL_SCHEMAS.items()
        ):
            kafka_topic_name = TRANSACTIONAL_TOPICS[table_name]

            logger.info(
                "Configuring table '%s' from Kafka topic '%s'.",
                table_name,
                kafka_topic_name,
            )

            kafka_df = read_kafka_topic(
                spark=spark,
                topic_name=kafka_topic_name,
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

        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info(
            "Bronze Transactional job interrupted by user."
        )

    except Exception:
        logger.exception(
            "Bronze Transactional job failed."
        )
        sys.exit(1)

    finally:
        for query in queries:
            try:
                if query.isActive:
                    query.stop()
            except Exception:
                logger.exception(
                    "Failed to stop streaming query '%s'.",
                    query.name,
                )

        if spark is not None:
            spark.stop()

        logger.info(
            "Bronze Transactional job stopped."
        )


if __name__ == "__main__":
    main()
