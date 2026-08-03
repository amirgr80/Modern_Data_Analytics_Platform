from __future__ import annotations

import os
import sys

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

SRC_PATH = os.getenv("PIPELINE_SRC_PATH", "/opt/airflow/src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from common.gold_transactional_config import GoldTransactionalConfig


TEHRAN_TZ = pendulum.timezone("Asia/Tehran")
GOLD_JOB_PATH = f"{SRC_PATH}/jobs/gold_transactional_job.py"

SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
    "org.apache.iceberg:iceberg-aws-bundle:1.6.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

PROCESS_DATE_TEMPLATE = (
    "{{ dag_run.conf.get("
    "'process_date', "
    "data_interval_end.in_timezone('Asia/Tehran').subtract(days=1).to_date_string()"
    ") }}"
)


def check_clickhouse_ready() -> None:
    import clickhouse_connect

    config = GoldTransactionalConfig.from_env()
    client = clickhouse_connect.get_client(
        host=config.clickhouse_host,
        port=config.clickhouse_http_port,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
        database=config.clickhouse_db,
    )
    try:
        client.command("SELECT 1")
    finally:
        client.close()


def build_spark_env() -> dict[str, str]:
    config = GoldTransactionalConfig.from_env()
    return {
        "PYTHONPATH": SRC_PATH,
        "SPARK_MASTER_URL": "spark://spark-master:7077",
        "ICEBERG_CATALOG_NAME": config.iceberg_catalog,
        "ICEBERG_REST_URI": config.iceberg_rest_uri,
        "ICEBERG_WAREHOUSE": config.iceberg_warehouse,
        "ICEBERG_NAMESPACE": config.iceberg_namespace,
        "CLICKHOUSE_HOST": config.clickhouse_host,
        "CLICKHOUSE_HTTP_PORT": str(config.clickhouse_http_port),
        "CLICKHOUSE_DB": config.clickhouse_db,
        "CLICKHOUSE_USER": config.clickhouse_user,
        "CLICKHOUSE_PASSWORD": config.clickhouse_password,
    }


def build_spark_command() -> str:
    return (
        "set -euo pipefail; "
        "spark-submit "
        "--master 'local[*]' "   
        "--driver-memory '2g' "
        "--executor-memory '4g' "
        "--executor-cores '2' "
        f"--packages '{SPARK_PACKAGES}' "
        "--conf spark.sql.extensions="
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
        "--conf spark.sql.catalog.lakekeeper="
        "org.apache.iceberg.spark.SparkCatalog "
        "--conf spark.sql.catalog.lakekeeper.type=rest "
        "--conf spark.sql.catalog.lakekeeper.uri="
        "http://lakekeeper:8181/catalog "
        "--conf spark.sql.catalog.lakekeeper.warehouse=silver "
        "--conf spark.sql.catalog.lakekeeper.io-impl="
        "org.apache.iceberg.aws.s3.S3FileIO "
        "--conf spark.sql.catalog.lakekeeper.s3.endpoint="
        "http://minio:9000 "
        "--conf spark.sql.catalog.lakekeeper.s3.path-style-access=true "
        "--conf spark.sql.catalog.lakekeeper.s3.access-key-id=minioadmin "
        "--conf spark.sql.catalog.lakekeeper.s3.secret-access-key=minioadmin123 "
        "--conf spark.sql.catalog.lakekeeper.s3.region=us-east-1 "
        "--conf spark.sql.catalog.lakekeeper.default-namespace=transactional "
        "--conf spark.sql.session.timeZone=Asia/Tehran "
        "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
        "--conf spark.hadoop.fs.s3a.path.style.access=true "
        "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
        "--conf spark.hadoop.fs.s3a.aws.credentials.provider="
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider "
        "--conf spark.hadoop.fs.s3a.access.key=minioadmin "
        "--conf spark.hadoop.fs.s3a.secret.key=minioadmin123 "
        f"'{GOLD_JOB_PATH}' "
        f"--order-date '{PROCESS_DATE_TEMPLATE}'"
    )


default_args = {
    "owner": "gold-transactional",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "execution_timeout": pendulum.duration(hours=2),
}


with DAG(
    dag_id="gold_transactional_daily",
    description=(
        "Load Silver Transactional Iceberg tables into ClickHouse Gold OBT"
    ),
    default_args=default_args,
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=TEHRAN_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "transactional", "clickhouse"],
) as dag:

    check_clickhouse = PythonOperator(
        task_id="check_clickhouse_ready",
        python_callable=check_clickhouse_ready,
    )

    run_gold_job = BashOperator(
        task_id="run_gold_transactional_job",
        append_env=True,
        env=build_spark_env(),
        bash_command=build_spark_command(),
    )

    check_clickhouse >> run_gold_job