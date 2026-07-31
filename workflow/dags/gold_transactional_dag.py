from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor


DAG_ID = "gold_transactional_daily"
SPARK_CONNECTION_ID = "spark_default"
APPLICATION_PATH = "/opt/spark-apps/jobs/gold_transactional_job.py"

SILVER_DAG_ID = "silver_transactional_pipeline"
SILVER_TASK_ID = "run_silver_transactional_job"


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _silver_execution_date(dt: datetime):
    return dt - timedelta(hours=1)


@task.sensor(poke_interval=30, timeout=600, mode="reschedule")
def check_clickhouse_ready() -> bool:
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username="default",
        password="clickhouse123",
        database="lakehouse",
    )
    try:
        client.command("SELECT 1")
        return True
    finally:
        client.close()


with DAG(
    dag_id=DAG_ID,
    description=(
        "Build Gold transactional OBT from Silver Iceberg "
        "and load partition into ClickHouse."
    ),
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["gold", "transactional", "spark", "iceberg", "clickhouse"],
) as dag:

    wait_for_silver = ExternalTaskSensor(
        task_id="check_silver_transactional_succeeded",
        external_dag_id=SILVER_DAG_ID,
        external_task_id=SILVER_TASK_ID,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60 * 2,
        execution_date_fn=_silver_execution_date,
    )

    clickhouse_ready = check_clickhouse_ready()

    run_gold_job = SparkSubmitOperator(
        task_id="run_gold_transactional_job",
        conn_id=SPARK_CONNECTION_ID,
        application=APPLICATION_PATH,
        name="gold-transactional-job-{{ ds }}",
        packages=(
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
            "org.apache.iceberg:iceberg-aws-bundle:1.6.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
        application_args=[
            "--order-date",
            "{{ data_interval_start.in_timezone('Asia/Tehran').strftime('%Y-%m-%d') }}",
        ],
        conf={
            "spark.serializer": "org.apache.spark.serializer.JavaSerializer",
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ),
            "spark.sql.catalog.lakekeeper": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.lakekeeper.type": "rest",
            "spark.sql.catalog.lakekeeper.uri": "http://lakekeeper:8181/catalog",
            "spark.sql.catalog.lakekeeper.warehouse": "silver",
            "spark.sql.catalog.lakekeeper.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.lakekeeper.s3.endpoint": "http://minio:9000",
            "spark.sql.catalog.lakekeeper.s3.path-style-access": "true",
            "spark.sql.catalog.lakekeeper.s3.access-key-id": "minioadmin",
            "spark.sql.catalog.lakekeeper.s3.secret-access-key": "minioadmin123",
            "spark.sql.catalog.lakekeeper.s3.region": "us-east-1",
            "spark.sql.catalog.lakekeeper.default-namespace": "transactional",
            "spark.sql.session.timeZone": "Asia/Tehran",
            "spark.sql.parquet.enableVectorizedReader": "false",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.aws.credentials.provider": (
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            ),
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
        },
        env_vars={
            "PYTHONPATH": "/opt/spark-apps",
            "CLICKHOUSE_HOST": "clickhouse",
            "CLICKHOUSE_HTTP_PORT": "8123",
            "CLICKHOUSE_DB": "lakehouse",
            "CLICKHOUSE_USER": "default",
            "CLICKHOUSE_PASSWORD": "clickhouse123",
        },
        driver_memory="2g",
        executor_memory="4g",
        executor_cores=2,
        verbose=True,
    )

    wait_for_silver >> clickhouse_ready >> run_gold_job