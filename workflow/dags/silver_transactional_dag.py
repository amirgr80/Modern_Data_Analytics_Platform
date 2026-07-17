from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)


DAG_ID = "silver_transactional_pipeline"
SPARK_CONNECTION_ID = "spark_default"
APPLICATION_PATH = "/opt/spark-apps/jobs/silver_transactional_job.py"


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    description="Read Bronze transactional Parquet and build Silver Iceberg Kimball tables.",
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["silver", "transactional", "spark", "iceberg"],
) as dag:

    run_silver_transactional_job = SparkSubmitOperator(
        task_id="run_silver_transactional_job",
        conn_id=SPARK_CONNECTION_ID,
        application=APPLICATION_PATH,
        name="silver-transactional-job",
        application_args=[
            "--partition-date",
            "{{ data_interval_start.in_timezone('Asia/Tehran').strftime('%Y%m%d') }}",
            "--catalog",
            "lakekeeper",
            "--namespace",
            "silver",
            "--quality-namespace",
            "silver_quality",
            "--dim-date-start",
            "2020-01-01",
            "--dim-date-end",
            "2035-12-31",
        ],
        conf={
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions."
                "IcebergSparkSessionExtensions"
            ),
            "spark.sql.catalog.lakekeeper": (
                "org.apache.iceberg.spark.SparkCatalog"
            ),
            "spark.sql.catalog.lakekeeper.type": "rest",
            "spark.sql.catalog.lakekeeper.uri": (
                "http://lakekeeper:8181/catalog"
            ),
            "spark.sql.catalog.lakekeeper.warehouse": "warehouse",
            "spark.sql.catalog.lakekeeper.io-impl": (
                "org.apache.iceberg.aws.s3.S3FileIO"
            ),
            "spark.sql.catalog.lakekeeper.s3.endpoint": (
                "http://minio:9000"
            ),
            "spark.sql.catalog.lakekeeper.s3.path-style-access": "true",
            "spark.sql.session.timeZone": "Asia/Tehran",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        env_vars={
            "PYTHONPATH": "/opt/spark-apps",
        },
        verbose=True,
    )
