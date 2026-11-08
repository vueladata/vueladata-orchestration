"""Spark batch on Fargate Spot; configuration and S3 checks run only at task time."""
import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from vueladata_orchestration.config import spark_configuration

DEFAULT_ARGS = {"owner": "vueladata-dataeng", "retries": 2,
                "retry_delay": timedelta(minutes=5), "depends_on_past": False}

@dag(dag_id="dag_daily_batch_spark", schedule="0 3 * * *", start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
     catchup=False, max_active_runs=1, default_args=DEFAULT_ARGS,
     render_template_as_native_obj=True, tags=["vueladata", "spark", "fargate-spot"])
def pipeline():
    @task(execution_timeout=timedelta(minutes=5))
    def validar_entorno():
        config = spark_configuration()
        hook = S3Hook(aws_conn_id="aws_default")
        for key in ['bronze/f6_rotacion/']:
            exists = (hook.check_for_prefix(key, "/", bucket_name=config["bucket"])
                      if key.endswith("/") else hook.check_for_key(key, bucket_name=config["bucket"]))
            if not exists:
                raise ValueError(f"Missing required S3 input: {key}")
        return config

    config = validar_entorno()
    spark = EcsRunTaskOperator(
        task_id="ejecutar_spark_medallion_fargate_spot",
        cluster="{{ ti.xcom_pull(task_ids='validar_entorno')['cluster'] }}",
        task_definition="{{ ti.xcom_pull(task_ids='validar_entorno')['task_definition'] }}",
        capacity_provider_strategy=[{"capacityProvider": "FARGATE_SPOT", "weight": 1}],
        network_configuration="{{ ti.xcom_pull(task_ids='validar_entorno')['network'] }}",
        overrides={"containerOverrides": [{
            "name": "{{ ti.xcom_pull(task_ids='validar_entorno')['container'] }}",
            "command": ["--layer", "all", "--mode", "daily", "--bucket",
                        "{{ ti.xcom_pull(task_ids='validar_entorno')['bucket'] }}",
                        "--date", "{{ ds }}", "--run-id", "{{ run_id }}", "--dag-id", "dag_daily_batch_spark"]
        }]},
        awslogs_group="/ecs/vueladata-spark-etl",
        awslogs_stream_prefix="spark-spot/spark-etl",
        wait_for_completion=True, deferrable=False,
        waiter_delay=15, waiter_max_attempts=120,
        execution_timeout=timedelta(minutes=35),
        aws_conn_id="aws_default", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    config >> spark

pipeline()
