"""One ECS task per persisted Medallion stage; shared pool protects S3 writers."""
import os
from datetime import timedelta

from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator


def spark_stage(layer, mode, dag_id):
    historical_gold = layer == "gold" and mode == "historical"
    return EcsRunTaskOperator(
        task_id=f"procesar_{layer}",
        cluster="{{ ti.xcom_pull(task_ids='validar_entorno')['cluster'] }}",
        task_definition="{{ ti.xcom_pull(task_ids='validar_entorno')['task_definition'] }}",
        capacity_provider_strategy=[{"capacityProvider": "FARGATE_SPOT", "weight": 1}],
        network_configuration="{{ ti.xcom_pull(task_ids='validar_entorno')['network'] }}",
        overrides={"containerOverrides": [{
            "name": "{{ ti.xcom_pull(task_ids='validar_entorno')['container'] }}",
            "command": ["--layer", layer, "--mode", mode, "--bucket",
                        "{{ ti.xcom_pull(task_ids='validar_entorno')['bucket'] }}",
                        "--date", "{{ dag_run.conf.get('date') or (dag_run.logical_date or dag_run.start_date).strftime('%Y-%m-%d') }}",
                        "--run-id", "{{ run_id }}", "--dag-id", dag_id],
        }]},
        awslogs_group="/ecs/vueladata-spark-etl",
        awslogs_stream_prefix="spark-spot/spark-etl",
        pool="gold_writer",
        wait_for_completion=True,
        # Keep the single-writer pool slot throughout the external job.
        deferrable=False,
        # Recover the same ECS task if Airflow retries while it is still running.
        reattach=historical_gold,
        waiter_delay=15,
        waiter_max_attempts=360 if historical_gold else 240,
        execution_timeout=timedelta(minutes=100 if historical_gold else 60),
        aws_conn_id="aws_default", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
