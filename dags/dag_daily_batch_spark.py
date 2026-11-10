"""DAG Batch Diario de Spark en Fargate Spot (Validar S3 >> Procesar Bronze >> Silver >> Gold)."""
from datetime import datetime, timedelta, timezone
import boto3

from airflow.sdk import dag, task
from vueladata_orchestration.config import spark_configuration
from vueladata_orchestration.spark_tasks import spark_stage

DEFAULT_ARGS = {
    "owner": "vueladata-dataeng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}

@dag(
    dag_id="dag_daily_batch_spark",
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    render_template_as_native_obj=True,
    tags=["vueladata", "spark", "fargate-spot", "daily"],
)
def pipeline_daily():

    @task(execution_timeout=timedelta(minutes=5))
    def validar_entorno() -> dict:
        config = spark_configuration()
        bucket = config["bucket"]
        s3 = boto3.client("s3", region_name=config.get("region", "us-east-1"))

        required_prefixes = ["bronze/f6_rotacion/", "bronze/events/", "bronze/rds_operacional/"]
        for prefix in required_prefixes:
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            if resp.get("KeyCount", 0) == 0:
                raise ValueError(f"Falta el prefijo requerido en S3: s3://{bucket}/{prefix}")

        return config

    val = validar_entorno()
    t_bronze = spark_stage("bronze", "daily", "dag_daily_batch_spark")
    t_silver = spark_stage("silver", "daily", "dag_daily_batch_spark")
    t_gold = spark_stage("gold", "daily", "dag_daily_batch_spark")

    val >> t_bronze >> t_silver >> t_gold

pipeline_daily()
