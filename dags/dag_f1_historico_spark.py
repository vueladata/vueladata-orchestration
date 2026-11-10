"""DAG Histórico F1 de Spark en Fargate Spot (Validar S3 >> Procesar Silver >> Gold)."""
from datetime import datetime, timedelta, timezone
import boto3

from airflow.sdk import dag, task
from vueladata_orchestration.config import spark_configuration
from vueladata_orchestration.spark_tasks import spark_stage

DEFAULT_ARGS = {
    "owner": "vueladata-dataeng",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
}

@dag(
    dag_id="dag_f1_historico_spark",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    render_template_as_native_obj=True,
    tags=["vueladata", "spark", "fargate-spot", "f1-historico"],
)
def pipeline_historico():

    @task(task_id="validar_entorno", execution_timeout=timedelta(minutes=5))
    def validar_archivos_f1_s3() -> dict:
        config = spark_configuration()
        bucket = config["bucket"]
        s3 = boto3.client("s3", region_name=config.get("region", "us-east-1"))

        required_prefixes = ["bronze/f1_kaggle/", "bronze/f6_rotacion/"]
        for prefix in required_prefixes:
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            if resp.get("KeyCount", 0) == 0:
                raise ValueError(f"Falta el prefijo requerido en S3: s3://{bucket}/{prefix}")

        return config

    val = validar_archivos_f1_s3()
    t_silver = spark_stage("silver", "historical", "dag_f1_historico_spark")
    t_gold = spark_stage("gold", "historical", "dag_f1_historico_spark")

    val >> t_silver >> t_gold

pipeline_historico()
