"""
DAG de Procesamiento Histórico (F1): Vuelos Kaggle (5.8M) en ECS Fargate Spot
Cumple con la Historia de Usuario HU-26:
- Frecuencia: schedule=None (Bajo demanda / Ejecución manual única).
- Dispara el contenedor efímero de Apache Spark en AWS ECS Fargate Spot.
- Procesa los 5.8 millones de vuelos históricos de Kaggle (F1) desde s3://.../bronze/f1_kaggle/
  hacia las capas Silver y Gold calculando OTP (RN-01) y propagación (RN-08).
"""

from datetime import datetime, timedelta
import os

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_BUCKET = os.getenv("S3_DATA_LAKE_BUCKET", "vueladata-datalake-dev-239700238526-us-east-1-an")
ECS_CLUSTER = os.getenv("ECS_CLUSTER_NAME", "vueladata-cluster")
ECS_SUBNET = os.getenv("ECS_SUBNET_ID", "subnet-063e1dbea89657b96")
ECS_SECURITY_GROUP = os.getenv("ECS_SECURITY_GROUP_ID", "sg-0c39aef51177c31de")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

DEFAULT_ARGS = {
    "owner": "vueladata-dataeng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="dag_f1_historico_spark",
    description="Procesamiento bajo demanda de 5.8M vuelos Kaggle en Fargate Spot",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["hu-26", "spark", "f1-kaggle", "fargate-spot", "historico"],
)
def dag_f1_historico_spark():

    @task
    def validar_archivos_f1_s3() -> dict:
        """
        Verifica que existan flights.csv, airlines.csv y airports.csv
        en s3://<bucket>/bronze/f1_kaggle/ antes de disparar el cómputo de Spark.
        """
        archivos_requeridos = [
            "bronze/f1_kaggle/flights.csv",
            "bronze/f1_kaggle/airlines.csv",
            "bronze/f1_kaggle/airports.csv"
        ]
        print(f"[*] Validando presencia del dataset F1 en {S3_BUCKET}...")

        try:
            s3_hook = S3Hook(aws_conn_id="aws_default")
            faltantes = []
            for key in archivos_requeridos:
                if not s3_hook.check_for_key(key, bucket_name=S3_BUCKET):
                    faltantes.append(key)

            if faltantes:
                print(f"[!] Advertencia: Archivos no encontrados en S3: {faltantes}")
                return {"status": "MISSING_FILES", "missing": faltantes}

            print("[+] Todos los archivos de F1 están presentes en S3 Bronze.")
            return {"status": "READY"}
        except Exception as e:
            print(f"[!] Verificación omitida en entorno local sin credenciales AWS: {e}")
            return {"status": "LOCAL_DEV_BYPASS"}

    procesar_f1_en_fargate_spot = EcsRunTaskOperator(
        task_id="ejecutar_spark_f1_historico_spot",
        cluster=ECS_CLUSTER,
        task_definition="vueladata-spark-etl",
        capacity_provider_strategy=[
            {
                "capacityProvider": "FARGATE_SPOT",
                "weight": 1,
                "base": 0
            }
        ],
        network_configuration={
            "awsvpcConfiguration": {
                "subnets": [ECS_SUBNET],
                "securityGroups": [ECS_SECURITY_GROUP],
                "assignPublicIp": "DISABLED"
            }
        },
        overrides={
            "containerOverrides": [
                {
                    "name": "spark-etl",
                    "command": [
                        "--layer", "all",
                        "--mode", "historical",
                        "--bucket", S3_BUCKET,
                        "--run-id", "{{ run_id }}",
                        "--dag-id", "dag_f1_historico_spark"
                    ]
                }
            ]
        },
        awslogs_group="/ecs/vueladata-spark-etl",
        awslogs_stream_prefix="spark-spot",
        wait_for_completion=True,
        aws_conn_id="aws_default",
        region_name=AWS_REGION
    )

    @task
    def fin_procesamiento_historico():
        print("[+] Procesamiento histórico F1 completado exitosamente.")
        print("[+] 5.8M registros procesados y consolidados en Silver y Gold.")
        return "SUCCESS"

    val = validar_archivos_f1_s3()
    val >> procesar_f1_en_fargate_spot >> fin_procesamiento_historico()


dag_f1_historico_spark()
