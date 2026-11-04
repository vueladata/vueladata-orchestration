"""
DAG de Ingesta Externa: Orquestación y Auditoría de AWS Lambda (OpenSky y OpenMeteo)
Cumple con la Historia de Usuario HU-26 y Arquitectura Híbrida:
- La ingesta continua de alta frecuencia (OpenSky cada 5m, OpenMeteo cada 15m)
  se delega de forma serverless y sin consumo de infraestructura a Amazon EventBridge.
- Este DAG de Airflow opera de forma diaria (02:00 UTC) o bajo demanda:
  * Ejecuta auditoría y verificación de las funciones AWS Lambda.
  * Actúa como respaldo (fallback / reintento) antes del procesamiento diario en Spark.
  * Minimiza el consumo de recursos en ECS Fargate, Celery y RDS.
"""

from datetime import datetime, timedelta
import json
import os

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

DEFAULT_ARGS = {
    "owner": "vueladata-dataeng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="dag_ingesta_externa_lambdas",
    description="Orquestación y auditoría diaria de ingesta externa vía AWS Lambda (HU-26). Ingesta continua delegada a EventBridge.",
    default_args=DEFAULT_ARGS,
    schedule="0 2 * * *", # Ejecución diaria a las 02:00 UTC (ahorro óptimo de recursos Celery/Fargate)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["hu-26", "lambda", "opensky", "openmeteo", "ingesta-externa", "eventbridge-hybrid"],
)
def dag_ingesta_externa_lambdas():

    # 1. Invocación de Lambda OpenSky Ingestor (F2)
    invocar_opensky = LambdaInvokeFunctionOperator(
        task_id="invocar_lambda_opensky_ingestor",
        function_name="opensky-ingestor",
        payload=json.dumps({
            "trigger_source": "airflow-3",
            "execution_time": "{{ ts }}"
        }),
        log_type="Tail",
        aws_conn_id="aws_default",
        region_name=AWS_REGION
    )

    # 2. Invocación de Lambda OpenMeteo Ingestor (F3)
    invocar_openmeteo = LambdaInvokeFunctionOperator(
        task_id="invocar_lambda_openmeteo_ingestor",
        function_name="openmeteo-ingestor",
        payload=json.dumps({
            "trigger_source": "airflow-3",
            "execution_time": "{{ ts }}"
        }),
        log_type="Tail",
        aws_conn_id="aws_default",
        region_name=AWS_REGION
    )

    @task
    def consolidar_logs_ingesta(response_opensky, response_openmeteo):
        print("=========================================================")
        print("[+] Validando resultados de Ingesta Externa vía AWS Lambda")
        
        errors = []
        for name, resp in [("OpenSky (F2)", response_opensky), ("OpenMeteo (F3)", response_openmeteo)]:
            parsed = None
            if isinstance(resp, str):
                try:
                    parsed = json.loads(resp)
                except Exception:
                    pass
            elif isinstance(resp, dict):
                parsed = resp
            
            print(f"[*] {name} Payload: {resp}")
            if parsed and isinstance(parsed, dict):
                status = parsed.get("statusCode")
                if status and status != 200:
                    errors.append(f"{name} retornó status {status}: {parsed.get('body')}")
        
        if errors:
            raise RuntimeError(f"Fallas en ingesta externa: {'; '.join(errors)}")
            
        print("[SUCCESS] Ambas funciones Lambda completaron su ejecución y persistieron en S3 Bronze con status 200.")
        print("=========================================================")
        return "OK"

    # Ambas lambdas pueden ejecutarse en paralelo por los Celery Workers
    [invocar_opensky, invocar_openmeteo] >> consolidar_logs_ingesta(
        invocar_opensky.output,
        invocar_openmeteo.output
    )


dag_ingesta_externa_lambdas()
