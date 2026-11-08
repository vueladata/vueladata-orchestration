"""Daily fallback. Continuous ingestion is owned by EventBridge, not this DAG.
Lambda must enforce the shared upstream quota and return statusCode (including 429).
"""
import json
import os
from datetime import datetime, timedelta, timezone
from airflow.sdk import dag
from vueladata_orchestration.operators import CheckedLambdaInvokeFunctionOperator

DEFAULT_ARGS = {"owner": "vueladata-dataeng", "retries": 2,
                "retry_delay": timedelta(minutes=15), "retry_exponential_backoff": True,
                "max_retry_delay": timedelta(hours=1)}

@dag(dag_id="dag_ingesta_externa_lambdas", schedule="0 2 * * *",
     start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), catchup=False,
     max_active_runs=1, default_args=DEFAULT_ARGS, tags=["lambda", "ingesta-externa"])
def pipeline():
    for source in ("opensky", "openmeteo"):
        CheckedLambdaInvokeFunctionOperator(
            task_id=f"invocar_lambda_{source}_ingestor",
            function_name=os.getenv(f"LAMBDA_{source.upper()}_NAME", f"{source}-ingestor"),
            invocation_type="RequestResponse", log_type="None",
            payload=json.dumps({"trigger_source": "airflow-3", "execution_time": "{{ ts }}"}),
            botocore_config={"connect_timeout": 10, "read_timeout": 910,
                             "retries": {"mode": "standard", "total_max_attempts": 1}},
            execution_timeout=timedelta(minutes=16), max_active_tis_per_dag=1,
            aws_conn_id="aws_default", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        )

pipeline()
